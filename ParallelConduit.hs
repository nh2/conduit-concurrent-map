#!/usr/bin/env stack
-- stack --resolver lts-8.12 script

-- Run this example with: `stack ParallelConduit.hs`

{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleContexts #-}

module ParallelConduit
  ( conduitPooledMapMBuffered
  ) where

import           Control.Concurrent.Async (Async, async, runConcurrently, Concurrently(..), link, wait)
import qualified Control.Concurrent.Async as Async
import           Control.Concurrent.MVar.Lifted
import           Control.Monad (when, void)
import           Control.Monad.IO.Class
import           Control.Monad.Trans
import           Control.Monad.Trans.Resource (ResourceT, MonadResource)
import           Data.Conduit
import qualified Data.Conduit as C
import           Data.Foldable
import           Data.IORef
import           Data.Maybe (fromMaybe)
import           Data.Sequence (Seq, ViewL((:<)), (|>))
import qualified Data.Sequence as Seq
import           Data.Vector ((!))
import qualified Data.Vector as V
import           GHC.Conc (getNumCapabilities)
import           Control.Monad.Trans.Unlift (askRunBase, MonadBaseUnlift)
import           Control.Monad.Catch

-- For the example `main`
import           Control.Concurrent (threadDelay)
import qualified Data.ByteString.Char8 as BS8
import qualified Data.Conduit.List as CL
import           Say (sayString)


-- This can be removed once we upgrade to `async-2.1.1`, which has this function.
forConcurrently_ :: (Foldable f) => f a -> (a -> IO b) -> IO ()
forConcurrently_ xs f = runConcurrently $ foldMap (Concurrently . void . f) xs

atomicModifyIORef_' :: IORef a -> (a -> a) -> IO ()
atomicModifyIORef_' ref f = atomicModifyIORef' ref $ \a -> (f a, ())

seqUncons :: Seq a -> (Seq a, Maybe a)
seqUncons s = case Seq.viewl s of
  Seq.EmptyL -> (s, Nothing)
  a :< s'    -> (s', Just a)

seqHeadMaybe :: Seq a -> Maybe a
seqHeadMaybe s = case Seq.viewl s of
  Seq.EmptyL -> Nothing
  a :< _     -> Just a


-- | Like conduit's `mapM`, but runs in parallel on `getNumCapabilities` threads,
-- returns outputs in the order of inputs (like `mapM`, no reordering),
-- and allows defining a bounded size output buffer for elements of type `b` to
-- maintain high parallelism despite head-of-line blocking.
--
-- Because of the no-reordering guarantee, there is head-of-line blocking:
-- When the conduit has to process a long-runing computation and a short-running
-- computation in parallel, the result of short one cannot be yielded before
-- the long one is done.
-- Unless we buffer the queued result somewhere, the thread that finished the
-- short-running computation is now blocked and sits idle (low utilisation).
--
-- To cope with this, `conduitPooledMapMBuffered workerOutputBufferSize f` gives each
-- thread `workerOutputBufferSize` output slots to store `b`s while they are blocked.
--
-- The `workerOutputBufferSize` keeps the memory usage of the conduit bounded,
-- namely to `getNumCapabilities * (workerOutputBufferSize + 1)` many `b`s at any
-- given time (the `+ 1` is for the currently processing ones).
--
-- To achieve maximum parallelism/utilisation, you should choose
-- `workerOutputBufferSize` ideally as the time factor between the fastest
-- and slowest `f` that will likely pass through the conduit; for example,
-- if most `f`s take 3 seconds, but some take 15 seconds, choose
-- `workerOutputBufferSize = 5` to avoid an earlier 15-second `f` blocking
-- a later 3-second `f`.
--
-- The threads inside the conduit will evaluate the results of the `f` to
-- WHNF, as in `!b <- f a`, so don't forget to make `f` itself `deepseq` the
-- result if there is any lazy datastructures envolved and you want to make
-- sure that they are evaluated *inside* the conduit (fully in parallel)
-- as opposed to the lazy parts of them being evaluated after being yielded.
--
-- Properties:
--
-- * Ordering / head of line blocking for outputs: The `b`s will come out in
--   the same order as their corresponding `a`s came in (the parallelism
--   doesn't change the order).
-- * Bounded memory: The conduit will only hold to as many `b`s as you have
--   `getNumCapabilities`.
-- * Full utilisation: The conduit will try to keep all cores busy as much as
--   it can. This means that it will always try to `await` if there's a free
--   core, and will only `yield` once it has to to make a core free.
--   It also ensures that any worker running for longer than others does not
--   prevent other free workers from starting new work, except from when
--   we're at the `workerOutputBufferSize` output buffer bound of `b` elements.
-- * Prompt starting: The conduit will start each `await`ed value immediately,
--   it will not batch up multiple `await`s before starting.
-- * Async exception safety: When then conduit is killed, the worker threads
--   will be killed too.
--
-- Example:
--   puts :: (MonadIO m) => String -> m () -- for non-interleaved output
--   puts s = liftIO $ BS8.putStrLn (BS8.pack s)
--   runConduitRes (CL.sourceList [1..6] .| conduitPooledMapMBuffered 4 (\i -> liftIO $ puts (show i ++ " before") >> threadDelay (i * 1000000) >> puts (show i ++ " after") >> return (i*2)) .| CL.consume )
conduitPooledMapMBuffered :: forall m a b . (MonadIO m, MonadBaseUnlift IO m, MonadResource m) => Int -> (a -> m b) -> Conduit a m b
conduitPooledMapMBuffered workerOutputBufferSize f = do
  when (workerOutputBufferSize < 1) $ do
    error $ "conduitPooledMapMBuffered requires workerOutputBufferSize < 1, got " ++ show workerOutputBufferSize

  numCaps <- liftIO getNumCapabilities

  -- Diagram:
  --
  --                     cyclic buffers, `workerOutputBufferSize` slots for each thread
  --                                               |
  -- -------------------------  [ workerOutVar1a workerOutVar1b ... ] <- f  \
  -- outQueue of workerOutVars  [ workerOutVar2a workerOutVar2b ... ] <- f  - inVar
  -- -------------------------  [ workerOutVarNa workerOutVarNb ... ] <- f  /
  --                                                                            o <- button to signal
  --                                                                                 inVarInqueued
  --
  -- Any worker that's not busy it hanging onto `inVar`, grabbing
  -- its contents as soon as `inVar` is filled.
  -- The conduit ("foreman") `awaits` upstream work, and when he gets
  -- some, puts it into the `inVar`.
  -- When a worker manages to grab it, the worker immediately puts
  -- its `workerOutVar` onto the `outQueue`, and then presses the
  -- `inVarEnqueued` button to tell the foreman that it has completed
  -- taking the work and placing its `workerOutVar` onto the queue.
  -- The foreman will wait for the signal button to be pressed before
  -- continuing his job; this guarantees that the take-inVar-queue-workerOutVar
  -- action is atomical, which guarantees input order = output order.
  --
  -- As visible in the diagram, maximally N invocations of `f` can happen at
  -- the same time, and since the `workerOutVar`s are storage places for
  -- f's outputs (`b`), maximally N `b`s are are buffered while the workers
  -- are working.

  inVar         :: MVar (Maybe a)       <- lift newEmptyMVar
  inVarEnqueued :: MVar ()              <- lift newEmptyMVar
  outQueueRef   :: IORef (Seq (MVar b)) <- liftIO $ newIORef Seq.empty

  let putInVar x = lift $ putMVar inVar x

  let signal mv     = putMVar mv ()
  let waitForSignal = takeMVar

  -- We use `MonadBaseUnlift` to make `f` run in `IO` instead of `m`, so that
  -- we can use it in conduit `bracketP`'s IO-based resource acquisition
  -- function (where we have to spawn our workers to guarantee they shut down
  -- when somebody async-kills the conduit).
  runInIO :: (m b -> IO b) <- lift askRunBase -- lift brings us into `m`

  let spawnWorkers :: IO (Async ())
      spawnWorkers = do
        workersAsync <- async $ do
          forConcurrently_ [1..numCaps] $ \_ -> do
            -- Each worker has `workerOutputBufferSize` many `workerOutVar`s
            -- in a ring buffer; until the shutdown signal is received, a worker
            -- loops to: grab an `a` from the `inVar`, picks its next `workerOutVar,
            -- puts it into the `outQueue`, signals that it has atomically done these
            -- 2 actions, processes `b <- f x`, and writes the `b` to the `workerOutVar`.
            workerOutVars <- V.replicateM workerOutputBufferSize newEmptyMVar
            let loop :: Int -> IO ()
                loop !i = do

                  m'a <- takeMVar inVar
                  case m'a of
                    Nothing -> return () -- shutdown signal, worker quits
                    Just a -> do
                      let workerOutVar = workerOutVars ! i
                      atomicModifyIORef_' outQueueRef (|> workerOutVar)
                      signal inVarEnqueued
                      -- Important: Force WHNF here so that f gets evaluated inside the
                      -- worker; it's `f`'s job to decide whether to deepseq or not.
                      !b <- runInIO (f a)
                      putMVar workerOutVar b
                      loop ((i + 1) `rem` workerOutputBufferSize)

            loop 0

        link workersAsync

        return workersAsync

  bracketP
    spawnWorkers
    (\workersAsync -> Async.cancel workersAsync) -- TODO switch to e.g. `liftBase UnliftedAsync.uninterruptibleCancel` once we have async-2.1.1
    $ \workersAsync -> do

      let mustBeNonempty = fromMaybe (error "conduitPooledMapMBuffered: outQueue cannot be empty")

      let yieldQueueHead = do
            workerVar <- mustBeNonempty <$>
              liftIO (atomicModifyIORef' outQueueRef seqUncons)

            b <- takeMVar workerVar
            C.yield b

      let tryYieldQueueHead = do
            m'workerVar <- seqHeadMaybe <$> liftIO (readIORef outQueueRef)
            case m'workerVar of
              Nothing -> return False
              Just workerVar -> do

                m'b <- tryTakeMVar workerVar

                case m'b of
                  Nothing -> return False
                  Just b -> do
                    _ <- mustBeNonempty <$> liftIO (atomicModifyIORef' outQueueRef seqUncons)
                    C.yield b
                    return True


      -- There are 3 phases in the life of this conduit, which happen subsequentially:
      -- 1) Ramp-up phase,
      --      while we've received less inputs than we have `numCaps`.
      --      We remember how many elements were received (`numWorkersRampedUp`).
      -- 2) Cruise phase,
      --      during which we always have at least `numWorkersRampedUp` many
      --      `workerOutVar`s in the output queue (this is an invariant).
      --      At all times `numInQueue` keeps track of how many `workerOutVar`s
      --      are in the output queue.
      --      Cruise phase doesn't happen if the conduit terminates before
      --      `numCaps` elements are awaited.
      -- 3) Drain phase,
      --      in which we drain off the `numWorkersRampedUp` elements that we
      --      know must be in the queue (due to above invariant),
      --      drain off all elements stored in output buffers,
      --      send all workers the stop signal and wait for their orderly termination.

      let loop :: Int -> Int -> Conduit a m b
          loop numWorkersRampedUp numInQueue = do

            await >>= \case
              Nothing -> do -- upstream conduit is done, tell all workers to finish
                for_ [1..numWorkersRampedUp] $ \_ -> do
                  putInVar Nothing
                  yieldQueueHead -- This will succeed due to the "Cruise phase invariant", see above.
                for_ [1..(numCaps - numWorkersRampedUp)] $ \_ -> do -- need to quit workers that were never ramped up too
                  putInVar Nothing
                let numInQueueAfterStopping = numInQueue - numWorkersRampedUp
                for_ [1..numInQueueAfterStopping] $ \_ -> do
                  yieldQueueHead
                liftIO $ wait workersAsync -- wait for workers to shut down

              Just a
                | numWorkersRampedUp < numCaps -> do
                    -- Ramp-up phase: This branch is taken until all `numCaps`
                    -- are doing something or the upstream conduit is done;
                    -- after that it is never taken again.
                    putInVar (Just a) >> waitForSignal inVarEnqueued
                    loop (numWorkersRampedUp + 1) (numInQueue + 1)

                | otherwise -> do
                    -- Cruise phase:

                    putInVar (Just a) >> waitForSignal inVarEnqueued
                    -- At the time `waitForSignal inVarEnqueued` completes, we know
                    -- that there is a `workerOutVar` in the `outQueue` we can wait for.

                    let numInQueueAfterEnqueued = numInQueue + 1

                    let popAsManyAsPossible !remainingInQueue
                          | remainingInQueue < numWorkersRampedUp = error "conduitPooledMapMBuffered: remainingInQueue < numWorkersRampedUp"
                          | remainingInQueue == numWorkersRampedUp = return remainingInQueue
                          | otherwise = do
                              popped <- tryYieldQueueHead
                              if not popped
                                then return remainingInQueue
                                else popAsManyAsPossible (remainingInQueue - 1)

                    remainingInQueue <- popAsManyAsPossible numInQueueAfterEnqueued
                    loop numWorkersRampedUp remainingInQueue
      loop 0 0


-- Example usage
main :: IO ()
main = do

  l <- runConduitRes $
       CL.sourceList [1..6]
    .| conduitPooledMapMBuffered 4 (\i -> liftIO $ do
                                      sayString (show i ++ " before")
                                      threadDelay (i * 1000000)
                                      sayString (show i ++ " after")
                                      return (i*2)
                                   )
    .| CL.consume

  sayString ("Result: " ++ show l)
