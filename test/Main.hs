{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import           Control.Monad (replicateM)
import           Control.Concurrent (threadDelay)
import           Control.Monad.IO.Class (liftIO)
import           Data.Conduit
import qualified Data.Conduit.Combinators as CC
import qualified Data.Conduit.List as CL
import           Data.Foldable (for_)
import           Test.Hspec
import           Test.QuickCheck
import           Test.QuickCheck.Monadic (monadicIO, run, pick, assert)
import           Say (sayString)

import           Data.Conduit.ConcurrentMap


prop_concurrentMapM_is_like_mapM :: Property
prop_concurrentMapM_is_like_mapM = monadicIO $ do
  ints :: [Int] <- pick (replicateM 10 (choose (1, 10)))
  bufferSize :: Int <- pick (choose (1, 30))
  numThreads :: Int <- pick (choose (1, 20))

  isEquals <- run $ do

    let serial = runConduit
          (   CC.yieldMany ints
           .| CC.mapM (liftIO . f)
           .| CC.sinkList
          )
    let buffered =
          runConduitRes
            (   CC.yieldMany ints
             .| concurrentMapM_ numThreads bufferSize (liftIO . f)
             .| CC.sinkList
            )

    outSerial <- serial
    outBuffered <- buffered

    return (outSerial == outBuffered)

  -- print ints -- for debugging failures
  assert isEquals

  where
    f :: Int -> IO Int
    f i = do
      -- sayString (show i ++ " before") -- for debugging
      threadDelay i -- microseconds
      -- sayString (show i ++ " after") -- for debugging
      return (i*2)


main :: IO ()
main = hspec $ do

  describe "concurrentMapM_numCaps" $ do

    it "performs a basic run" $ do
      l <- runConduitRes $
           CL.sourceList [1..6]
        .| concurrentMapM_numCaps 4
             (\i -> liftIO $ do
                sayString (show i ++ " before")
                threadDelay (i * 1000000)
                sayString (show i ++ " after")
                return (i*2)
             )
        .| CL.consume

      l `shouldBe` [2,4,6,8,10,12]

    it "does not hang when 3 elements are processed by the same thread in order" $ do
      for_ [(1::Int)..100] $ \t -> do -- try many times due to timing
        sayString ("Test " ++ show t)
        l <- runConduitRes $
             CL.sourceList [1,   0,0,0,   0,0,0,   0,0,0]
          .| concurrentMapM_ 4 2
               (\i -> liftIO $ do
                  threadDelay (i * 1000)
                  return (i*2)
               )
          .| CL.consume

        l `shouldBe` [2,0,0,0,0,0,0,0,0,0]

  describe "concurrentMapM_" $ do
    it "is like mapM" $ prop_concurrentMapM_is_like_mapM
