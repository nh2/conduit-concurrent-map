module Main where

import           Control.Concurrent (threadDelay)
import           Control.Monad.IO.Class (liftIO)
import           Data.Conduit
import qualified Data.Conduit.List as CL
import           Test.Hspec
import           Say (sayString)

import           Data.Conduit.ConcurrentMap


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
