conduit-concurrent-map
======================

Provides a `Conduit` that maps a function concurrently over incoming elements, maintaining input order.

Note that this is _not_ about running different parts of a conduit pipeline concurrently (see [`stm-conduit`](http://hackage.haskell.org/package/stm-conduit-4.0.0) for something that does that).
Instead, it it provides one pipeline element that processes elements concurrently internally.

## Comparison to other libraries

* [`conduit-algorithms`](https://hackage.haskell.org/package/conduit-algorithms)
  * `Data.Conduit.Algorithms.Async`'s `asyncMapC` is similar but only does pure maps (`a -> b`) instead of monadic maps (`a -> m b`)
  * `Data.Conduit.Algorithms.Async`'s `asyncMapC` [is not async exception safe](https://github.com/luispedro/conduit-algorithms/issues/9)
* [`stm-conduit`](http://hackage.haskell.org/package/stm-conduit)
  * Completely different goal: Connects multiple conduit components so that they run concurrently.
