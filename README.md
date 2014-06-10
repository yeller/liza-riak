# liza-riak

An implementation of liza's buckets for riak.

Supports merge functions (specified per bucket), serialization/deserialization
(ships with a fressian/snappy implementation out of the box), metrics (using
yammer metrics), and riak's CRDT counters

[Get it from clojars](https://clojars.org/liza-riak)

## Design / Goals

`liza-riak` (unlike `liza` itself) is meant to be easy - there's only some
configuration that this library supports. Specific things that are configurable:

- serialization (done with a multi method on content-type)
- allow-siblings (defaults to true)
- r values
- last-write-wins
- host/port
- storage backends
- bucket names

## Usage

```clojure
(require '[liza.store :as store])
(require '[liza.store.riak :as riak])
(require '[clojure.set :as set])

(let [riak-client (riak/connect-pb-client "localhost" 8087)
      users-bucket (riak/connect-pb-bucket "users" riak-client set/union riak/default-content-type pre-serialize post-deserialize {})]
  (store/modify users-bucket "my-key" (fn [existing] (conj (or existing #{}) {:email "foo@example.com"})))
  (store/get users-bucket "my-key"))
```

There are two options here that might not be so obvious: `pre-serialize` and
`post-deserialize`. These let you convert your data in arbitrary ways
before/after passing them to the normal deserialization method. This option is
useful if your storage is different from your domain object, but you don't want
to write that into custom serialization logic for all buckets.

## Important Notes

90% of the time you use this project to mutate data in riak, you'll want the
`modify` function, to prevent vector clock/sibling explosion.

If you're writing anything other than immutable data, you almost definitely
want `allow-siblings` true (which is the default in riak 2.0, and the default
in this project). See http://aphyr.com/posts/254-burn-the-library and
http://aphyr.com/posts/285-call-me-maybe-riak.

You should (probably) test that your merge function actually is
associative/commutative/etc, preferably using generative testing. I like both
`test.check` and `clojure.test.generative` for that.

## Testing

This client includes a `connect-pb-test-bucket`, which is just a helper for
using the in memory riak storage option.

## License

Copyright © 2014 Tom Crayford

Distributed under the Eclipse Public License, the same as Clojure.
