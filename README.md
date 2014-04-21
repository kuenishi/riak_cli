riak_cli
========

yet another Riak cli in Erlang/OTP

## compile

```
$ ./rebar compile escriptize
```

## drop a whole bucket

```
$ ./riak_cli -n localhost -p 8087 -c drop -t yourbucketype -b yourbucket
```


## list the buckets under a bucket type

```
$ ./riak_cli -n localhost -p 8087 -c listbuckets -t yourbucketype
```

## list all keys under a bucket

```
$ ./riak_cli -n localhost -p 8087 -c listkeys -t yourbucketype -b yourbucket
```
