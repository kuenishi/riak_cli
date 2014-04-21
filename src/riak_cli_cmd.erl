-module(riak_cli_cmd).

-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").

-export([drop_bucket/2,
         list_buckets/2, list_keys/2,
         cleanup/2,
         get/3]).

drop_bucket(Client, Bucket0) ->
    {ok, ReqId} = riakc_pb_socket:stream_list_keys(Client, Bucket0),
    fold_drop_bucket(Client, Bucket0, ReqId, 0).

fold_drop_bucket(Client, Bucket, ReqId, Count) ->
    receive
        {ReqId, done} ->
            io:format("~n"),
            ok;
        {ReqId, {error, Reason}} ->
            io:format("error ~p~n", [Reason]),
            {error, Reason};
        {ReqId, {_, Keys}} ->
            delete_all(Client, Bucket, Keys),
            Count1 = Count + length(Keys),
            io:format("deleted ~p keys\r", [Count1]),
            %% don't spawn: we have to wait for the process finishx
            %% spawn(fun() -> delete_all(Client, Keys) end),
            fold_drop_bucket(Client, Bucket, ReqId, Count1)
    end.

delete_all(Client, Bucket, Keys) ->
    Deleter = fun(Key) ->
                      %%{ok, RiakObj} = riakc_
                      ok = riakc_pb_socket:delete(Client, Bucket, Key)
                      %% io:format("~p~n", [Key])
              end,
    pmap(Deleter, Keys).
    %% lists:foreach(Deleter, Keys).

pmap(Fun, List) ->
    Self = self(),
    Pids = [ spawn(fun()->
                           Fun(Elem),
                           Self ! done
                   end) || Elem <- List ],
    [ receive done -> ok end || _Pid <- Pids ].


list_buckets(Client, Type) when is_binary(Type) ->
    {ok, Buckets} = riakc_pb_socket:list_buckets(Client, Type),
    lists:foreach(fun(Bucket) ->
                          io:format("~s~n", [Bucket])
                  end, Buckets).

list_keys(Client, Bucket) ->
    {ok, Keys} = riakc_pb_socket:list_keys(Client, Bucket),
    lists:foreach(fun(Key) ->
                          io:format("~s~n", [Key])
                  end, Keys).

cleanup(Client, Bucket) ->
    {ok, ReqId} = riakc_pb_socket:stream_list_keys(Client, Bucket),
    fold_cleanup_bucket(Client, Bucket, ReqId, 0).

fold_cleanup_bucket(Client, Bucket, ReqId, Count) ->
    receive
        {ReqId, done} ->
            io:format("~n"),
            ok;
        {ReqId, {error, Reason}} ->
            io:format("error ~p~n", [Reason]),
            {error, Reason};
        {ReqId, {_, Keys}} ->
            cleanup_all(Client, Bucket, Keys),
            Count1 = Count + length(Keys),
            io:format("cleaned ~p keys\r", [Count1]),
            %% don't spawn: we have to wait for the process finishx
            %% spawn(fun() -> delete_all(Client, Keys) end),
            fold_cleanup_bucket(Client, Bucket, ReqId, Count1)
    end.

cleanup_all(Client, Bucket, Keys) ->
    Cleanup =
        fun(Key) ->
                {ok, RiakObj} = riakc_pb_socket:get(Client, Bucket, Key),
                case riakc_obj:value_count(RiakObj) of
                    1 -> ok;
                    N when N > 1 ->
                        handle_siblings(Client, RiakObj)
                end
                %% io:format("~p~n", [Key])
        end,
    pmap(Cleanup, Keys).
    %% lists:foreach(Deleter, Keys).

handle_siblings(Client, RiakObj0) ->
    Contents = riakc_obj:get_contents(RiakObj0),
    DecodedSiblings = [Content || Content <- Contents,
                                  not has_tombstone(Content)],
    B = riakc_obj:bucket(RiakObj0),
    K = riakc_obj:key(RiakObj0),
    case DecodedSiblings of
        [] ->
            io:format("deleting ~p:~s~n", [B, K]),
            riakc_pb_socket:delete(Client, B, K);
        [{M,V}|_] ->
            io:format("updating ~p:~s~n", [B, K]),
            RiakObj1 = riakc_obj:update_value(RiakObj0, V),
            RiakObj2 = riakc_obj:update_metadata(RiakObj1, M),
            riakc_pb_socket:put(Client, RiakObj2)

    end.


-spec has_tombstone({dict(), binary()}) -> boolean().
has_tombstone({_, <<>>}) ->
    true;
has_tombstone({MD, _V}) ->
    dict:is_key(?MD_DELETED, MD) =:= true.


get(Client, Bucket, Key) ->
    {ok, RiakObj} = riakc_pb_socket:get(Client, Bucket, Key),
    io:format("~p", [RiakObj]).
