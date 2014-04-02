-module(riak_cli_cmd).

-export([drop_bucket/2]).
 
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
               

                            