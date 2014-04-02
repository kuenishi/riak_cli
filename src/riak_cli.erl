%%%-------------------------------------------------------------------
%%% @author UENISHI Kota <kota@basho.com>
%%% @copyright (C) 2014, UENISHI Kota
%%% @doc
%%%
%%% @end
%%% Created :  2 Apr 2014 by UENISHI Kota <kota@basho.com>
%%%-------------------------------------------------------------------
-module(riak_cli).

-export([main/1]).


main([]) ->
    usage(option_spec_list());
main(Args) ->
    {ok, {Options, NonOptArgs}} = getopt:parse(option_spec_list(), Args),

    Cmd = proplists:get_value(command, Options),
    Host = proplists:get_value(node, Options, localhost),
    Port = proplists:get_value(port, Options, 8098),
    io:format("~p: ~p, ~p~n", [Cmd, Options, NonOptArgs]),
    io:format("connecting to ~s:~p~n", [Host, Port]),

    case Cmd of
        "drop" ->
            Type = proplists:get_value(type, Options),
            Bucket0 = proplists:get_value(bucket, Options),
            Bucket = {list_to_binary(Type),
                      list_to_binary(Bucket0)},
            run(Host, Port, fun riak_cli_cmd:drop_bucket/2, [Bucket]);
        "get" ->
            Type = proplists:get_value(type, Options),
            Bucket0 = proplists:get_value(bucket, Options),
            Key = proplists:get_value(key, Options),
            Bucket = {list_to_binary(Type),
                      list_to_binary(Bucket0)},
            run(Host, Port, fun riak_cli_cmd:get/3, [Bucket, Key])    
    end.

run(Host, Port, Fun, Args) ->
    {ok, Client} = riakc_pb_socket:start_link(Host, Port),
    Res = erlang:apply(Fun, [Client|Args]),
    ok = riakc_pb_socket:stop(Client),
    Res.

%% usage(OptSpecList) ->
%%     io:format

usage(OptSpecList) ->
    getopt:usage(OptSpecList, escript:script_name(), " ... ",
                 []).

option_spec_list() ->
    [{help, $h, "help", undefined, "show the program options"},
     {command, $c, "command", string, "command: drop|... "},
     {node, $n, "node", string, "node name of erlang node"},
     {port, $p, "port", {integer, 65536}, "port number of riak"},

     {type, $t, "type", string, "specify bucket type"},
     {bucket, $b, "bucket", string, "specify bucket"},
     {key, $k, "key", string, "specify key"}
    ].
