.PHONY: compile get-deps escriptize

all: escriptize

escriptize: riak_cli

riak_cli: compile
	./rebar escriptize skip_deps=true

compile: get-deps
	./rebar compile

get-deps:
	./rebar get-deps
