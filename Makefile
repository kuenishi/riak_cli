.PHONY: compile get-deps install

all: riak_cli

riak_cli: compile
	./rebar escriptize skip_deps=true

compile: get-deps
	./rebar compile

get-deps:
	./rebar get-deps

install:
	@install riak_cli /usr/local/bin
