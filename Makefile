.PHONY: all clean compile test
REBAR := $(shell which ./rebar3 || which rebar3 || (wget https://s3.amazonaws.com/rebar3/rebar3 && chmod +x rebar3 && which ./rebar3))

all: compile

compile:
	$(REBAR) compile

clean:
	$(REBAR) clean
	rm -rf ./logs
	rm -f ./erl_crash.dump
	rm -rf ./.eunit
	rm -f ./test/*.beam

test: test-ct

test-ct: compile
	$(REBAR) ct skip_deps=true
