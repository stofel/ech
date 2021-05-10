%%
%%
%%

-module(eunit_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([
  all/0,
  init_per_suite/1,
  end_per_suite/1
]).


%% Tests
-export([
  eunit/1
]).


init_per_suite(_Config) ->
    {ok, _} = application:ensure_all_started(ech),
    [].
end_per_suite(_Config) ->
    ok.


all() -> [
  eunit
].

%%
eunit(_) ->
  ok = eunit:test(ech),
  eunit:test().

