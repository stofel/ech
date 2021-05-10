%%%-------------------------------------------------------------------
%% @doc ech public API
%% @end
%%%-------------------------------------------------------------------

-module(ech_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    ech_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
