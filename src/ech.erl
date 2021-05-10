%%
%% Connects and resquests 
%%
-module(ech).

-include_lib("eunit/include/eunit.hrl").


-export([start_channel/2, stop_channel/1, stop_channel/0, list/0]).
-export([insert/2]).


%%
start_channel(Name, Args) ->
  ChannelArgs = Args#{
    name => Name,
    from => self()
  },
  ChannelSpecs = #{
    id       => Name,
    type     => worker,
    start    => {gen_server, start_link, [{local, Name}, ech_worker, ChannelArgs, []]}, 
    restart  => permanent,
    shutdown => 5000
  },
  supervisor:start_child(ech_sup, ChannelSpecs).


%%
create_url(Args = #{base := Base, table := Table}) ->
  QueryUrlStr = uri_string:compose_query([
    {"user",     maps:get(user, Args, "default")},
    {"password", maps:get(pass, Args, "")},
    {"database", Base},
    {"query", "insert into " ++ Table ++ " format JSONEachRow"}]),
  Url = uri_string:normalize(#{
    scheme => maps:get(scheme, Args, "http"),
    host   => maps:get(host,   Args, "localhost"),
    port   => maps:get(port,   Args, 8123),
    path   => maps:get(path,   Args, "/"),
    query  => QueryUrlStr}),
  {ok, Url};
%%
create_url(_Args) ->
  {err, {wrong_args, asgsdg}}.






stop_channel(Name) ->
  case supervisor:terminate_child(ech_sup, Name) of
    ok -> supervisor:delete_child(ech_sup, Name);
    Er -> Er
  end.


%%
stop_channel() ->
  list().


%% Async INSERT in table from connect/2 Args
insert(Name, DataMap) ->
  ech_worker:insert(Name, DataMap).


%%
list() ->
  supervisor:which_children(ech_sup).




%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Tests
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%
start_stop_test() ->
  {ok, _Pid} = ech:start_channel(test, #{url => test}),
  [_] = ech:list(),
  ok  = ech:stop_channel(test),
  {error,not_found} = ech:stop_channel(test),
  []  = ech:list(),
  [] == ech:stop_channel().

%%
insert_test() ->
  {ok, _Pid} = ech:start_channel(test, #{url => test}),
  ech:insert(test, #{<<"field">> => <<"value">>}),
  ok  = ech:stop_channel(test),
  true.
