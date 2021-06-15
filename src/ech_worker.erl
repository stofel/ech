%%
%% Accumulate and insert data
%% Insert batch if count of rows is more than BatchSize
%% Insert batch if time of accumulating is more then BatchTime
%% If insert fail by any reason try to repeat
%% While inserts failed, data queue to new batches
%% If batches count is more than BatchesMax return error channel_fail
%%

-module(ech_worker).

-behaviour(gen_server).

-export([
  init/1, terminate/2,
  handle_info/2, handle_cast/2, handle_call/3, handle_continue/2
]).

-export([
  insert/2,
  get_status/1,
  make_request/4,
  batch_fail/1
]).


%%
init(ChannelArgs = #{name := Name}) ->
  timer:sleep(500), %% Slow down restarts
  case create_url(ChannelArgs) of
    {ok, Url} ->
      State = #{
        channel => Name,
        url     => Url,  %% Url for clickhouse connection and query
        size    => maps:get(batch_size,   ChannelArgs, 1000), %% rows to save
        time    => maps:get(sync_time,    ChannelArgs, 1000), %% Max timeout to save
        batches => maps:get(batches_count,ChannelArgs, 1000), %% max batches queue size
        failmfa => maps:get(failmfa,      ChannelArgs, {?MODULE, batch_fail, []}), %% MFA for droped batches
        stat    => #{rows => 0},
        in      => [],                                        %% batch for incoming rows
        out     => [],                                        %% batch for inserting to clickhouse
        queue   => queue:new(),                               %% batches queue
        timer   => none                                       %% Ref|none|buzy Timer ref() or channel states
      },
      {ok, State};
    Err ->
      {stop, Err}
  end.


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
  wrong_args.





%%
terminate(_Reason, _State) ->
  ok.


%% Infos
handle_info(batch_save_time, State) -> batch_save_time(State);
handle_info(Msg, State)             -> io:format("Unknown info msg ~p~n", [#{pid => self(), msg => Msg}]), {noreply, State}.
%% Casts
handle_cast(Msg, State)             -> io:format("Unknown cast msg ~p~n", [#{pid => self(), msg => Msg}]), {noreply, State}.
%% Calls
handle_call({insert, Data}, _F, State)                -> insert_(State, Data);
handle_call({batch_insert_result, Result}, _F, State) -> batch_insert_result(State, Result);
handle_call(get_status, _F, State)                    -> get_status_(State);
handle_call(Msg, _From, State)                        -> {reply, {err, {unknown_msg, Msg}}, State}.
%% Continues
handle_continue(batch_insert, State) -> batch_insert(State).



%% Insert to in
insert(Name, DataMap) ->
  gen_server:call(Name, {insert, DataMap}).
%%
insert_(State = #{in := BIn, size := BSize, timer := TimerRef, time := Time, 
                  queue := Queue, batches := MaxQueueSize}, Data) ->
  NewBIn = [Data|BIn],
  case length(NewBIn) == BSize of
    true -> %% Batch size ready to save to clickhouse
      case TimerRef == buzy of
        false ->
          timer:cancel(TimerRef),
          NewState =  State#{in := [], timer := buzy, out := NewBIn}, %% timer not avaliable while batch inserting
          {reply, ok, inc_stat(NewState, rows), {continue, batch_insert}};
        true ->
          case queue:len(Queue) == MaxQueueSize of
            false ->
              NewState = State#{in := [], queue := queue:in(NewBIn, Queue)},
              {reply, ok, inc_stat(NewState, rows)};
            true ->
              {reply, {err, channel_too_buzy}, inc_stat(State, channel_too_buzy)}
          end
      end;
    false ->
      NewRef = case TimerRef == none of
        true  -> {ok, Ref} = timer:send_after(Time, batch_save_time), Ref;
        false -> TimerRef
      end,
      {reply, ok, inc_stat(State#{in := NewBIn, timer := NewRef}, rows)}
  end.


%% handle_continue/2
batch_insert(State = #{out := Batch, url := Url, channel := ChannelName}) ->
  spawn(ech_worker, make_request,  [ChannelName, self(), Url, Batch]),
  {noreply, State}.


%%
make_request(ChannelName, ChannelPid, Url, Batch) ->
  {Result, RequestContext} = try
    begin
      case prepare_json(Batch) of
          {ok, BatchBin} ->
            ibrowse:send_req(Url, [], post, BatchBin);
          not_json ->
            {err, batch_not_json}
      end
    end of
      {ok, "200", _H, ""}                         -> {ok, none};
      {ok, "200", _H, Body}                       -> io:format("save ok ~p~n", [Body]), {ok, none};
      {ok, "500", _H, Body}                       -> {{err, clickhouse_500},      #{body => Body}};
      {error,connection_closed}                   -> {{err, connection_closed},   #{}};
      {error,{conn_failed,{error,econnrefused}}}  -> {{err, econnrefused},        #{}};
      Else                                        -> {{err, unknown_response},    #{response => Else}}
  catch
    E:R:T -> {{err, crash}, #{err => E, reason => R, stacktrace => T}}
  end,
  %% error mapping, решить что делать повтор или дроп
  %% Если проблемы со всязью или досутпом (timeout, connection refuse) то ухоидить на репит
  %% Если проблемы с json или sql синтаксисом то дропать.
  case gen_server:call(ChannelPid, {batch_insert_result, Result}) of %% TODO спросить у сервера продолжать ли попытки
    {batch_done, _} -> batch_done;
    {repeat, Time}  -> timer:sleep(Time), make_request(ChannelName, ChannelPid, Url, Batch);
    {batch_drop, {M,F,A}} -> 
      %% Try to safe batch
      DropMessage = #{name => ChannelName, pid => ChannelPid, batch => Batch, context => RequestContext},
      apply(M, F, [DropMessage|A]),
      batch_drop
  end.



%%
prepare_json(Batch) ->
  try 
    JsonList = lists:reverse([jsx:encode(Map) || Map <- Batch]),
    {ok, << <<JsonPart/binary, " ">> || JsonPart <- JsonList >>}
  catch
    _E:_R:_D -> 
      not_json
  end.


%% handle_info/2
batch_save_time(State = #{timer := buzy}) ->
  {noreply, State};
%
batch_save_time(State = #{in := BIn}) ->
  case BIn == [] of
    false -> {noreply, State#{timer := buzy, in := [], out := BIn}, {continue, batch_insert}};
    true  -> {noreply, State#{timer := none}}
  end.
  

next_batch(State = #{queue := Queue, time := Time, failmfa := MFA, stat := Stat}, Status, Reply) ->
  IncrementFun = fun(V) -> V + 1 end,
  NewState = State#{stat := maps:update_with(Status, IncrementFun, 1, Stat)},
  case queue:is_empty(Queue) of
    false ->
      {{value, NewBOut}, NewQueue} = queue:out(Queue),
      {reply, {Reply, MFA}, NewState#{queue := NewQueue, out := NewBOut}, {continue, batch_insert}};
    true ->
      {ok, TimerRef} = timer:send_after(Time, batch_save_time),
      {reply, {Reply, MFA}, NewState#{out := [], timer := TimerRef}}
  end.

  

%% Save to clickhouse result mapping
batch_insert_result(State, ok) -> 
  %% Take new batch to or do new timer
  next_batch(State, ok, batch_done);


%% Error lead to drop batch
batch_insert_result(State, {err, batch_not_json}) ->
  next_batch(State, batch_not_json, batch_drop);
%%
batch_insert_result(State, {err, clickhouse_500}) ->
  next_batch(State, clickhouse_500, batch_drop);
%%
batch_insert_result(State, {err, unknown_response}) ->
  next_batch(State, unknown_response, batch_drop);


%% Errors lead to repeat
batch_insert_result(State, {err, crash}) ->
  NewState = inc_stat(State, crash),
  {reply, {repeat, 5000}, NewState};
%%
batch_insert_result(State, {err, connection_closed}) ->
  NewState = inc_stat(State, connection_closed),
  {reply, {repeat, 5000}, NewState};
%%
batch_insert_result(State, {err, econnrefused}) ->
  NewState = inc_stat(State, econnrefused),
  {reply, {repeat, 5000}, NewState}.


%%
inc_stat(State = #{stat := Stat}, Key) ->
  IncrementFun = fun(V) -> V + 1 end,
  State#{stat := maps:update_with(Key, IncrementFun, 1, Stat)}.


%% Default droped batches fun
batch_fail(_RequestData = #{name := ChannelName, pid := ChannelPid, batch := Batch, context := RequestContext}) ->
  Msg = #{channel_name => ChannelName, pid => ChannelPid, size => length(Batch), context => RequestContext},
  io:format("Batch dropped ~p~n", [Msg]),
  ok.




%%
get_status(Name) ->
  gen_server:call(Name, get_status).

get_status_(State = #{in := In, out := Out, queue := Q}) ->
  ReplyState = State#{
    in := length(In),
    out := length(Out),
    queue := queue:len(Q)},
  {reply, ReplyState, State}.


