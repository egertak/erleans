-module(erleans_riak_kv_babel_map_provider).

-behaviour(erleans_provider).
-behaviour(gen_server).

-export([start_link/2,
  all/2,
  read/4,
  read_by_hash/3,
  insert/5,
  insert/6,
  update/6,
  update/7]).

-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2]).

-define(TYPE, <<"index_data">>).
-define(BUCKET, <<"riak_kv_babel_map_provider">>).

-include("erleans.hrl").

enable_trace() ->
  dbg:stop_clear(),
  dbg:tracer(),
  dbg:tpl(erleans_grain, [{'_', [], [{return_trace}]}]),
  dbg:tpl(door_grain, [{'_', [], [{return_trace}]}]),
  dbg:tpl(?MODULE, [{'_', [], [{return_trace}]}]),
  dbg:p(all, call),
  dbg:p(all, return_to).

start_link(ProviderName, Args) ->
  gen_server:start_link({local, ProviderName}, ?MODULE, [ProviderName, Args], []).

init([_ProviderName, ProviderArgs]) ->
  enable_trace(),
  Host = proplists:get_value(host, ProviderArgs, undefined),
  persistent_term:put({?MODULE, host}, Host),

  Port = proplists:get_value(port, ProviderArgs, undefined),
  persistent_term:put({?MODULE, port}, Port),

  {ok, undefined}.

all(Type, ProviderName) ->
  do(ProviderName, fun(C) -> all_(Type, C) end).

read(Type, ProviderName, Id) ->
  do(ProviderName, fun(C) ->
                     case read(Id, Type,
                               erlang:phash2({Id, Type}), C) of
                       {ok, {_, _, _, NewGrainState}} ->
                         {ok, NewGrainState, erlang:phash2({Id, Type})};
                       error ->
                         not_found
                     end
                   end).

read_by_hash(Type, ProviderName, Hash) ->
  do(ProviderName, fun(C) -> read_by_hash_(Hash, Type, C) end).

insert(Type, ProviderName, Id, GrainState, ETag) ->
  insert(Type, ProviderName, Id, erlang:phash2({Id, Type}), GrainState, ETag).

insert(Type, ProviderName, Id, Hash, GrainState, ETag) ->
  do(ProviderName, fun(C) -> insert_(Id, Type, Hash, ETag, GrainState, C) end).

update(Type, ProviderName, Id, GrainState, OldETag, NewETag) ->
  update(Type, ProviderName, Id, erlang:phash2({Id, Type}), GrainState, OldETag, NewETag).

update(Type, ProviderName, Id, Hash, GrainState, OldETag, NewETag) ->
  do(ProviderName, fun(C) -> update_(Id, Type, Hash, OldETag, NewETag, GrainState, C) end).

%%%

do(ProviderName, Fun) ->
  do(ProviderName, Fun, undefined, 1).

do(_ProviderName, _Fun, LastError, 0) ->
  {error, {"database update failure", LastError}};
do(ProviderName, Fun, _LastError, Retry) ->
  Pid = babel_get_socket(),
  try
    Fun(Pid)
  catch
    Type:Error ->
      do(ProviderName, Fun, {Type, Error}, Retry - 1)
  end.

all_(_Type, _C) ->
  [].

read(Id, Type, RefHash, Pid) ->
  IdBin = term_to_binary(Id),
  case babel_get(IdBin, Type:get_spec(), Pid) of
    {ok, BabelMap} ->
      {ok, {Id, Type, RefHash, BabelMap}};
    _ ->
      error
  end.

read_by_hash_(_Hash, _Type, _C) ->
  error.

insert_(Id, Type, RefHash, GrainETag, GrainState, Pid) ->
  update_(Id, Type, RefHash, GrainETag, GrainETag, GrainState, Pid).

update_(Id, Type, _RefHash, _OldGrainETag, _NewGrainETag, NewGrainState, Pid) ->
  IdBin = term_to_binary(Id),
  case babel_put(IdBin, NewGrainState, Type:get_spec(), Pid) of
    ok ->
      ok;
    Error ->
      {error, {"babel_put failure", Error}}
  end.

babel_get(IdBin, Spec, Pid) ->
  babel:get({?TYPE, ?BUCKET}, IdBin, Spec, #{connection => Pid}).

babel_put(IdBin, BabelMap, Spec, Pid) ->
  babel:put({?TYPE, ?BUCKET}, IdBin, BabelMap, Spec, #{connection => Pid}).

babel_get_socket() ->
  case persistent_term:get({?MODULE, pid}, undefined) of
    undefined ->
      {ok, Pid1} = riakc_pb_socket:start(host(), port()),
      persistent_term:put({?MODULE, pid}, Pid1),
      Pid1;
    Pid1 ->
      case is_process_alive(Pid1) of
        true -> Pid1;
        _ ->
          {ok, Pid2} = riakc_pb_socket:start(host(), port()),
          persistent_term:put({?MODULE, pid}, Pid2),
          Pid2
      end
  end.

host() ->
  case persistent_term:get({?MODULE, host}, undefined) of
    undefined ->
      "127.0.0.1";
    Host ->
      Host
  end.

port() ->
  case persistent_term:get({?MODULE, port}, undefined) of
    undefined ->
      8087;
    Port ->
      Port
  end.

handle_call(_, _, State) ->
  {noreply, State}.

handle_cast(_, State) ->
  {noreply, State}.

handle_info(_, State) ->
  {noreply, State}.