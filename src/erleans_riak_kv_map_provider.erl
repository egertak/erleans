-module(erleans_riak_kv_map_provider).

-behaviour(erleans_provider).
-behaviour(gen_server).

-export([start_link/2,
  all/2,
  read/3,
  read_by_hash/3,
  insert/5,
  insert/6,
  update/6,
  update/7]).

-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2]).

-define(BUCKET, <<"riak_kv_map_provider">>).

-include("erleans.hrl").

start_link(ProviderName, Args) ->
  dbg:stop_clear(),
  dbg:tracer(),
  dbg:tpl(?MODULE, [{'_', [], [{return_trace}]}]),
  dbg:p(all, call),
  dbg:p(all, return_to),
  gen_server:start_link({local, ProviderName}, ?MODULE, [ProviderName, Args], []).

init([_ProviderName, ProviderArgs]) ->
  Host = proplists:get_value(host, ProviderArgs, undefined),
  persistent_term:put({?MODULE, host}, Host),

  Port = proplists:get_value(port, ProviderArgs, undefined),
  persistent_term:put({?MODULE, port}, Port),

  {ok, undefined}.

all(Type, ProviderName) ->
  do(ProviderName, fun(C) -> all_(Type, C) end).

read(Type, ProviderName, Id) ->
  do(ProviderName, fun(C) ->
    case read(Id, Type, erlang:phash2({Id, Type}), C) of
      {ok, {_, _, _, State}} ->
        {ok, binary_to_term(State), erlang:phash2({Id, Type})};
      error ->
        not_found
    end
                   end).

read_by_hash(Type, ProviderName, Hash) ->
  do(ProviderName, fun(C) -> read_by_hash_(Hash, Type, C) end).

insert(Type, ProviderName, Id, State, ETag) ->
  insert(Type, ProviderName, Id, erlang:phash2({Id, Type}), State, ETag).

insert(Type, ProviderName, Id, Hash, State, ETag) ->
  do(ProviderName, fun(C) -> insert_(Id, Type, Hash, ETag, State, C) end).

update(Type, ProviderName, Id, State, OldETag, NewETag) ->
  update(Type, ProviderName, Id, erlang:phash2({Id, Type}), State, OldETag, NewETag).

update(Type, ProviderName, Id, Hash, State, OldETag, NewETag) ->
  do(ProviderName, fun(C) -> update_(Id, Type, Hash, OldETag, NewETag, State, C) end).

%%%

do(ProviderName, Fun) ->
  do(ProviderName, Fun, 1).

do(_ProviderName, _Fun, 0) ->
  lager:error("Failed to obtain database connection"),
  {error, no_db_connection};
do(ProviderName, Fun, Retry) ->
  Pid = case persistent_term:get({?MODULE, pid}, undefined) of
          undefined ->
            {ok, Pid1} = riakc_pb_socket:start_link(host(), port()),
            persistent_term:put({?MODULE, pid}, Pid1),
            Pid1;
          Pid1 ->
            Pid1
        end,

  try
    Fun(Pid)
  catch
    _:_Error ->
      do(ProviderName, Fun, Retry - 1)
  end.

all_(_Type, _C) ->
  [].

read(Id, Type, RefHash, Pid) ->
  IdBin = term_to_binary(Id),
  case riakc_pb_socket:fetch_type(Pid, {<<"maps">>, ?BUCKET}, IdBin) of
    {ok, O1} ->
      %% Starting with an empty map, construct the map based on the raw values.
      State = riakc_map:fold(
        fun(Key, Value, Acc) -> Acc#{Key => binary_to_term(Value)} end,
        #{}, O1),
      {ok, {Id, Type, RefHash, State}};
    _ ->
      error
  end.

read_by_hash_(_Hash, _Type, _C) ->
  error.

insert_(Id, Type, RefHash, GrainETag, GrainState, Pid) when is_map(GrainState) ->
  update_(Id, Type, RefHash, GrainETag, GrainETag, GrainState, Pid).

update_(Id, _Type, _RefHash, _OldGrainETag, _NewGrainETag, GrainState, Pid) when is_map(GrainState) ->
  IdBin = term_to_binary(Id),
  case riakc_pb_socket:fetch_type(Pid, {<<"maps">>, ?BUCKET}, IdBin) of
    {ok, O1} ->
      %% For now, shallow iteration.
      O2 = maps:fold(fun(K, V, O) ->
        riakc_map:update({term_to_binary(K), register}, fun(R) ->
          riakc_register:set(term_to_binary(V), R) end, O)
                     end, O1, GrainState),

      case riakc_pb_socket:update_type(Pid, {<<"maps">>, ?BUCKET}, IdBin, riakc_map:to_op(O2)) of
        ok ->
          ok;
        _ ->
          error
      end;
    _ ->
      error
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