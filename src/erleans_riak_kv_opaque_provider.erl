-module(erleans_riak_kv_opaque_provider).

-behaviour(erleans_provider).

-export([init/2,
  post_init/2,
  all/2,
  read/3,
  read_by_hash/3,
  insert/5,
  insert/6,
  update/6,
  update/7]).

-define(BUCKET, <<"riak_kv_opaque_provider">>).

-include("erleans.hrl").

init(_ProviderName, ProviderArgs) ->
  Host = proplists:get_value(host, ProviderArgs, undefined),
  put(host, Host),

  Port = proplists:get_value(port, ProviderArgs, undefined),
  put(port, Port),

  ok.

post_init(_ProviderName, _Args) ->
  ok.

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
  Pid = case get(pid) of
          undefined ->
            {ok, Pid1} = riakc_pb_socket:start_link(host(), port()),
            put(pid, Pid1),
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
  case riakc_pb_socket:get(Pid, ?BUCKET, IdBin, [{r, one}]) of
    {ok, G0} ->
      State = riakc_obj:get_value(G0),
      {ok, {Id, Type, RefHash, State}};
    _ ->
      error
  end.

read_by_hash_(_Hash, _Type, _C) ->
  error.

insert_(Id, _Type, _RefHash, _GrainETag, GrainState, Pid) ->
  IdBin = term_to_binary(Id),
  O1 = riakc_obj:new(?BUCKET, IdBin),
  O2 = riakc_obj:update_value(O1, term_to_binary(GrainState)),
  case riakc_pb_socket:put(Pid, O2, [return_body]) of
    {ok, _PO2} ->
      ok;
    _ ->
      error
  end.

update_(Id, _Type, _RefHash, _OldGrainETag, _NewGrainETag, GrainState, Pid) ->
  IdBin = term_to_binary(Id),
  case riakc_pb_socket:get(Pid, ?BUCKET, IdBin, [{r, one}]) of
    {ok, G0} ->
      O = riakc_obj:update_value(G0, term_to_binary(GrainState)),
      case riakc_pb_socket:put(Pid, O, [return_body]) of
        {ok, _PO} ->
          ok;
        _ ->
          error
      end;
    _ ->
      error
  end.

host() ->
  case get(host) of
    undefined ->
      "127.0.0.1";
    Host ->
      Host
  end.

port() ->
  case get(port) of
    undefined ->
      8087;
    Port ->
      Port
  end.
