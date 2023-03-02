-module(hashtree_storage).

-export_type([db_ref/0, itr_ref/0, path/0, key/0, value/0, write_action/0, write_actions/0, iterator_action/0]).

-export([fold_via_iterator/5]).

-type db_ref() :: any().
-type itr_ref() :: any().
-type path() :: filelib:filename_all() | filelib:dirname_all().
-type key() :: binary().
-type value() :: binary().
-type write_action() :: {put, key(), value()} | {delete, key()}.
-type write_actions() :: list(write_action()).
-type iterator_action() :: prefetch | prefetch_stop | key().

-callback open(path()) -> db_ref().
-callback close(db_ref()) -> ok | {error, any()}.
-callback destroy(path()) -> ok | {error, any()}.
-callback get(db_ref(), key()) -> {ok, binary()} | not_found | {error, any()}.
-callback put(db_ref(), key(), value()) -> ok | {error, any()}.
-callback write(db_ref(), write_actions()) -> ok | {error, any()}.
-callback delete(db_ref(), key()) -> ok | {error, any()}.
-callback fold(db_ref(), function(), Acc :: any(), FirstKey :: key()) -> any().

%% @doc returned iterator must behave as a snapshot, and must be stateful
-callback iterator(db_ref()) -> {ok, itr_ref()}.
-callback iterator_move(itr_ref(), iterator_action()) -> {ok, key(), value()} | {error, invalid_iterator}.
-callback iterator_close(itr_ref()) -> ok.

%% @doc A default implementation for fold that is based on the iterator API. Behaviours can call this
%% function or implement their own fold technique
-spec fold_via_iterator(atom(), db_ref(), function(), any(), key()) -> any().
fold_via_iterator(Mod, Ref, Fun, Acc, FirstKey) ->
    {ok, Itr} = Mod:iterator(Ref),
    fold_via_iterator_(Mod, Ref, Fun, Acc, Mod:iterator_move(Itr, FirstKey), Itr).

fold_via_iterator_(Mod, _Ref, _Fun, Acc, {error, invalid_iterator}, Itr) ->
    Mod:iterator_close(Itr),
    Acc;
fold_via_iterator_(Mod, Ref, Fun, Acc, {ok, K, V}, Itr) ->
    fold_via_iterator_(Mod, Ref, Fun, Fun({K, V}, Acc), Mod:iterator_move(Itr, prefetch), Itr).
