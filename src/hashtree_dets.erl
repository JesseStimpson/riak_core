%% @doc This storage engine for hashtree is not intended to be used for any real purpose.
%%
%% It is an experimental change that allows LevelDB to be removed from deps.
%%
%% It passes unit tests, but hasn't been checked for performance or accuracy. It's very likely to
%% cause your system to run out of memory on any modestly sized dataset, because the entire table
%% is loaded into memory when an iterator is created.
-module(hashtree_dets).
-behaviour(hashtree_storage).

-export([open/1,
         close/1,
         destroy/1,
         get/2,
         put/3,
         write/2,
         delete/2,
         fold/4,
         iterator/1,
         iterator_move/2,
         iterator_close/1]).

-define(ItrEntry, '$hashtree_dets_iterator').
-include_lib("eunit/include/eunit.hrl").

open(Path) ->
    ok = filelib:ensure_path(Path),
    File = filename:join([Path, "hashtree.dets"]),
    % File is equivalent to both name and ref (required for destroy)
    dets:open_file(File, [{file, File}]).

close(Ref) ->
    dets:close(Ref).

destroy(Ref) ->
    File = case re:run(Ref, "hashtree\\.dets$", [{capture, none}]) of
        match ->
            Ref;
        nomatch ->
            filename:join([Ref, "hashtree.dets"])
    end,
    case file:delete(File) of
        {error, enoent} ->
            ok;
        Res ->
            Res
    end.

get(Ref, Key) ->
    case dets:lookup(Ref, Key) of
        [{_, Value}] ->
            {ok, Value};
        [] ->
            not_found;
        Error={error, _Reason} ->
            Error
    end.

put(Ref, Key, Value) ->
    dets:insert(Ref, {Key, Value}).

write(_Ref, []) ->
    ok;
write(Ref, [{put, Key, Value}|WriteActions]) ->
    case put(Ref, Key, Value) of
        ok ->
            write(Ref, WriteActions);
        Error ->
            Error
    end;
write(Ref, [{delete, Key}|WriteActions]) ->
    case delete(Ref, Key) of
        ok ->
            write(Ref, WriteActions);
        Error ->
            Error
    end.

delete(Ref, Key) ->
    dets:delete(Ref, Key).

fold(Ref, Fun, Acc, FirstKey) ->
    hashtree_storage:fold_via_iterator(?MODULE, Ref, Fun, Acc, FirstKey).

iterator(Ref) ->
    Ets = ets:new(?MODULE, [ordered_set]),
    Ets = dets:to_ets(Ref, Ets),
    {ok, Ets}.

iterator_move(Ets, prefetch) ->
    Next = case ets:lookup(Ets, ?ItrEntry) of
        [] ->
            ets:first(Ets);
        [{?ItrEntry, K}] ->
            iterator_next(Ets, K)
    end,
    true = ets:insert(Ets, {?ItrEntry, Next}),
    case ets:lookup(Ets, Next) of
        [{Next, V}] ->
            {ok, Next, V};
        [] ->
            {error, invalid_iterator}
    end;
iterator_move(Ets, prefetch_stop) ->
    case ets:lookup(Ets, ?ItrEntry) of
        [] ->
            {error, invalid_iterator};
        [{?ItrEntry, K}] ->
            case ets:lookup(Ets, K) of
                [{K, V}] ->
                    {ok, K, V};
                [] ->
                    {error, invalid_iterator}
            end
    end;
iterator_move(Ets, SeekKey) when is_binary(SeekKey) ->
    true = ets:insert(Ets, {?ItrEntry, SeekKey}),
    iterator_move(Ets, prefetch).

iterator_close(Ets) ->
    true = ets:delete(Ets),
    ok.

iterator_next(Ets, Key1) ->
    % skip the metadata entry
    case ets:next(Ets, Key1) of
        ?ItrEntry=Key2 ->
            iterator_next(Ets, Key2);
        K2 ->
            K2
    end.
