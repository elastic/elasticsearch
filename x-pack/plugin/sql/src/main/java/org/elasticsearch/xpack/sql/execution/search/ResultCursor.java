/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.ql.type.Schema;
import org.elasticsearch.xpack.sql.session.Cursors;
import org.elasticsearch.xpack.sql.session.SqlConfiguration;
import org.elasticsearch.xpack.sql.session.Cursor;
import org.elasticsearch.xpack.sql.session.Rows;
import org.elasticsearch.xpack.sql.util.Check;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.elasticsearch.action.ActionListener.wrap;

public abstract class ResultCursor<E extends NamedWriteable> implements Cursor {

    private final String[] indices;
    private final byte[] nextQuery;
    private final List<E> extractors;
    private final BitSet mask;
    private final int limit;
    private final boolean includeFrozen;

    ResultCursor(@Nullable byte[] next, List<E> exts, BitSet mask, int remainingLimit, boolean includeFrozen, String... indices) {
        this.indices = indices;
        this.nextQuery = next;
        this.extractors = exts;
        this.mask = mask;
        this.limit = remainingLimit;
        this.includeFrozen = includeFrozen;
    }

    ResultCursor(List<E> exts, BitSet mask, int remainingLimit, boolean includeFrozen, String... indices) {
        this(null, exts, mask, remainingLimit, includeFrozen, indices);
    }

    public ResultCursor(StreamInput in) throws IOException {
        indices = in.readStringArray();
        nextQuery = in.readByteArray();
        limit = in.readVInt();

        extractors = in.readNamedWriteableList(reify(this));
        mask = BitSet.valueOf(in.readByteArray());
        includeFrozen = in.readBoolean();
    }

    @SuppressWarnings("unchecked")
    private static <E extends NamedWriteable> Class<E> reify(ResultCursor<E> instance) {
        Class<E> clazz = null;
        for (Class<E> crr = (Class<E>)instance.getClass(); crr.equals(ResultCursor.class) == false; crr = (Class<E>) crr.getSuperclass()) {
            clazz = crr;
        }
        assert clazz != null; // since this class is abstract
        return (Class<E>) ((ParameterizedType)(clazz.getGenericSuperclass())).getActualTypeArguments()[0];
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringArray(indices);
        out.writeByteArray(nextQuery);
        out.writeVInt(limit);

        out.writeNamedWriteableList(extractors);
        out.writeByteArray(mask.toByteArray());
        out.writeBoolean(includeFrozen);
    }

    String[] indices() {
        return indices;
    }

    byte[] next() {
        return nextQuery;
    }

    BitSet mask() {
        return mask;
    }

    List<E> extractors() {
        return extractors;
    }

    int limit() {
        return limit;
    }

    boolean includeFrozen() {
        return includeFrozen;
    }

    @Override
    public void nextPage(SqlConfiguration cfg, Client client, NamedWriteableRegistry registry, ActionListener<Page> listener) {
        SearchSourceBuilder query;
        try {
            query = Cursors.deserializeQuery(registry, nextQuery);
        } catch (Exception ex) {
            listener.onFailure(ex);
            return;
        }

        SearchRequest request = Querier.prepareRequest(client, query, cfg.pageTimeout(), includeFrozen, indices);

        client.search(request, new ActionListener<>() {
            @Override
            public void onResponse(SearchResponse response) {
                try {
                    doHandle(response, request.source(),
                        makeRowSet(response),
                        () -> client.search(request, this),
                        listener::onResponse,
                        p -> Querier.closePointInTime(client, response.pointInTimeId(),
                            wrap(success -> listener.onResponse(p), listener::onFailure)),
                        Schema.EMPTY);
                } catch (Exception ex) {
                    listener.onFailure(ex);
                }
            }

            @Override
            public void onFailure(Exception ex) {
                listener.onFailure(ex);
            }
        });
    }

    protected abstract Supplier<ResultRowSet<E>> makeRowSet(SearchResponse response);

    protected abstract BiFunction<byte[], ResultRowSet<E>, ResultCursor<E>> makeCursor();

    static <E extends NamedWriteable, RS extends ResultRowSet<E>, RC extends ResultCursor<E>> void handle(
            SearchResponse response,
            SearchSourceBuilder source,
            Supplier<RS> makeRowSet,
            Supplier<RC> makeCursor,
            Runnable retry,
            Consumer<Page> onPage,
            Consumer<Page> onEnd,
            Schema schema) throws Exception {
        makeCursor.get().doHandle(response, source, makeRowSet, retry, onPage, onEnd, schema);
    }

    <RS extends ResultRowSet<E>> void doHandle(
            SearchResponse response, SearchSourceBuilder source,
            Supplier<RS> makeRowSet,
            @Nullable
            Runnable retry,
            Consumer<Page> onPage,
            Consumer<Page> onEnd,
            Schema schema) throws Exception {

        // there are some results
        if (hasResults(response)) {
            // retry
            if (shouldRetryUpdatedRequest(response, source)) {
                retry.run();
                return;
            }

            RS rowSet = makeRowSet.get();

            if (rowSet.remainingLimit() == 0) {
                onEnd.accept(new Page(rowSet, Cursor.EMPTY));
            } else {
                assert source.pointInTimeBuilder() != null;
                // update the PIT ID from answer, but don't extend the PIT timeout
                source.pointInTimeBuilder(new PointInTimeBuilder(response.pointInTimeId()));

                updateSourceAfterKey(rowSet, source);

                byte[] queryAsBytes = Cursors.serializeQuery(source);
                Cursor next = makeCursor().apply(queryAsBytes, rowSet);

                onPage.accept(new Page(rowSet, next));
            }
        }
        // no results
        else {
            onEnd.accept(Page.last(Rows.empty(schema)));
        }
    }

    protected abstract boolean hasResults(SearchResponse response);

    protected boolean shouldRetryUpdatedRequest(SearchResponse response, SearchSourceBuilder source) {
        return false;
    }

    protected abstract void updateSourceAfterKey(ResultRowSet<E> rowSet, SearchSourceBuilder source);

    @Override
    public void clear(SqlConfiguration cfg, Client client, NamedWriteableRegistry registry, ActionListener<Boolean> listener) {
        SearchSourceBuilder query;
        try {
            query = Cursors.deserializeQuery(registry, nextQuery);
            Check.isTrue(query.pointInTimeBuilder() != null, "Invalid cursor: missing point-in-time ID");
        } catch (Exception ex) {
            listener.onFailure(ex);
            return;
        }
        String pointInTimeId = query.pointInTimeBuilder().getId();
        Querier.closePointInTime(client, pointInTimeId, listener);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(indices), Arrays.hashCode(nextQuery), extractors, limit, mask, includeFrozen);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        ResultCursor<E> other = (ResultCursor<E>) obj;
        return Arrays.equals(indices, other.indices)
                && Arrays.equals(nextQuery, other.nextQuery)
                && Objects.equals(extractors, other.extractors)
                && Objects.equals(limit, other.limit)
                && Objects.equals(includeFrozen, other.includeFrozen);
    }
}
