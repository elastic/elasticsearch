/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.execution.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.ql.execution.search.extractor.HitExtractor;
import org.elasticsearch.xpack.ql.util.StringUtils;
import org.elasticsearch.xpack.sql.session.Cursor;
import org.elasticsearch.xpack.sql.session.SqlConfiguration;

import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public class SearchHitCursor implements Cursor {

    private static final Logger log = LogManager.getLogger(SearchHitCursor.class);

    public static final String NAME = "s";

    private final byte[] nextQuery;
    private final List<HitExtractor> extractors;
    private final BitSet mask;
    private final int limit;
    private final boolean includeFrozen;

    SearchHitCursor(byte[] nextQuery, List<HitExtractor> exts, BitSet mask, int remainingLimit, boolean includeFrozen) {
        this.nextQuery = nextQuery;
        this.extractors = exts;
        this.mask = mask;
        this.limit = remainingLimit;
        this.includeFrozen = includeFrozen;
    }

    public SearchHitCursor(StreamInput in) throws IOException {
        nextQuery = in.readByteArray();
        limit = in.readVInt();

        extractors = in.readNamedWriteableList(HitExtractor.class);
        mask = BitSet.valueOf(in.readByteArray());
        includeFrozen = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByteArray(nextQuery);
        out.writeVInt(limit);

        out.writeNamedWriteableList(extractors);
        out.writeByteArray(mask.toByteArray());
        out.writeBoolean(includeFrozen);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    byte[] next() {
        return nextQuery;
    }

    BitSet mask() {
        return mask;
    }

    List<HitExtractor> extractors() {
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
        SearchSourceBuilder q;
        try {
            q = deserializeQuery(registry, nextQuery);
        } catch (Exception ex) {
            listener.onFailure(ex);
            return;
        }

        SearchSourceBuilder query = q;
        if (log.isTraceEnabled()) {
            log.trace("About to execute composite query {}", StringUtils.toString(query));
        }

        SearchRequest request = Querier.prepareRequest(query, cfg.requestTimeout(), includeFrozen);

        client.search(
            request,
            ActionListener.wrap(
                (SearchResponse response) -> handle(
                    client,
                    response,
                    request.source(),
                    makeRowSet(query.size(), response),
                    listener,
                    includeFrozen
                ),
                listener::onFailure
            )
        );
    }

    protected Supplier<SearchHitRowSet> makeRowSet(int sizeRequested, SearchResponse response) {
        return () -> new SearchHitRowSet(extractors, mask, sizeRequested, limit, response);
    }

    static void handle(
        Client client,
        SearchResponse response,
        SearchSourceBuilder source,
        Supplier<SearchHitRowSet> makeRowSet,
        ActionListener<Page> listener,
        boolean includeFrozen
    ) {

        if (log.isTraceEnabled()) {
            Querier.logSearchResponse(response, log);
        }

        SearchHit[] hits = response.getHits().getHits();

        SearchHitRowSet rowSet = makeRowSet.get();

        if (rowSet.hasRemaining() == false) {
            Querier.closePointInTime(
                client,
                response.pointInTimeId(),
                ActionListener.wrap(r -> listener.onResponse(Page.last(rowSet)), listener::onFailure)
            );
        } else {
            source.pointInTimeBuilder(new PointInTimeBuilder(response.pointInTimeId()));
            updateSearchAfter(hits, source);

            byte[] nextQuery;
            try {
                nextQuery = serializeQuery(source);
            } catch (IOException e) {
                listener.onFailure(e);
                return;
            }

            SearchHitCursor nextCursor = new SearchHitCursor(
                nextQuery,
                rowSet.extractors(),
                rowSet.mask(),
                rowSet.getRemainingLimit(),
                includeFrozen
            );
            listener.onResponse(new Page(rowSet, nextCursor));
        }
    }

    private static void updateSearchAfter(SearchHit[] hits, SearchSourceBuilder source) {
        assert hits.length > 0;
        SearchHit lastHit = hits[hits.length - 1];
        source.searchAfter(lastHit.getSortValues());
    }

    /**
     * Deserializes the search source from a byte array.
     */
    private static SearchSourceBuilder deserializeQuery(NamedWriteableRegistry registry, byte[] source) throws IOException {
        try (NamedWriteableAwareStreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(source), registry)) {
            return new SearchSourceBuilder(in);
        }
    }

    /**
     * Serializes the search source to a byte array.
     */
    private static byte[] serializeQuery(SearchSourceBuilder source) throws IOException {
        if (source == null) {
            return new byte[0];
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            source.writeTo(out);
            return BytesReference.toBytes(out.bytes());
        }
    }

    @Override
    public void clear(Client client, ActionListener<Boolean> listener) {
        listener.onResponse(true);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(nextQuery), extractors, limit, mask, includeFrozen);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        SearchHitCursor other = (SearchHitCursor) obj;
        return Arrays.equals(nextQuery, other.nextQuery)
            && Objects.equals(extractors, other.extractors)
            && Objects.equals(limit, other.limit)
            && Objects.equals(includeFrozen, other.includeFrozen);
    }
}
