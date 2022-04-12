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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.ql.execution.search.extractor.HitExtractor;
import org.elasticsearch.xpack.ql.util.StringUtils;
import org.elasticsearch.xpack.sql.session.Cursor;
import org.elasticsearch.xpack.sql.session.SqlConfiguration;
import org.elasticsearch.xpack.sql.util.Check;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.sql.execution.search.Querier.closePointInTime;
import static org.elasticsearch.xpack.sql.execution.search.Querier.logSearchResponse;
import static org.elasticsearch.xpack.sql.execution.search.Querier.prepareRequest;

public class SearchHitCursor implements Cursor {

    private static final Logger log = LogManager.getLogger(SearchHitCursor.class);

    public static final String NAME = "h";

    private final SearchSourceBuilder nextQuery;
    private final List<HitExtractor> extractors;
    private final BitSet mask;
    private final int limit;
    private final boolean includeFrozen;

    SearchHitCursor(SearchSourceBuilder nextQuery, List<HitExtractor> exts, BitSet mask, int remainingLimit, boolean includeFrozen) {
        this.nextQuery = nextQuery;
        this.extractors = exts;
        this.mask = mask;
        this.limit = remainingLimit;
        this.includeFrozen = includeFrozen;
    }

    public SearchHitCursor(StreamInput in) throws IOException {
        nextQuery = new SearchSourceBuilder(in);
        limit = in.readVInt();

        extractors = in.readNamedWriteableList(HitExtractor.class);
        mask = BitSet.valueOf(in.readByteArray());
        includeFrozen = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        nextQuery.writeTo(out);
        out.writeVInt(limit);

        out.writeNamedWriteableList(extractors);
        out.writeByteArray(mask.toByteArray());
        out.writeBoolean(includeFrozen);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    SearchSourceBuilder next() {
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
    public void nextPage(SqlConfiguration cfg, Client client, ActionListener<Page> listener) {
        if (log.isTraceEnabled()) {
            log.trace("About to execute search hit query {}", StringUtils.toString(nextQuery));
        }

        SearchRequest request = prepareRequest(nextQuery, cfg.requestTimeout(), includeFrozen);

        client.search(
            request,
            ActionListener.wrap(
                (SearchResponse response) -> handle(client, response, request.source(), makeRowSet(response), listener, includeFrozen),
                listener::onFailure
            )
        );
    }

    private Supplier<SearchHitRowSet> makeRowSet(SearchResponse response) {
        return () -> new SearchHitRowSet(extractors, mask, nextQuery.size(), limit, response);
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
            logSearchResponse(response, log);
        }

        SearchHit[] hits = response.getHits().getHits();

        SearchHitRowSet rowSet = makeRowSet.get();

        if (rowSet.hasRemaining() == false) {
            closePointInTime(
                client,
                response.pointInTimeId(),
                ActionListener.wrap(r -> listener.onResponse(Page.last(rowSet)), listener::onFailure)
            );
        } else {
            updateSearchAfter(hits, source);

            SearchHitCursor nextCursor = new SearchHitCursor(
                source,
                rowSet.extractors(),
                rowSet.mask(),
                rowSet.getRemainingLimit(),
                includeFrozen
            );
            listener.onResponse(new Page(rowSet, nextCursor));
        }
    }

    private static void updateSearchAfter(SearchHit[] hits, SearchSourceBuilder source) {
        SearchHit lastHit = hits[hits.length - 1];
        source.searchAfter(lastHit.getSortValues());
    }

    @Override
    public void clear(Client client, ActionListener<Boolean> listener) {
        Check.isTrue(nextQuery.pointInTimeBuilder() != null, "Expected cursor with point-in-time id but got null");
        closePointInTime(client, nextQuery.pointInTimeBuilder().getEncodedId(), listener);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nextQuery, extractors, limit, mask, includeFrozen);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        SearchHitCursor other = (SearchHitCursor) obj;
        return Objects.equals(nextQuery, other.nextQuery)
            && Objects.equals(extractors, other.extractors)
            && Objects.equals(limit, other.limit)
            && Objects.equals(includeFrozen, other.includeFrozen);
    }
}
