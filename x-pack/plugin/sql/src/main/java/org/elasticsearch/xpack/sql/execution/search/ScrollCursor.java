/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.ql.execution.search.extractor.HitExtractor;
import org.elasticsearch.xpack.ql.type.Schema;
import org.elasticsearch.xpack.sql.session.Configuration;
import org.elasticsearch.xpack.sql.session.Cursor;
import org.elasticsearch.xpack.sql.session.Rows;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.elasticsearch.action.ActionListener.wrap;

public class ScrollCursor implements Cursor {

    private final Logger log = LogManager.getLogger(getClass());

    public static final String NAME = "s";

    private final String scrollId;
    private final List<HitExtractor> extractors;
    private final BitSet mask;
    private final int limit;

    public ScrollCursor(String scrollId, List<HitExtractor> extractors, BitSet mask, int limit) {
        this.scrollId = scrollId;
        this.extractors = extractors;
        this.mask = mask;
        this.limit = limit;
    }

    public ScrollCursor(StreamInput in) throws IOException {
        scrollId = in.readString();
        limit = in.readVInt();

        extractors = in.readNamedWriteableList(HitExtractor.class);
        mask = BitSet.valueOf(in.readByteArray());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(scrollId);
        out.writeVInt(limit);

        out.writeNamedWriteableList(extractors);
        out.writeByteArray(mask.toByteArray());
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    String scrollId() {
        return scrollId;
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
    @Override
    public void nextPage(Configuration cfg, Client client, NamedWriteableRegistry registry, ActionListener<Page> listener) {
        if (log.isTraceEnabled()) {
            log.trace("About to execute scroll query {}", scrollId);
        }

        SearchScrollRequest request = new SearchScrollRequest(scrollId).scroll(cfg.pageTimeout());
        client.searchScroll(request, wrap(response -> {
            handle(response, () -> new SearchHitRowSet(extractors, mask, limit, response),
                    p -> listener.onResponse(p),
                    p -> clear(cfg, client, wrap(success -> listener.onResponse(p), listener::onFailure)),
                    Schema.EMPTY);
        }, listener::onFailure));
    }

    @Override
    public void clear(Configuration cfg, Client client, ActionListener<Boolean> listener) {
        cleanCursor(client, scrollId, wrap(
                        clearScrollResponse -> listener.onResponse(clearScrollResponse.isSucceeded()),
                        listener::onFailure));
    }
    
    static void handle(SearchResponse response, Supplier<SearchHitRowSet> makeRowHit, Consumer<Page> onPage, Consumer<Page> clearScroll,
            Schema schema) {
        SearchHit[] hits = response.getHits().getHits();
        // clean-up
        if (hits.length > 0) {
            SearchHitRowSet rowSet = makeRowHit.get();
            Tuple<String, Integer> nextScrollData = rowSet.nextScrollData();

            if (nextScrollData == null) {
                // no more data, let's clean the scroll before continuing
                clearScroll.accept(Page.last(rowSet));
            } else {
                Cursor next = new ScrollCursor(nextScrollData.v1(), rowSet.extractors(), rowSet.mask(), nextScrollData.v2());
                onPage.accept(new Page(rowSet, next));
            }
        }
        // no-hits
        else {
            clearScroll.accept(Page.last(Rows.empty(schema)));
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        ScrollCursor other = (ScrollCursor) obj;
        return Objects.equals(scrollId, other.scrollId)
                && Objects.equals(extractors, other.extractors)
                && Objects.equals(limit, other.limit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scrollId, extractors, limit);
    }

    @Override
    public String toString() {
        return "cursor for scroll [" + scrollId + "]";
    }

    public static void cleanCursor(Client client, String scrollId, ActionListener<ClearScrollResponse> listener) {
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        client.clearScroll(clearScrollRequest, listener);
    }
}
