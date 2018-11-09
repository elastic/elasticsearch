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
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.sql.execution.search.extractor.HitExtractor;
import org.elasticsearch.xpack.sql.session.Configuration;
import org.elasticsearch.xpack.sql.session.Cursor;
import org.elasticsearch.xpack.sql.session.RowSet;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class ScrollCursor implements Cursor {

    private final Logger log = LogManager.getLogger(getClass());

    public static final String NAME = "s";

    private final String scrollId;
    private final List<HitExtractor> extractors;
    private final int limit;

    public ScrollCursor(String scrollId, List<HitExtractor> extractors, int limit) {
        this.scrollId = scrollId;
        this.extractors = extractors;
        this.limit = limit;
    }

    public ScrollCursor(StreamInput in) throws IOException {
        scrollId = in.readString();
        limit = in.readVInt();

        extractors = in.readNamedWriteableList(HitExtractor.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(scrollId);
        out.writeVInt(limit);

        out.writeNamedWriteableList(extractors);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    String scrollId() {
        return scrollId;
    }

    List<HitExtractor> extractors() {
        return extractors;
    }

    int limit() {
        return limit;
    }
    @Override
    public void nextPage(Configuration cfg, Client client, NamedWriteableRegistry registry, ActionListener<RowSet> listener) {
        log.trace("About to execute scroll query {}", scrollId);

        SearchScrollRequest request = new SearchScrollRequest(scrollId).scroll(cfg.pageTimeout());
        client.searchScroll(request, ActionListener.wrap((SearchResponse response) -> {
            SearchHitRowSet rowSet = new SearchHitRowSet(extractors, response.getHits().getHits(),
                    limit, response.getScrollId());
            if (rowSet.nextPageCursor() == Cursor.EMPTY ) {
                // we are finished with this cursor, let's clean it before continuing
                clear(cfg, client, ActionListener.wrap(success -> listener.onResponse(rowSet), listener::onFailure));
            } else {
                listener.onResponse(rowSet);
            }
        }, listener::onFailure));
    }

    @Override
    public void clear(Configuration cfg, Client client, ActionListener<Boolean> listener) {
        cleanCursor(client, scrollId,
                ActionListener.wrap(
                        clearScrollResponse -> listener.onResponse(clearScrollResponse.isSucceeded()),
                        listener::onFailure));
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
