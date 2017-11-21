/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search;


import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.Client;
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
        extractors = in.readNamedWriteableList(HitExtractor.class);
        limit = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(scrollId);
        out.writeNamedWriteableList(extractors);
        out.writeVInt(limit);
    }


    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void nextPage(Configuration cfg, Client client, ActionListener<RowSet> listener) {
        SearchScrollRequest request = new SearchScrollRequest(scrollId).scroll(cfg.pageTimeout());
        client.searchScroll(request, ActionListener.wrap((SearchResponse response) -> {
            int limitHits = limit;
            listener.onResponse(new ScrolledSearchHitRowSet(extractors, response.getHits().getHits(),
                    limitHits, response.getScrollId()));
        }, listener::onFailure));
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
}
