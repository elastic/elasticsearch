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
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.sql.execution.search.extractor.HitExtractor;
import org.elasticsearch.xpack.sql.execution.search.extractor.HitExtractors;
import org.elasticsearch.xpack.sql.session.Configuration;
import org.elasticsearch.xpack.sql.session.Cursor;
import org.elasticsearch.xpack.sql.session.RowSet;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Base64;
import java.util.List;
import java.util.Objects;

public class ScrollCursor implements Cursor {
    public static final String NAME = "s";
    /**
     * {@link NamedWriteableRegistry} used to resolve the {@link #extractors}.
     */
    private static final NamedWriteableRegistry REGISTRY = new NamedWriteableRegistry(HitExtractors.getNamedWriteables());

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

    public ScrollCursor(java.io.Reader reader) throws IOException {
        StringBuffer scrollId = new StringBuffer();
        int c;
        while ((c = reader.read()) != -1 && c != ':') {
            scrollId.append((char) c);
        }
        this.scrollId = scrollId.toString();
        if (c == -1) {
            throw new IllegalArgumentException("invalid cursor");
        }
        try (StreamInput delegate = new InputStreamStreamInput(Base64.getDecoder().wrap(new InputStream() {
            @Override
            public int read() throws IOException {
                int c = reader.read();
                if (c < -1 || c > 0xffff) {
                    throw new IllegalArgumentException("invalid cursor [" + Integer.toHexString(c) + "]");
                }
                return c;
            }
        })); StreamInput in = new NamedWriteableAwareStreamInput(delegate, REGISTRY)) {
            extractors = in.readNamedWriteableList(HitExtractor.class);
            limit = in.readVInt();
        }
    }

    @Override
    public void writeTo(java.io.Writer writer) throws IOException {
        writer.write(scrollId);
        writer.write(':');
        try (StreamOutput out = new OutputStreamStreamOutput(Base64.getEncoder().wrap(new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                writer.write(b);
            }
        }))) {
            out.writeNamedWriteableList(extractors);
            out.writeVInt(limit);
        }
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
        return "cursor for scoll [" + scrollId + "]";
    }
}
