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
import org.elasticsearch.xpack.sql.session.Cursor;
import org.elasticsearch.xpack.sql.session.RowSetCursor;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;

public class ScrollCursor implements Cursor {
    public static final String NAME = "s";
    /**
     * {@link NamedWriteableRegistry} used to resolve the {@link #extractors}.
     */
    private static final NamedWriteableRegistry REGISTRY = new NamedWriteableRegistry(HitExtractor.getNamedWriteables());

    private final String scrollId;
    private final List<HitExtractor> extractors;

    public ScrollCursor(String scrollId, List<HitExtractor> extractors) {
        this.scrollId = scrollId;
        this.extractors = extractors;
    }

    public ScrollCursor(StreamInput in) throws IOException {
        scrollId = in.readString();
        extractors = in.readNamedWriteableList(HitExtractor.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(scrollId);
        out.writeNamedWriteableList(extractors);
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
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void nextPage(Client client, ActionListener<RowSetCursor> listener) {
        // Fake the schema for now. We'll try to remove the need later.
        List<String> names = new ArrayList<>(extractors.size());
        List<DataType> dataTypes = new ArrayList<>(extractors.size());
        for (int i = 0; i < extractors.size(); i++) {
            names.add("dummy");
            dataTypes.add(null);
        }
        // NOCOMMIT make schema properly nullable for the second page
        Schema schema = new Schema(names, dataTypes);
        // NOCOMMIT add keep alive to the settings and pass it here
        /* Or something. The trouble is that settings is for *starting*
         * queries, but maybe we should actually have two sets of settings,
         * one for things that are only valid when going to the next page
         * and one that is valid for starting queries.
         */
        SearchScrollRequest request = new SearchScrollRequest(scrollId).scroll(timeValueSeconds(90));
        client.searchScroll(request, ActionListener.wrap((SearchResponse response) -> {
            int limitHits = -1; // NOCOMMIT do a thing with this
            listener.onResponse(new SearchHitRowSetCursor(schema, extractors, response.getHits().getHits(),
                    limitHits, response.getScrollId(), null));
        }, listener::onFailure));
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        ScrollCursor other = (ScrollCursor) obj;
        return Objects.equals(scrollId, other.scrollId)
                && Objects.equals(extractors, other.extractors);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scrollId, extractors);
    }

    @Override
    public String toString() {
        return "cursor for scoll [" + scrollId + "]";
    }
}
