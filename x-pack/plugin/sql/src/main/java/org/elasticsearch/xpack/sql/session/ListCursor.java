/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.session;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.ql.type.Schema;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;

public class ListCursor implements Cursor {

    public static final String NAME = "l";

    private final List<List<?>> data;
    private final int columnCount;
    private final int pageSize;

    public ListCursor(List<List<?>> data, int pageSize, int columnCount) {
        this.data = data;
        this.columnCount = columnCount;
        this.pageSize = pageSize;
    }

    @SuppressWarnings("unchecked")
    public ListCursor(StreamInput in) throws IOException {
        data = (List<List<?>>) in.readGenericValue();
        columnCount = in.readVInt();
        pageSize = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeGenericValue(data);
        out.writeVInt(columnCount);
        out.writeVInt(pageSize);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    List<List<?>> data() {
        return data;
    }

    int columnCount() {
        return columnCount;
    }

    int pageSize() {
        return pageSize;
    }

    public static Page of(Schema schema, List<List<?>> data, int pageSize) {
        return of(schema, data, pageSize, schema.size());
    }

    // NB: private since the columnCount is for public cases inferred by the columnCount
    // only on the next-page the schema becomes null however that's an internal detail hence
    // why this method is not exposed
    private static Page of(Schema schema, List<List<?>> data, int pageSize, int columnCount) {
        List<List<?>> nextData = data.size() > pageSize ? data.subList(pageSize, data.size()) : emptyList();
        Cursor next = nextData.isEmpty()
                ? Cursor.EMPTY
                : new ListCursor(nextData, pageSize, columnCount);
        List<List<?>> currData = data.isEmpty() || pageSize == 0
                ? emptyList()
                : data.size() == pageSize ? data : data.subList(0, Math.min(pageSize, data.size()));
        return new Page(new ListRowSet(schema, currData, columnCount), next);
    }
    
    @Override
    public void nextPage(Configuration cfg, Client client, NamedWriteableRegistry registry, ActionListener<Page> listener) {
        listener.onResponse(of(Schema.EMPTY, data, pageSize, columnCount));
    }

    @Override
    public void clear(Configuration cfg, Client client, ActionListener<Boolean> listener) {
        listener.onResponse(true);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data, columnCount, pageSize);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ListCursor other = (ListCursor) obj;
        return Objects.equals(pageSize, other.pageSize)
                && Objects.equals(columnCount, other.columnCount)
                && Objects.equals(data, other.data);
    }

    @Override
    public String toString() {
        return "cursor for paging list";
    }
}