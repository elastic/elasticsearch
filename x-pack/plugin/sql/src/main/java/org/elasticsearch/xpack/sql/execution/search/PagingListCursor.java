/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.execution.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.sql.session.Configuration;
import org.elasticsearch.xpack.sql.session.Cursor;
import org.elasticsearch.xpack.sql.session.RowSet;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;

public class PagingListCursor implements Cursor {

    public static final String NAME = "p";

    private final List<List<?>> data;
    private final int pageSize;

    PagingListCursor(List<List<?>> data, int pageSize) {
        this.data = data;
        this.pageSize = pageSize;
    }

    @SuppressWarnings("unchecked")
    public PagingListCursor(StreamInput in) throws IOException {
        data = (List<List<?>>) in.readGenericValue();
        pageSize = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeGenericValue(data);
        out.writeVInt(pageSize);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    List<List<?>> data() {
        return data;
    }

    int pageSize() {
        return pageSize;
    }

    @Override
    public void nextPage(Configuration cfg, Client client, NamedWriteableRegistry registry, ActionListener<RowSet> listener) {
        // the check is really a safety measure since the page initialization handles it already (by returning an empty cursor)
        List<List<?>> nextData = data.size() > pageSize ? data.subList(pageSize, data.size()) : emptyList();
        listener.onResponse(new PagingListRowSet(nextData, pageSize));
    }

    @Override
    public void clear(Configuration cfg, Client client, ActionListener<Boolean> listener) {
        listener.onResponse(true);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data, pageSize);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        PagingListCursor other = (PagingListCursor) obj;
        return Objects.equals(pageSize, other.pageSize) && Objects.equals(data, other.data);
    }

    @Override
    public String toString() {
        return "cursor for paging list";
    }
}