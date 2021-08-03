/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.searchafter.SearchAfterBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;

import java.io.IOException;
import java.util.List;

public final class QueryApiKeyRequest extends ActionRequest {

    @Nullable
    private final QueryBuilder queryBuilder;
    private final int from;
    private final int size;
    @Nullable
    private final List<FieldSortBuilder> fieldSortBuilders;
    @Nullable
    private final SearchAfterBuilder searchAfterBuilder;
    private boolean filterForCurrentUser;

    public QueryApiKeyRequest() {
        this((QueryBuilder) null);
    }

    public QueryApiKeyRequest(QueryBuilder queryBuilder) {
        this(queryBuilder, -1, -1, null, null);
    }

    public QueryApiKeyRequest(StreamInput in) throws IOException {
        super(in);
        this.queryBuilder = in.readOptionalNamedWriteable(QueryBuilder.class);
        this.from = in.readInt();
        this.size = in.readInt();
        if (in.readBoolean()) {
            this.fieldSortBuilders = in.readList(FieldSortBuilder::new);
        } else {
            this.fieldSortBuilders = null;
        }
        this.searchAfterBuilder = in.readOptionalWriteable(SearchAfterBuilder::new);
    }

    public QueryApiKeyRequest(
        @Nullable QueryBuilder queryBuilder,
        @Nullable Integer from,
        @Nullable Integer size,
        @Nullable List<FieldSortBuilder> fieldSortBuilders,
        @Nullable SearchAfterBuilder searchAfterBuilder
    ) {
        this.queryBuilder = queryBuilder;
        this.from = from == null ? -1 : from;
        this.size = size == null ? -1 : size;
        this.fieldSortBuilders = fieldSortBuilders;
        this.searchAfterBuilder = searchAfterBuilder;
    }

    public QueryBuilder getQueryBuilder() {
        return queryBuilder;
    }

    public int getFrom() {
        return from;
    }

    public int getSize() {
        return size;
    }

    public List<FieldSortBuilder> getFieldSortBuilders() {
        return fieldSortBuilders;
    }

    public SearchAfterBuilder getSearchAfterBuilder() {
        return searchAfterBuilder;
    }

    public boolean isFilterForCurrentUser() {
        return filterForCurrentUser;
    }

    public void setFilterForCurrentUser() {
        filterForCurrentUser = true;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalNamedWriteable(queryBuilder);
        out.writeInt(from);
        out.writeInt(size);
        if (fieldSortBuilders == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeList(fieldSortBuilders);
        }
        out.writeOptionalWriteable(searchAfterBuilder);
    }
}
