/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.role;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.searchafter.SearchAfterBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public final class QueryRoleRequest extends LegacyActionRequest {

    @Nullable
    private final QueryBuilder queryBuilder;
    @Nullable
    private final Integer from;
    @Nullable
    private final Integer size;
    @Nullable
    private final List<FieldSortBuilder> fieldSortBuilders;
    @Nullable
    private final SearchAfterBuilder searchAfterBuilder;

    public QueryRoleRequest(
        @Nullable QueryBuilder queryBuilder,
        @Nullable Integer from,
        @Nullable Integer size,
        @Nullable List<FieldSortBuilder> fieldSortBuilders,
        @Nullable SearchAfterBuilder searchAfterBuilder
    ) {
        this.queryBuilder = queryBuilder;
        this.from = from;
        this.size = size;
        this.fieldSortBuilders = fieldSortBuilders;
        this.searchAfterBuilder = searchAfterBuilder;
    }

    public QueryBuilder getQueryBuilder() {
        return queryBuilder;
    }

    public Integer getFrom() {
        return from;
    }

    public Integer getSize() {
        return size;
    }

    public List<FieldSortBuilder> getFieldSortBuilders() {
        return fieldSortBuilders;
    }

    public SearchAfterBuilder getSearchAfterBuilder() {
        return searchAfterBuilder;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (from != null && from < 0) {
            validationException = addValidationError("[from] parameter cannot be negative but was [" + from + "]", validationException);
        }
        if (size != null && size < 0) {
            validationException = addValidationError("[size] parameter cannot be negative but was [" + size + "]", validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        TransportAction.localOnly();
    }
}
