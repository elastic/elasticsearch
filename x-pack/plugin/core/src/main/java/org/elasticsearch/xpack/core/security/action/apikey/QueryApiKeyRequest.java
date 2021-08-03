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

import java.io.IOException;

public final class QueryApiKeyRequest extends ActionRequest {

    @Nullable
    private final QueryBuilder queryBuilder;
    private boolean filterForCurrentUser;

    public QueryApiKeyRequest() {
        this((QueryBuilder) null);
    }

    public QueryApiKeyRequest(QueryBuilder queryBuilder) {
        this.queryBuilder = queryBuilder;
    }

    public QueryApiKeyRequest(StreamInput in) throws IOException {
        super(in);
        queryBuilder = in.readOptionalNamedWriteable(QueryBuilder.class);
    }

    public QueryBuilder getQueryBuilder() {
        return queryBuilder;
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
    }
}
