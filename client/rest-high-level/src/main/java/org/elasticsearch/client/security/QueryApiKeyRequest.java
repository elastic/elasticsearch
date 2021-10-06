/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ValidationException;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.searchafter.SearchAfterBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class QueryApiKeyRequest implements Validatable, ToXContentObject {

    @Nullable
    private QueryBuilder queryBuilder;
    private Integer from;
    private Integer size;
    @Nullable
    private List<FieldSortBuilder> fieldSortBuilders;
    @Nullable
    private SearchAfterBuilder searchAfterBuilder;

    public QueryApiKeyRequest() {
        this(null, null, null, null, null);
    }

    public QueryApiKeyRequest(
        @Nullable QueryBuilder queryBuilder,
        @Nullable Integer from,
        @Nullable Integer size,
        @Nullable List<FieldSortBuilder> fieldSortBuilders,
        @Nullable SearchAfterBuilder searchAfterBuilder) {
        this.queryBuilder = queryBuilder;
        this.from = from;
        this.size = size;
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

    public QueryApiKeyRequest queryBuilder(QueryBuilder queryBuilder) {
        this.queryBuilder = queryBuilder;
        return this;
    }

    public QueryApiKeyRequest from(int from) {
        this.from = from;
        return this;
    }

    public QueryApiKeyRequest size(int size) {
        this.size = size;
        return this;
    }

    public QueryApiKeyRequest fieldSortBuilders(List<FieldSortBuilder> fieldSortBuilders) {
        this.fieldSortBuilders = fieldSortBuilders;
        return this;
    }

    public QueryApiKeyRequest searchAfterBuilder(SearchAfterBuilder searchAfterBuilder) {
        this.searchAfterBuilder = searchAfterBuilder;
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (queryBuilder != null) {
            builder.field("query");
            queryBuilder.toXContent(builder, params);
        }
        if (from != null) {
            builder.field("from", from);
        }
        if (size != null) {
            builder.field("size", size);
        }
        if (fieldSortBuilders != null && false == fieldSortBuilders.isEmpty()) {
            builder.field("sort", fieldSortBuilders);
        }
        if (searchAfterBuilder != null) {
            builder.array(SearchAfterBuilder.SEARCH_AFTER.getPreferredName(), searchAfterBuilder.getSortValues());
        }
        return builder.endObject();
    }

    @Override
    public Optional<ValidationException> validate() {
        ValidationException validationException = null;
        if (from != null && from < 0) {
            validationException = addValidationError(validationException, "from must be non-negative");
        }
        if (size != null && size < 0) {
            validationException = addValidationError(validationException, "size must be non-negative");
        }
        return validationException == null ? Optional.empty() : Optional.of(validationException);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        QueryApiKeyRequest that = (QueryApiKeyRequest) o;
        return Objects.equals(queryBuilder, that.queryBuilder) && Objects.equals(from, that.from) && Objects.equals(
            size,
            that.size) && Objects.equals(fieldSortBuilders, that.fieldSortBuilders) && Objects.equals(
            searchAfterBuilder,
            that.searchAfterBuilder);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryBuilder, from, size, fieldSortBuilders, searchAfterBuilder);
    }

    private ValidationException addValidationError(ValidationException validationException, String message) {
        if (validationException == null) {
            validationException = new ValidationException();
        }
        validationException.addValidationError(message);
        return validationException;
    }
}
