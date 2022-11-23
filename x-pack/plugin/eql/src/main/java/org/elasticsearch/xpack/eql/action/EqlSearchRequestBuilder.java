/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.action;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.index.query.QueryBuilder;

public class EqlSearchRequestBuilder extends ActionRequestBuilder<EqlSearchRequest, EqlSearchResponse> {
    public EqlSearchRequestBuilder(ElasticsearchClient client, EqlSearchAction action) {
        super(client, action, new EqlSearchRequest());
    }

    public EqlSearchRequestBuilder indices(String... indices) {
        request.indices(indices);
        return this;
    }

    public EqlSearchRequestBuilder filter(QueryBuilder filter) {
        request.filter(filter);
        return this;
    }

    public EqlSearchRequestBuilder timestampField(String timestampField) {
        request.timestampField(timestampField);
        return this;
    }

    public EqlSearchRequestBuilder tiebreakerField(String tiebreakerField) {
        request.tiebreakerField(tiebreakerField);
        return this;
    }

    public EqlSearchRequestBuilder eventCategoryField(String eventCategoryField) {
        request.eventCategoryField(eventCategoryField);
        return this;
    }

    public EqlSearchRequestBuilder size(int size) {
        request.size(size);
        return this;
    }

    public EqlSearchRequestBuilder fetchSize(int fetchSize) {
        request.fetchSize(fetchSize);
        return this;
    }

    public EqlSearchRequestBuilder query(String query) {
        request.query(query);
        return this;
    }
}
