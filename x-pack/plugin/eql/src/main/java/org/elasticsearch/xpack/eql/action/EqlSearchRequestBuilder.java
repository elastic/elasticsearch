/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.action;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
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

    public EqlSearchRequestBuilder eventCategoryField(String eventCategoryField) {
        request.eventCategoryField(eventCategoryField);
        return this;
    }

    public EqlSearchRequestBuilder implicitJoinKeyField(String implicitJoinKeyField) {
        request.implicitJoinKeyField(implicitJoinKeyField);
        return this;
    }

    public EqlSearchRequestBuilder fetchSize(int size) {
        request.fetchSize(size);
        return this;
    }

    public EqlSearchRequestBuilder searchAfter(Object[] values) {
        request.searchAfter(values);
        return this;
    }

    public EqlSearchRequestBuilder query(String query) {
        request.query(query);
        return this;
    }

    public EqlSearchRequestBuilder query(boolean isCaseSensitive) {
        request.isCaseSensitive(isCaseSensitive);
        return this;
    }
}
