/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.rollup.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.ElasticsearchClient;

public class RollupSearchAction extends Action<SearchRequest, SearchResponse, RollupSearchAction.RequestBuilder> {

    public static final RollupSearchAction INSTANCE = new RollupSearchAction();
    public static final String NAME = "indices:admin/xpack/rollup/search";

    private RollupSearchAction() {
        super(NAME);
    }

    @Override
    public RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RequestBuilder(client);
    }

    @Override
    public SearchResponse newResponse() {
        return new SearchResponse();
    }

    public static class RequestBuilder extends ActionRequestBuilder<SearchRequest, SearchResponse, RequestBuilder> {
        public RequestBuilder(ElasticsearchClient client, SearchRequest searchRequest) {
            super(client, INSTANCE, searchRequest);
        }

        RequestBuilder(ElasticsearchClient client) {
            super(client, INSTANCE, new SearchRequest());
        }
    }
}
