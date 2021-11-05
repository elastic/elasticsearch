/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.rollup.action;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.ElasticsearchClient;

public class RollupSearchAction extends ActionType<SearchResponse> {

    public static final RollupSearchAction INSTANCE = new RollupSearchAction();
    public static final String NAME = "indices:data/read/xpack/rollup/search";

    private RollupSearchAction() {
        super(NAME, SearchResponse::new);
    }

    public static class RequestBuilder extends ActionRequestBuilder<SearchRequest, SearchResponse> {
        public RequestBuilder(ElasticsearchClient client, SearchRequest searchRequest) {
            super(client, INSTANCE, searchRequest);
        }
    }
}
