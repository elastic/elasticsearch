/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.rest.action;

import org.elasticsearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * Builder for reloading of analyzers
 */
public class ReloadAnalyzerRequestBuilder
        extends BroadcastOperationRequestBuilder<ReloadAnalyzersRequest, ReloadAnalyzersResponse, ReloadAnalyzerRequestBuilder> {

    public ReloadAnalyzerRequestBuilder(ElasticsearchClient client, ReloadAnalyzerAction action, String... indices) {
        super(client, action, new ReloadAnalyzersRequest(indices));
    }

}
