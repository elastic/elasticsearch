/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexlifecycle.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.ElasticsearchClient;

public class RetryActionRequestBuilder extends ActionRequestBuilder<RetryAction.Request, RetryAction.Response, RetryActionRequestBuilder> {

    public RetryActionRequestBuilder(
            final ElasticsearchClient client,
            final Action<RetryAction.Request, RetryAction.Response, RetryActionRequestBuilder> action) {
        super(client, action, new RetryAction.Request());
    }

    public RetryActionRequestBuilder setIndices(final String... indices) {
        request.indices(indices);
        return this;
    }

    public RetryActionRequestBuilder setIndicesOptions(final IndicesOptions indicesOptions) {
        request.indicesOptions(indicesOptions);
        return this;
    }

}
