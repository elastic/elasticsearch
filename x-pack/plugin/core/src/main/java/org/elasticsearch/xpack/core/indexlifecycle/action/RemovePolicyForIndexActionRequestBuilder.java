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

public class RemovePolicyForIndexActionRequestBuilder
        extends ActionRequestBuilder<
        RemoveIndexLifecyclePolicyAction.Request,
        RemoveIndexLifecyclePolicyAction.Response,
        RemovePolicyForIndexActionRequestBuilder> {

    public RemovePolicyForIndexActionRequestBuilder(
            final ElasticsearchClient client,
            final Action<
                    RemoveIndexLifecyclePolicyAction.Request,
                    RemoveIndexLifecyclePolicyAction.Response,
                    RemovePolicyForIndexActionRequestBuilder> action) {
        super(client, action, new RemoveIndexLifecyclePolicyAction.Request());
    }

    public RemovePolicyForIndexActionRequestBuilder setIndices(final String... indices) {
        request.indices(indices);
        return this;
    }

    public RemovePolicyForIndexActionRequestBuilder setIndicesOptions(final IndicesOptions indicesOptions) {
        request.indicesOptions(indicesOptions);
        return this;
    }

}
