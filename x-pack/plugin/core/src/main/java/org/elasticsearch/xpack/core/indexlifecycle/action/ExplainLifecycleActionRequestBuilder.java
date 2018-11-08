/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexlifecycle.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.support.master.info.ClusterInfoRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.xpack.core.indexlifecycle.ExplainLifecycleRequest;
import org.elasticsearch.xpack.core.indexlifecycle.ExplainLifecycleResponse;

public class ExplainLifecycleActionRequestBuilder
        extends ClusterInfoRequestBuilder<ExplainLifecycleRequest, ExplainLifecycleResponse, ExplainLifecycleActionRequestBuilder> {

    public ExplainLifecycleActionRequestBuilder(
            final ElasticsearchClient client,
            final Action<ExplainLifecycleRequest, ExplainLifecycleResponse, ExplainLifecycleActionRequestBuilder> action) {
        super(client, action, new ExplainLifecycleRequest());
    }

}
