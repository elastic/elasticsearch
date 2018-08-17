/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexlifecycle.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.xpack.core.indexlifecycle.action.PutLifecycleAction;

public class PutLifecycleActionRequestBuilder
        extends ActionRequestBuilder<PutLifecycleAction.Request, PutLifecycleAction.Response, PutLifecycleActionRequestBuilder> {

    public PutLifecycleActionRequestBuilder(
            final ElasticsearchClient client,
            final Action<PutLifecycleAction.Request, PutLifecycleAction.Response, PutLifecycleActionRequestBuilder> action) {
        super(client, action, new PutLifecycleAction.Request());
    }

    public PutLifecycleActionRequestBuilder setPolicy(LifecyclePolicy policy) {
        request.setPolicy(policy);
        return this;
    }

}
