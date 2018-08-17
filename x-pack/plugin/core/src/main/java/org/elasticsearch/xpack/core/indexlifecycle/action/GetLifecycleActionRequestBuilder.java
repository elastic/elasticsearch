/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexlifecycle.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

public class GetLifecycleActionRequestBuilder
        extends ActionRequestBuilder<GetLifecycleAction.Request, GetLifecycleAction.Response, GetLifecycleActionRequestBuilder> {

    public GetLifecycleActionRequestBuilder(
            final ElasticsearchClient client,
            final Action<GetLifecycleAction.Request, GetLifecycleAction.Response, GetLifecycleActionRequestBuilder> action) {
        super(client, action, new GetLifecycleAction.Request());
    }

    public GetLifecycleActionRequestBuilder setPolicyNames(final String[] policyNames) {
        request.setPolicyNames(policyNames);
        return this;
    }

}
