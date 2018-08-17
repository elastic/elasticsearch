/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexlifecycle.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

public class GetStatusActionRequestBuilder
        extends ActionRequestBuilder<GetStatusAction.Request, GetStatusAction.Response, GetStatusActionRequestBuilder> {

    public GetStatusActionRequestBuilder(
            final ElasticsearchClient client,
            final Action<GetStatusAction.Request, GetStatusAction.Response, GetStatusActionRequestBuilder> action) {
        super(client, action, new GetStatusAction.Request());
    }

}
