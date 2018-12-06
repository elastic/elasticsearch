/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

public class GetUsersRequestBuilder extends ActionRequestBuilder<GetUsersRequest, GetUsersResponse> {

    public GetUsersRequestBuilder(ElasticsearchClient client) {
        this(client, GetUsersAction.INSTANCE);
    }

    public GetUsersRequestBuilder(ElasticsearchClient client, GetUsersAction action) {
        super(client, action, new GetUsersRequest());
    }

    public GetUsersRequestBuilder usernames(String... usernames) {
        request.usernames(usernames);
        return this;
    }
}
