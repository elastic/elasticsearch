/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;

public class GetUsersRequestBuilder extends ActionRequestBuilder<GetUsersRequest, GetUsersResponse> {

    public GetUsersRequestBuilder(ElasticsearchClient client) {
        super(client, GetUsersAction.INSTANCE, new GetUsersRequest());
    }

    public GetUsersRequestBuilder usernames(String... usernames) {
        request.usernames(usernames);
        return this;
    }

    public GetUsersRequestBuilder withProfileUid(boolean withProfileUid) {
        request.setWithProfileUid(withProfileUid);
        return this;
    }
}
