/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

import java.io.IOException;

/**
 * Request builder for checking a user's privileges
 */
public class GetUserPrivilegesRequestBuilder
        extends ActionRequestBuilder<GetUserPrivilegesRequest, GetUserPrivilegesResponse> {

    public GetUserPrivilegesRequestBuilder(ElasticsearchClient client) {
        super(client, GetUserPrivilegesAction.INSTANCE, new GetUserPrivilegesRequest());
    }

    /**
     * Set the username of the user that should enabled or disabled. Must not be {@code null}
     */
    public GetUserPrivilegesRequestBuilder username(String username) {
        request.username(username);
        return this;
    }

    /**
     * Set whether the user should be enabled or not
     */
    public GetUserPrivilegesRequestBuilder source(String username) throws IOException {
        request.username(username);
        return this;
    }
}
