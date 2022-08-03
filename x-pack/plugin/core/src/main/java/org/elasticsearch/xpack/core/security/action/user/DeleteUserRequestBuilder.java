/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.support.WriteRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;

public class DeleteUserRequestBuilder extends ActionRequestBuilder<DeleteUserRequest, DeleteUserResponse>
    implements
        WriteRequestBuilder<DeleteUserRequestBuilder> {

    public DeleteUserRequestBuilder(ElasticsearchClient client) {
        this(client, DeleteUserAction.INSTANCE);
    }

    public DeleteUserRequestBuilder(ElasticsearchClient client, DeleteUserAction action) {
        super(client, action, new DeleteUserRequest());
    }

    public DeleteUserRequestBuilder username(String username) {
        request.username(username);
        return this;
    }
}
