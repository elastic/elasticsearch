/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.role;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.support.WriteRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;

/**
 * A builder for requests to delete a role from the security index
 */
public class DeleteRoleRequestBuilder extends ActionRequestBuilder<DeleteRoleRequest, DeleteRoleResponse>
    implements
        WriteRequestBuilder<DeleteRoleRequestBuilder> {

    public DeleteRoleRequestBuilder(ElasticsearchClient client) {
        super(client, DeleteRoleAction.INSTANCE, new DeleteRoleRequest());

    }

    public DeleteRoleRequestBuilder name(String name) {
        request.name(name);
        return this;
    }
}
