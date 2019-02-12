/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.role;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.support.WriteRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * A builder for requests to delete a role from the security index
 */
public class DeleteRoleRequestBuilder extends ActionRequestBuilder<DeleteRoleRequest, DeleteRoleResponse>
        implements WriteRequestBuilder<DeleteRoleRequestBuilder> {

    public DeleteRoleRequestBuilder(ElasticsearchClient client) {
        this(client, DeleteRoleAction.INSTANCE);
    }

    public DeleteRoleRequestBuilder(ElasticsearchClient client, DeleteRoleAction action) {
        super(client, action, new DeleteRoleRequest());
    }

    public DeleteRoleRequestBuilder name(String name) {
        request.name(name);
        return this;
    }
}
