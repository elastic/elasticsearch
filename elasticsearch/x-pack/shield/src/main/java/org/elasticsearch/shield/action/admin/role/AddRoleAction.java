/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.action.admin.role;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * Action for adding a role to the shield administrative index
 */
public class AddRoleAction extends Action<AddRoleRequest, AddRoleResponse, AddRoleRequestBuilder> {

    public static final AddRoleAction INSTANCE = new AddRoleAction();
    public static final String NAME = "cluster:admin/shield/role/add";


    protected AddRoleAction() {
        super(NAME);
    }

    @Override
    public AddRoleRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new AddRoleRequestBuilder(client, this);
    }

    @Override
    public AddRoleResponse newResponse() {
        return new AddRoleResponse();
    }
}
