/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.action.admin.user;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * Action for adding a user to the shield administrative index
 */
public class AddUserAction extends Action<AddUserRequest, AddUserResponse, AddUserRequestBuilder> {

    public static final AddUserAction INSTANCE = new AddUserAction();
    public static final String NAME = "cluster:admin/shield/user/add";


    protected AddUserAction() {
        super(NAME);
    }

    @Override
    public AddUserRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new AddUserRequestBuilder(client, this);
    }

    @Override
    public AddUserResponse newResponse() {
        return new AddUserResponse();
    }
}
