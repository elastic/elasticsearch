/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.action.admin.user;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.shield.authc.support.Hasher;
import org.elasticsearch.shield.authc.support.SecuredString;

public class AddUserRequestBuilder extends ActionRequestBuilder<AddUserRequest, AddUserResponse, AddUserRequestBuilder> {

    private final Hasher hasher = Hasher.BCRYPT;

    public AddUserRequestBuilder(ElasticsearchClient client) {
        this(client, AddUserAction.INSTANCE);
    }

    public AddUserRequestBuilder(ElasticsearchClient client, AddUserAction action) {
        super(client, action, new AddUserRequest());
    }

    public AddUserRequestBuilder username(String username) {
        request.username(username);
        return this;
    }

    public AddUserRequestBuilder roles(String... roles) {
        request.roles(roles);
        return this;
    }

    public AddUserRequestBuilder password(String password) {
        request.passwordHash(hasher.hash(new SecuredString(password.toCharArray())));
        return this;
    }
}
