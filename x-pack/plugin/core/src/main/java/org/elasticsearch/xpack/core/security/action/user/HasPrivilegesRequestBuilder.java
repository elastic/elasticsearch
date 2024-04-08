/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;

/**
 * Request builder for checking a user's privileges
 */
public class HasPrivilegesRequestBuilder extends ActionRequestBuilder<HasPrivilegesRequest, HasPrivilegesResponse> {

    public HasPrivilegesRequestBuilder(ElasticsearchClient client) {
        super(client, HasPrivilegesAction.INSTANCE, new HasPrivilegesRequest());
    }

    public HasPrivilegesRequestBuilder username(String username) {
        request.username(username);
        return this;
    }

    public HasPrivilegesRequestBuilder source(String username, BytesReference source, XContentType xContentType) throws IOException {
        final AuthorizationEngine.PrivilegesToCheck privilegesToCheck = RoleDescriptor.parsePrivilegesToCheck(
            username + "/has_privileges",
            true, // hard-coded for now, but it doesn't have to be
            source,
            xContentType
        );
        request.username(username);
        request.privilegesToCheck(privilegesToCheck);
        return this;
    }
}
