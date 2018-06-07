/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;

/**
 * Request builder for checking a user's privileges
 */
public class HasPrivilegesRequestBuilder
        extends ActionRequestBuilder<HasPrivilegesRequest, HasPrivilegesResponse> {

    public HasPrivilegesRequestBuilder(ElasticsearchClient client) {
        super(client, HasPrivilegesAction.INSTANCE, new HasPrivilegesRequest());
    }

    /**
     * Set the username of the user that should enabled or disabled. Must not be {@code null}
     */
    public HasPrivilegesRequestBuilder username(String username) {
        request.username(username);
        return this;
    }

    /**
     * Set whether the user should be enabled or not
     */
    public HasPrivilegesRequestBuilder source(String username, BytesReference source, XContentType xContentType) throws IOException {
        final RoleDescriptor role = RoleDescriptor.parsePrivilegesCheck(username + "/has_privileges", source, xContentType);
        request.username(username);
        request.indexPrivileges(role.getIndicesPrivileges());
        request.clusterPrivileges(role.getClusterPrivileges());
        request.applicationPrivileges(role.getApplicationPrivileges());
        return this;
    }
}
