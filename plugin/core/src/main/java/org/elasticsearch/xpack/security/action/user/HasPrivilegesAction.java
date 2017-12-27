/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.user;

import java.util.Collections;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * This action is testing whether a user has the specified
 * {@link org.elasticsearch.xpack.security.authz.RoleDescriptor.IndicesPrivileges privileges}
 */
public class HasPrivilegesAction extends Action<HasPrivilegesRequest, HasPrivilegesResponse, HasPrivilegesRequestBuilder> {

    public static final HasPrivilegesAction INSTANCE = new HasPrivilegesAction();
    public static final String NAME = "cluster:admin/xpack/security/user/has_privileges";

    private HasPrivilegesAction() {
        super(NAME);
    }

    @Override
    public HasPrivilegesRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new HasPrivilegesRequestBuilder(client);
    }

    @Override
    public HasPrivilegesResponse newResponse() {
        return new HasPrivilegesResponse();
    }
}
