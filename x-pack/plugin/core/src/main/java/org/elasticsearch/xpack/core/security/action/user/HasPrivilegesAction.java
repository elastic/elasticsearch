/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.action.Action;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

/**
 * This action is testing whether a user has the specified
 * {@link RoleDescriptor.IndicesPrivileges privileges}
 */
public class HasPrivilegesAction extends Action<HasPrivilegesRequest, HasPrivilegesResponse> {

    public static final HasPrivilegesAction INSTANCE = new HasPrivilegesAction();
    public static final String NAME = "cluster:admin/xpack/security/user/has_privileges";

    private HasPrivilegesAction() {
        super(NAME);
    }

    @Override
    public HasPrivilegesResponse newResponse() {
        return new HasPrivilegesResponse();
    }
}
