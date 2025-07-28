/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.RemoteClusterActionType;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

/**
 * This action is testing whether a user has the specified
 * {@link RoleDescriptor.IndicesPrivileges privileges}
 */
public class HasPrivilegesAction extends ActionType<HasPrivilegesResponse> {

    public static final HasPrivilegesAction INSTANCE = new HasPrivilegesAction();
    public static final String NAME = "cluster:admin/xpack/security/user/has_privileges";
    public static final RemoteClusterActionType<HasPrivilegesResponse> REMOTE_TYPE = new RemoteClusterActionType<>(
        NAME,
        HasPrivilegesResponse::new
    );

    private HasPrivilegesAction() {
        super(NAME);
    }
}
