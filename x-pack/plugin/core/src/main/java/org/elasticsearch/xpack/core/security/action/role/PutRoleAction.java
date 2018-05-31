/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.role;

import org.elasticsearch.action.Action;

/**
 * Action for adding a role to the security index
 */
public class PutRoleAction extends Action<PutRoleRequest, PutRoleResponse> {

    public static final PutRoleAction INSTANCE = new PutRoleAction();
    public static final String NAME = "cluster:admin/xpack/security/role/put";


    protected PutRoleAction() {
        super(NAME);
    }

    @Override
    public PutRoleResponse newResponse() {
        return new PutRoleResponse();
    }
}
