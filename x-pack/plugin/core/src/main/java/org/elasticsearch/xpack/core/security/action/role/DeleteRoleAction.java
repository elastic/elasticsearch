/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.role;

import org.elasticsearch.action.Action;

/**
 * Action for deleting a role from the security index
 */
public class DeleteRoleAction extends Action<DeleteRoleResponse> {

    public static final DeleteRoleAction INSTANCE = new DeleteRoleAction();
    public static final String NAME = "cluster:admin/xpack/security/role/delete";


    protected DeleteRoleAction() {
        super(NAME);
    }

    @Override
    public DeleteRoleResponse newResponse() {
        return new DeleteRoleResponse();
    }
}
