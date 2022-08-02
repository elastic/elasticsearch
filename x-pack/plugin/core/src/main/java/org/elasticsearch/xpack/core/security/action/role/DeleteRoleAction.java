/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.role;

import org.elasticsearch.action.ActionType;

/**
 * ActionType for deleting a role from the security index
 */
public class DeleteRoleAction extends ActionType<DeleteRoleResponse> {

    public static final DeleteRoleAction INSTANCE = new DeleteRoleAction();
    public static final String NAME = "cluster:admin/xpack/security/role/delete";

    protected DeleteRoleAction() {
        super(NAME, DeleteRoleResponse::new);
    }
}
