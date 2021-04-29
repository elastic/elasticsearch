/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.rolemapping;

import org.elasticsearch.action.ActionType;

/**
 * ActionType for deleting a role-mapping from the
 * org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore
 */
public class DeleteRoleMappingAction extends ActionType<DeleteRoleMappingResponse> {

    public static final DeleteRoleMappingAction INSTANCE = new DeleteRoleMappingAction();
    public static final String NAME = "cluster:admin/xpack/security/role_mapping/delete";

    private DeleteRoleMappingAction() {
        super(NAME, DeleteRoleMappingResponse::new);
    }
}
