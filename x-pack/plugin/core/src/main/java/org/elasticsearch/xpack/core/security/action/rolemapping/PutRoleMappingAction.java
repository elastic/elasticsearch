/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.rolemapping;

import org.elasticsearch.action.ActionType;

/**
 * ActionType for adding a role to the security index
 */
public class PutRoleMappingAction extends ActionType<PutRoleMappingResponse> {

    public static final PutRoleMappingAction INSTANCE = new PutRoleMappingAction();
    public static final String NAME = "cluster:admin/xpack/security/role_mapping/put";

    private PutRoleMappingAction() {
        super(NAME, PutRoleMappingResponse::new);
    }
}
