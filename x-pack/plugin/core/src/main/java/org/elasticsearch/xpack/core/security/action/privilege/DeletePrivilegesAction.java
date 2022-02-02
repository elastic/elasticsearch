/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.privilege;

import org.elasticsearch.action.ActionType;

/**
 * ActionType for deleting application privileges.
 */
public final class DeletePrivilegesAction extends ActionType<DeletePrivilegesResponse> {

    public static final DeletePrivilegesAction INSTANCE = new DeletePrivilegesAction();
    public static final String NAME = "cluster:admin/xpack/security/privilege/delete";

    private DeletePrivilegesAction() {
        super(NAME, DeletePrivilegesResponse::new);
    }
}
