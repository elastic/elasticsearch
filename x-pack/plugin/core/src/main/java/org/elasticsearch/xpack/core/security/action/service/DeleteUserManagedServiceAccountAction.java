/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.action.ActionType;

public class DeleteUserManagedServiceAccountAction extends ActionType<DeleteUserManagedServiceAccountResponse> {

    public static final String NAME = "cluster:admin/xpack/security/user_managed_service_account/delete";
    public static final DeleteUserManagedServiceAccountAction INSTANCE = new DeleteUserManagedServiceAccountAction();

    private DeleteUserManagedServiceAccountAction() {
        super(NAME);
    }
}
