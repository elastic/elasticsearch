/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.action.ActionType;

/**
 * Deletes a token belonging to a user-managed service account. A separate action from
 * {@link DeleteServiceAccountTokenAction} only so that the two kinds of account can be authorized separately, so both
 * actions take a {@link DeleteServiceAccountTokenRequest}.
 */
public class DeleteUserManagedServiceAccountTokenAction extends ActionType<DeleteServiceAccountTokenResponse> {

    public static final String NAME = "cluster:admin/xpack/security/user_managed_service_account/token/delete";
    public static final DeleteUserManagedServiceAccountTokenAction INSTANCE = new DeleteUserManagedServiceAccountTokenAction();

    private DeleteUserManagedServiceAccountTokenAction() {
        super(NAME);
    }
}
