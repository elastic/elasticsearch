/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.action.ActionType;

/**
 * Creates a token for a user-managed service account. A separate action from
 * {@link CreateServiceAccountTokenAction} only so that the two kinds of account can be authorized separately: a token
 * names the same namespace, service and token name either way, so both actions take a
 * {@link CreateServiceAccountTokenRequest}.
 */
public class CreateUserManagedServiceAccountTokenAction extends ActionType<CreateServiceAccountTokenResponse> {

    public static final String NAME = "cluster:admin/xpack/security/user_managed_service_account/token/create";
    public static final CreateUserManagedServiceAccountTokenAction INSTANCE = new CreateUserManagedServiceAccountTokenAction();

    private CreateUserManagedServiceAccountTokenAction() {
        super(NAME);
    }
}
