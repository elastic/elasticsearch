/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.token;

import org.elasticsearch.action.ActionType;

public final class RefreshTokenAction extends ActionType<CreateTokenResponse> {

    public static final String NAME = "cluster:admin/xpack/security/token/refresh";
    public static final RefreshTokenAction INSTANCE = new RefreshTokenAction();

    private RefreshTokenAction() {
        super(NAME, CreateTokenResponse::new);
    }
}