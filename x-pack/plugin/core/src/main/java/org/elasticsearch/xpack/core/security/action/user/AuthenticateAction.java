/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.action.ActionType;

public class AuthenticateAction extends ActionType<AuthenticateResponse> {

    public static final String NAME = "cluster:admin/xpack/security/user/authenticate";
    public static final AuthenticateAction INSTANCE = new AuthenticateAction();

    public AuthenticateAction() {
        super(NAME, AuthenticateResponse::new);
    }
}
