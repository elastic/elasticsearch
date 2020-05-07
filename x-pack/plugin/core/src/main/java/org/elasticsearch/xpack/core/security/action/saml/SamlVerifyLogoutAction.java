/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.saml;

import org.elasticsearch.action.ActionType;

/**
 * ActionType for verifying SAML LogoutResponse
 */
public final class SamlVerifyLogoutAction extends ActionType<SamlVerifyLogoutResponse> {

    public static final String NAME = "cluster:admin/xpack/security/saml/verify_logout";
    public static final SamlVerifyLogoutAction INSTANCE = new SamlVerifyLogoutAction();

    private SamlVerifyLogoutAction() {
        super(NAME, SamlVerifyLogoutResponse::new);
    }
}
