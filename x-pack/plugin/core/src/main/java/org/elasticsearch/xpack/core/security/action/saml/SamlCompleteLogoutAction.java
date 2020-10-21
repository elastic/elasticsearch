/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.saml;

import org.elasticsearch.action.ActionType;

/**
 * ActionType for completing SAML LogoutResponse
 */
public final class SamlCompleteLogoutAction extends ActionType<SamlCompleteLogoutResponse> {

    public static final String NAME = "cluster:admin/xpack/security/saml/complete_logout";
    public static final SamlCompleteLogoutAction INSTANCE = new SamlCompleteLogoutAction();

    private SamlCompleteLogoutAction() {
        super(NAME, SamlCompleteLogoutResponse::new);
    }
}
