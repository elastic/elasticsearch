/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.saml;

import org.elasticsearch.action.ActionType;

/**
 * ActionType for initiating a logout process for a SAML-SSO user
 */
public final class SamlLogoutAction extends ActionType<SamlLogoutResponse> {

    public static final String NAME = "cluster:admin/xpack/security/saml/logout";
    public static final SamlLogoutAction INSTANCE = new SamlLogoutAction();

    private SamlLogoutAction() {
        super(NAME, SamlLogoutResponse::new);
    }
}
