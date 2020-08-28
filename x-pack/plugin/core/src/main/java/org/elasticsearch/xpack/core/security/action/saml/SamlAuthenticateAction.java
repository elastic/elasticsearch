/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.saml;

import org.elasticsearch.action.ActionType;

/**
 * ActionType for authenticating using SAML assertions
 */
public final class SamlAuthenticateAction extends ActionType<SamlAuthenticateResponse> {

    public static final String NAME = "cluster:admin/xpack/security/saml/authenticate";
    public static final SamlAuthenticateAction INSTANCE = new SamlAuthenticateAction();

    private SamlAuthenticateAction() {
        super(NAME, SamlAuthenticateResponse::new);
    }
}
