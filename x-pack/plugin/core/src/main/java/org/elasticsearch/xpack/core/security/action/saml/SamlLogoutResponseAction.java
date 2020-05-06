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
public final class SamlLogoutResponseAction extends ActionType<SamlLogoutResponseResponse> {

    public static final String NAME = "cluster:admin/xpack/security/saml/logout_response";
    public static final SamlLogoutResponseAction INSTANCE = new SamlLogoutResponseAction();

    private SamlLogoutResponseAction() {
        super(NAME, SamlLogoutResponseResponse::new);
    }
}
