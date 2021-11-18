/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.saml;

import org.elasticsearch.action.ActionType;

/**
 * ActionType to perform IdP-initiated logout for a SAML-SSO user
 */
public final class SamlInvalidateSessionAction extends ActionType<SamlInvalidateSessionResponse> {

    public static final String NAME = "cluster:admin/xpack/security/saml/invalidate";
    public static final SamlInvalidateSessionAction INSTANCE = new SamlInvalidateSessionAction();

    private SamlInvalidateSessionAction() {
        super(NAME, SamlInvalidateSessionResponse::new);
    }
}
