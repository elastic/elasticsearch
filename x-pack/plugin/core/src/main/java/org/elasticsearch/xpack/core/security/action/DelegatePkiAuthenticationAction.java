/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.action;

import org.elasticsearch.action.ActionType;

/**
 * ActionType for delegating PKI authentication
 */
public class DelegatePkiAuthenticationAction extends ActionType<DelegatePkiAuthenticationResponse> {

    public static final String NAME = "cluster:admin/xpack/security/delegate_pki";
    public static final DelegatePkiAuthenticationAction INSTANCE = new DelegatePkiAuthenticationAction();

    private DelegatePkiAuthenticationAction() {
        super(NAME, DelegatePkiAuthenticationResponse::new);
    }
}
