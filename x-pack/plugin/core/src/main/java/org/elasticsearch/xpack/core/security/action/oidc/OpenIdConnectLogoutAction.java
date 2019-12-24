/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.oidc;

import org.elasticsearch.action.ActionType;

public class OpenIdConnectLogoutAction extends ActionType<OpenIdConnectLogoutResponse> {

    public static final OpenIdConnectLogoutAction INSTANCE = new OpenIdConnectLogoutAction();
    public static final String NAME = "cluster:admin/xpack/security/oidc/logout";

    private OpenIdConnectLogoutAction() {
        super(NAME, OpenIdConnectLogoutResponse::new);
    }
}
