/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.oidc;

import org.elasticsearch.action.Action;
import org.elasticsearch.common.io.stream.Writeable;

/**
 * Action for initiating an authentication process using OpenID Connect
 */
public final class OpenIdConnectAuthenticateAction extends Action<OpenIdConnectAuthenticateResponse> {

    public static final OpenIdConnectAuthenticateAction INSTANCE = new OpenIdConnectAuthenticateAction();
    public static final String NAME = "cluster:admin/xpack/security/oidc/authenticate";

    private OpenIdConnectAuthenticateAction() {
        super(NAME);
    }

    @Override
    public OpenIdConnectAuthenticateResponse newResponse() {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    public Writeable.Reader<OpenIdConnectAuthenticateResponse> getResponseReader() {
        return OpenIdConnectAuthenticateResponse::new;
    }
}
