/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.saml;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;

/**
 * A {@link AuthenticationToken} to hold OpenID Connect related content.
 * Depending on the flow this can content only a code ( oAuth2 authorization code
 * grant flow ) or an Identity Token ( oAuth2 implicit flow )
 */
public class OpenIdConnectToken implements AuthenticationToken {

    @Nullable
    private String code;

    public OpenIdConnectToken(String code) {
        this.code = code;
    }

    @Override
    public String principal() {
        return "<unauthenticated-didc-user>";
    }

    @Override
    public Object credentials() {
        return code;
    }

    @Override
    public void clearCredentials() {
        this.code = null;
    }
}
