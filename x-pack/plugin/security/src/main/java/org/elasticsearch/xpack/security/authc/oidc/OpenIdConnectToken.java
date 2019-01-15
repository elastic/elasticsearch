/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.oidc;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;

/**
 * A {@link AuthenticationToken} to hold OpenID Connect related content.
 * Depending on the flow the token can contain only a code ( oAuth2 authorization code
 * grant flow ) or even an Identity Token ( oAuth2 implicit flow )
 */
public class OpenIdConnectToken implements AuthenticationToken {

    private String redirectUri;
    private String state;
    @Nullable
    private String nonce;

    /**
     * @param redirectUri The URI where the OP redirected the browser after the authentication event at the OP. This is passed as is from
     *                    the facilitator entity (i.e. Kibana), so it is URL Encoded.
     * @param state       The state value that we generated for this specific flow and should be stored at the user's session with the
     *                    facilitator.
     * @param nonce       The nonce value that we generated for this specific flow and should be stored at the user's session with the
     *                    facilitator.
     */
    public OpenIdConnectToken(String redirectUri, String state, String nonce) {
        this.redirectUri = redirectUri;
        this.state = state;
        this.nonce = nonce;
    }

    @Override
    public String principal() {
        return "<OIDC Token>";
    }

    @Override
    public Object credentials() {
        return redirectUri;
    }

    @Override
    public void clearCredentials() {
        this.redirectUri = null;
    }

    public String getState() {
        return state;
    }

    public String getNonce() {
        return nonce;
    }

    public String toString() {
        return getClass().getSimpleName() + "{ redirectUri=" + redirectUri + ", state=" + state + ", nonce=" + nonce + "}";
    }
}
