/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.oidc;

import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;

/**
 * A {@link AuthenticationToken} to hold OpenID Connect related content.
 * Depending on the flow the token can contain only a code ( oAuth2 authorization code
 * grant flow ) or even an Identity Token ( oAuth2 implicit flow )
 */
public class OpenIdConnectToken implements AuthenticationToken {

    private String redirectUrl;
    private String state;
    private String nonce;

    /**
     * @param redirectUrl The URI where the OP redirected the browser after the authentication event at the OP. This is passed as is from
     *                    the facilitator entity (i.e. Kibana), so it is URL Encoded. It contains either the code or the id_token itself
     *                    depending on the flow used
     * @param state       The state value that we generated for this specific flow and should be stored at the user's session with the
     *                    facilitator.
     * @param nonce       The nonce value that we generated for this specific flow and should be stored at the user's session with the
     *                    facilitator.
     */
    public OpenIdConnectToken(String redirectUrl, String state, String nonce) {
        this.redirectUrl = redirectUrl;
        this.state = state;
        this.nonce = nonce;
    }

    @Override
    public String principal() {
        return "<OIDC Token>";
    }

    @Override
    public Object credentials() {
        return redirectUrl;
    }

    @Override
    public void clearCredentials() {
        this.redirectUrl = null;
    }

    public String getState() {
        return state;
    }

    public String getNonce() {
        return nonce;
    }

    public String getRedirectUrl() {
        return redirectUrl;
    }

    public String toString() {
        return getClass().getSimpleName() + "{ redirectUrl=" + redirectUrl + ", state=" + state + ", nonce=" + nonce + "}";
    }
}
