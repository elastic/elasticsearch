/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.oidc;

import com.nimbusds.oauth2.sdk.id.State;
import com.nimbusds.openid.connect.sdk.Nonce;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;

/**
 * A {@link AuthenticationToken} to hold OpenID Connect related content.
 * Depending on the flow the token can contain only a code ( oAuth2 authorization code
 * grant flow ) or even an Identity Token ( oAuth2 implicit flow )
 */
public class OpenIdConnectToken implements AuthenticationToken {

    private String redirectUrl;
    private State state;
    private Nonce nonce;
    private String authenticatingRealm;

    /**
     * @param redirectUrl The URI where the OP redirected the browser after the authentication event at the OP. This is passed as is from
     *                    the facilitator entity (i.e. Kibana), so it is URL Encoded. It contains either the code or the id_token itself
     *                    depending on the flow used
     * @param state       The state value that we generated or the facilitator provided for this specific flow and should be stored at the
     *                    user's session with the facilitator.
     * @param nonce       The nonce value that we generated or the facilitator provided for this specific flow and should be stored at the
     *                    user's session with the facilitator.
     * @param authenticatingRealm The realm that should authenticate this OpenId Connect Authentication Response
     */
    public OpenIdConnectToken(String redirectUrl, State state, Nonce nonce, @Nullable String authenticatingRealm) {
        this.redirectUrl = redirectUrl;
        this.state = state;
        this.nonce = nonce;
        this.authenticatingRealm = authenticatingRealm;
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

    public State getState() {
        return state;
    }

    public Nonce getNonce() {
        return nonce;
    }

    public String getRedirectUrl() {
        return redirectUrl;
    }

    public String getAuthenticatingRealm() { return authenticatingRealm; }

    public String toString() {
        return getClass().getSimpleName() + "{ redirectUrl=" + redirectUrl + ", state=" + state + ", nonce=" + nonce + ", " +
            "authenticatingRealm="+ authenticatingRealm +"}";
    }
}
