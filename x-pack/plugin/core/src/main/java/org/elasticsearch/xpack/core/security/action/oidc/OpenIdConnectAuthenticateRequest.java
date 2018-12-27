/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.oidc;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;

/**
 * Represents a request for authentication using OpenID Connect
 */
public class OpenIdConnectAuthenticateRequest extends ActionRequest {

    /**
     * The URI were the OP redirected the browser after the authentication attempt. This is passed as is from the
     * facilitator entity (i.e. Kibana)
     */
    private String redirectUri;

    /**
     * The state value that either we or the facilitator generated for this specific flow and that was stored at the user's session with
     * the facilitator
     */
    private String state;

    /**
     * The nonce value that  the facilitator generated for this specific flow and that was stored at the user's session with
     * the facilitator
     */
    private String nonce;

    public OpenIdConnectAuthenticateRequest() {
    }

    public String getRedirectUri() {
        return redirectUri;
    }

    public void setRedirectUri(String redirectUri) {
        this.redirectUri = redirectUri;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getNonce() {
        return nonce;
    }

    public void setNonce(String nonce) {
        this.nonce = nonce;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public String toString() {
        return "{redirectUri=" + redirectUri + ", state=" + state + ", nonce=" + nonce + "}";
    }
}

