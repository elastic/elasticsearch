/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.oidc;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Represents a request for authentication using OpenID Connect
 */
public class OpenIdConnectAuthenticateRequest extends ActionRequest {

    /**
     * The URI where the OP redirected the browser after the authentication attempt. This is passed as is from the
     * facilitator entity (i.e. Kibana)
     */
    private String redirectUri;

    /**
     * The state value that we generated or the facilitator provided for this specific flow and that should be stored at the user's session
     * with the facilitator
     */
    private String state;

    /**
     * The nonce value that we generated or the facilitator provided for this specific flow and that should be stored at the user's session
     * with the facilitator
     */
    private String nonce;

    /**
     * The name of the OIDC Realm that should consume the authentication request
     */
    private String realm;

    public OpenIdConnectAuthenticateRequest() {

    }

    public OpenIdConnectAuthenticateRequest(StreamInput in) throws IOException {
        super(in);
        redirectUri = in.readString();
        state = in.readString();
        nonce = in.readString();
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_7_4_0)) {
            realm = in.readOptionalString();
        }

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

    public String getRealm() {
        return realm;
    }

    public void setRealm(String realm) {
        this.realm = realm;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (Strings.isNullOrEmpty(state)) {
            validationException = addValidationError("state parameter is missing", validationException);
        }
        if (Strings.isNullOrEmpty(nonce)) {
            validationException = addValidationError("nonce parameter is missing", validationException);
        }
        if (Strings.isNullOrEmpty(redirectUri)) {
            validationException = addValidationError("redirect_uri parameter is missing", validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(redirectUri);
        out.writeString(state);
        out.writeString(nonce);
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_7_4_0)) {
            out.writeOptionalString(realm);
        }
    }

    public String toString() {
        return "{redirectUri=" + redirectUri + ", state=" + state + ", nonce=" + nonce + ", realm=" + realm + "}";
    }
}
