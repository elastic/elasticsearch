/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.oidc;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * A response object that contains the OpenID Connect Authentication Request as a URL and the state and nonce values that were
 * generated for this request.
 */
public class OpenIdConnectPrepareAuthenticationResponse extends ActionResponse implements ToXContentObject {

    private String authenticationRequestUrl;
    /*
     * The oAuth2 state parameter used for CSRF protection.
     */
    private String state;
    /*
     * String value used to associate a Client session with an ID Token, and to mitigate replay attacks.
     */
    private String nonce;
    /*
     * String value: name of the realm used to perform authentication.
     */
    private String realmName;

    public OpenIdConnectPrepareAuthenticationResponse(String authorizationEndpointUrl, String state, String nonce, String realmName) {
        this.authenticationRequestUrl = authorizationEndpointUrl;
        this.state = state;
        this.nonce = nonce;
        this.realmName = realmName;
    }

    public OpenIdConnectPrepareAuthenticationResponse(StreamInput in) throws IOException {
        super(in);
        authenticationRequestUrl = in.readString();
        state = in.readString();
        nonce = in.readString();
        if (in.getVersion().onOrAfter(Version.V_7_11_0)) {
            realmName = in.readString();
        }
    }

    public String getAuthenticationRequestUrl() {
        return authenticationRequestUrl;
    }

    public String getState() {
        return state;
    }

    public String getNonce() {
        return nonce;
    }

    public String getRealmName() {
        return realmName;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(authenticationRequestUrl);
        out.writeString(state);
        out.writeString(nonce);
        if (out.getVersion().onOrAfter(Version.V_7_11_0)) {
            out.writeString(realmName);
        }
    }

    public String toString() {
        return "{authenticationRequestUrl="
            + authenticationRequestUrl
            + ", state="
            + state
            + ", nonce="
            + nonce
            + ", realmName"
            + realmName
            + "}";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("redirect", authenticationRequestUrl);
        builder.field("state", state);
        builder.field("nonce", nonce);
        if (realmName != null) {
            builder.field("realm", realmName);
        }
        builder.endObject();
        return builder;
    }
}
