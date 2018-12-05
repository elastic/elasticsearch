/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.oidc;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;


public class OpenIdConnectPrepareAuthenticationResponse extends ActionResponse {

    private String redirectUrl;
    private String state;

    public OpenIdConnectPrepareAuthenticationResponse(String redirectUrl, String state) {
        this.redirectUrl = redirectUrl;
        this.state = state;
    }

    public OpenIdConnectPrepareAuthenticationResponse() {
    }

    public String getRedirectUrl() {
        return redirectUrl;
    }

    public String getState() {
        return state;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        state = in.readString();
        redirectUrl = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(state);
        out.writeString(redirectUrl);
    }
}
