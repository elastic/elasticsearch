/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.saml;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Response containing a SAML {@code &lt;LogoutRequest&gt;} for the current user
 */
public final class SamlLogoutResponse extends ActionResponse {

    private final String requestId;
    private final String redirectUrl;

    public SamlLogoutResponse(StreamInput in) throws IOException {
        super(in);
        requestId = in.readString();
        redirectUrl = in.readString();
    }

    public SamlLogoutResponse(String requestId, String redirectUrl) {
        this.requestId = requestId;
        this.redirectUrl = redirectUrl;
    }

    public String getRequestId() {
        return requestId;
    }

    public String getRedirectUrl() {
        return redirectUrl;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(requestId);
        out.writeString(redirectUrl);
    }

}
