/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.saml;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Response containing a SAML {@code &lt;AuthnRequest&gt;} for a specific realm.
 */
public final class SamlPrepareAuthenticationResponse extends ActionResponse {

    private final String realmName;
    private final String requestId;
    private final String redirectUrl;

    public SamlPrepareAuthenticationResponse(String realmName, String requestId, String redirectUrl) {
        this.realmName = realmName;
        this.requestId = requestId;
        this.redirectUrl = redirectUrl;
    }

    public String getRealmName() {
        return realmName;
    }

    public String getRequestId() {
        return requestId;
    }

    public String getRedirectUrl() {
        return redirectUrl;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(redirectUrl);
    }

}
