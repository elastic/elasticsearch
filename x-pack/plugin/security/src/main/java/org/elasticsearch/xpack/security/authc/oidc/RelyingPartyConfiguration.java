/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.oidc;

import java.util.List;
import java.util.Objects;

/**
 * A Class that contains all the OpenID Connect Relying Party configuration
 */
public class RelyingPartyConfiguration {
    private final String clientId;
    private final String redirectUri;
    private final String responseType;
    private final List<String> requestedScopes;

    public RelyingPartyConfiguration(String clientId, String redirectUri, String responseType, List<String> requestedScopes) {
        this.clientId = Objects.requireNonNull(clientId, "clientId must be provided");
        this.redirectUri = Objects.requireNonNull(redirectUri, "redirectUri must be provided");
        this.responseType = Objects.requireNonNull(responseType, "responseType must be provided");
        this.requestedScopes = Objects.requireNonNull(requestedScopes, "responseType must be provided");
    }

    public String getClientId() {
        return clientId;
    }

    public String getRedirectUri() {
        return redirectUri;
    }

    public String getResponseType() {
        return responseType;
    }

    public List<String> getRequestedScopes() {
        return requestedScopes;
    }
}
