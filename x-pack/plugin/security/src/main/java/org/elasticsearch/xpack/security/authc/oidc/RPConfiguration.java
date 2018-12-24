/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.oidc;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A Class that contains all the OpenID Connect Relying Party configuration
 */
public class RPConfiguration {
    private final String clientId;
    private final String redirectUri;
    private final String responseType;
    private final List<String> requestedScopes;

    public RPConfiguration(String clientId, String redirectUri, String responseType, @Nullable List<String> requestedScopes) {
        this.clientId = Objects.requireNonNull(clientId, "RP Client ID must be provided");
        this.redirectUri = Objects.requireNonNull(redirectUri, "RP Redirect URI must be provided");
        if (Strings.hasText(responseType) == false) {
            throw new IllegalArgumentException("Response type must be provided");
        } else if (responseType.equals("code") == false && responseType.equals("implicit") == false) {
            throw new IllegalArgumentException("Invalid response type provided. Only code or implicit are allowed");
        } else {
            this.responseType = responseType;
        }
        if (null == requestedScopes || requestedScopes.isEmpty()) {
            this.requestedScopes = Collections.singletonList("openid");
        } else {
            this.requestedScopes = requestedScopes;
        }
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
