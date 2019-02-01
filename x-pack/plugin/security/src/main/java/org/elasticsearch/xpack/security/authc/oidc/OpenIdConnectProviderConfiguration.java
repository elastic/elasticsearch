/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.oidc;

import org.elasticsearch.common.Nullable;

import java.util.Objects;

/**
 * A Class that contains all the OpenID Connect Provider configuration
 */
public class OpenIdConnectProviderConfiguration {
    private final String providerName;
    private final String authorizationEndpoint;
    private final String tokenEndpoint;
    private final String userinfoEndpoint;
    private final String endsessionEndpoint;
    private final String issuer;

    public OpenIdConnectProviderConfiguration(String providerName, String issuer, String authorizationEndpoint,
                                              @Nullable String tokenEndpoint, @Nullable String userinfoEndpoint,
                                              @Nullable String endsessionEndpoint) {
        this.providerName = Objects.requireNonNull(providerName, "OP Name must be provided");
        this.authorizationEndpoint = Objects.requireNonNull(authorizationEndpoint, "Authorization Endpoint must be provided");
        this.tokenEndpoint = tokenEndpoint;
        this.userinfoEndpoint = userinfoEndpoint;
        this.endsessionEndpoint = endsessionEndpoint;
        this.issuer = Objects.requireNonNull(issuer, "OP Issuer must be provided");
    }

    public String getProviderName() {
        return providerName;
    }

    public String getAuthorizationEndpoint() {
        return authorizationEndpoint;
    }

    public String getTokenEndpoint() {
        return tokenEndpoint;
    }

    public String getUserinfoEndpoint() {
        return userinfoEndpoint;
    }

    public String getEndsessionEndpoint() {
        return endsessionEndpoint;
    }

    public String getIssuer() {
        return issuer;
    }
}
