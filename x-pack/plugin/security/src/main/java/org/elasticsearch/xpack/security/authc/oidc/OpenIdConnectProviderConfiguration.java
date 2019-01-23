/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.oidc;

import com.nimbusds.oauth2.sdk.id.Issuer;
import org.elasticsearch.common.Nullable;

import java.net.URI;
import java.net.URL;
import java.util.Objects;

/**
 * A Class that contains all the OpenID Connect Provider configuration
 */
public class OpenIdConnectProviderConfiguration {
    private final String providerName;
    private final URI authorizationEndpoint;
    private final URI tokenEndpoint;
    private final URI userinfoEndpoint;
    private final Issuer issuer;
    private final URL jwkSetUrl;

    public OpenIdConnectProviderConfiguration(String providerName, Issuer issuer, URL jwkSetUrl, URI authorizationEndpoint,
                                              URI tokenEndpoint, @Nullable URI userinfoEndpoint) {
        this.providerName = Objects.requireNonNull(providerName, "OP Name must be provided");
        this.authorizationEndpoint = Objects.requireNonNull(authorizationEndpoint, "Authorization Endpoint must be provided");
        this.tokenEndpoint = Objects.requireNonNull(tokenEndpoint, "Token Endpoint must be provided");
        this.userinfoEndpoint = userinfoEndpoint;
        this.issuer = Objects.requireNonNull(issuer, "OP Issuer must be provided");
        this.jwkSetUrl = Objects.requireNonNull(jwkSetUrl, "jwkSetUrl must be provided");
    }

    public String getProviderName() {
        return providerName;
    }

    public URI getAuthorizationEndpoint() {
        return authorizationEndpoint;
    }

    public URI getTokenEndpoint() {
        return tokenEndpoint;
    }

    public URI getUserinfoEndpoint() {
        return userinfoEndpoint;
    }

    public Issuer getIssuer() {
        return issuer;
    }

    public URL getJwkSetUrl() {
        return jwkSetUrl;
    }
}
