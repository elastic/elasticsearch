/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.oidc;

import com.nimbusds.oauth2.sdk.id.Issuer;

import org.elasticsearch.core.Nullable;

import java.net.URI;
import java.util.Objects;

/**
 * A Class that contains all the OpenID Connect Provider configuration
 */
public class OpenIdConnectProviderConfiguration {
    private final URI authorizationEndpoint;
    private final URI tokenEndpoint;
    private final URI userinfoEndpoint;
    private final URI endsessionEndpoint;
    private final Issuer issuer;
    private final String jwkSetPath;

    public OpenIdConnectProviderConfiguration(
        Issuer issuer,
        String jwkSetPath,
        URI authorizationEndpoint,
        @Nullable URI tokenEndpoint,
        @Nullable URI userinfoEndpoint,
        @Nullable URI endsessionEndpoint
    ) {
        this.authorizationEndpoint = Objects.requireNonNull(authorizationEndpoint, "Authorization Endpoint must be provided");
        this.tokenEndpoint = tokenEndpoint;
        this.userinfoEndpoint = userinfoEndpoint;
        this.endsessionEndpoint = endsessionEndpoint;
        this.issuer = Objects.requireNonNull(issuer, "OP Issuer must be provided");
        this.jwkSetPath = Objects.requireNonNull(jwkSetPath, "jwkSetUrl must be provided");
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

    public URI getEndsessionEndpoint() {
        return endsessionEndpoint;
    }

    public Issuer getIssuer() {
        return issuer;
    }

    public String getJwkSetPath() {
        return jwkSetPath;
    }
}
