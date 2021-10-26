/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.oidc;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.oauth2.sdk.ResponseType;
import com.nimbusds.oauth2.sdk.Scope;
import com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod;
import com.nimbusds.oauth2.sdk.id.ClientID;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.settings.SecureString;

import java.net.URI;
import java.util.Objects;

/**
 * A Class that contains all the OpenID Connect Relying Party configuration
 */
public class RelyingPartyConfiguration {
    private final ClientID clientId;
    private final SecureString clientSecret;
    private final URI redirectUri;
    private final ResponseType responseType;
    private final Scope requestedScope;
    private final JWSAlgorithm signatureAlgorithm;
    private final URI postLogoutRedirectUri;
    private final ClientAuthenticationMethod clientAuthenticationMethod;
    private final JWSAlgorithm clientAuthenticationJwtAlgorithm;

    public RelyingPartyConfiguration(ClientID clientId, SecureString clientSecret, URI redirectUri, ResponseType responseType,
                                     Scope requestedScope, JWSAlgorithm algorithm, ClientAuthenticationMethod clientAuthenticationMethod,
                                     JWSAlgorithm clientAuthenticationJwtAlgorithm, @Nullable URI postLogoutRedirectUri) {
        this.clientId = Objects.requireNonNull(clientId, "clientId must be provided");
        this.clientSecret = Objects.requireNonNull(clientSecret, "clientSecret must be provided");
        this.redirectUri = Objects.requireNonNull(redirectUri, "redirectUri must be provided");
        this.responseType = Objects.requireNonNull(responseType, "responseType must be provided");
        this.requestedScope = Objects.requireNonNull(requestedScope, "responseType must be provided");
        this.signatureAlgorithm = Objects.requireNonNull(algorithm, "algorithm must be provided");
        this.clientAuthenticationMethod = Objects.requireNonNull(clientAuthenticationMethod,
            "clientAuthenticationMethod must be provided");
        this.clientAuthenticationJwtAlgorithm = Objects.requireNonNull(clientAuthenticationJwtAlgorithm,
            "clientAuthenticationJwtAlgorithm must be provided");
        this.postLogoutRedirectUri = postLogoutRedirectUri;
    }

    public ClientID getClientId() {
        return clientId;
    }

    public SecureString getClientSecret() {
        return clientSecret;
    }

    public URI getRedirectUri() {
        return redirectUri;
    }

    public ResponseType getResponseType() {
        return responseType;
    }

    public Scope getRequestedScope() {
        return requestedScope;
    }

    public JWSAlgorithm getSignatureAlgorithm() {
        return signatureAlgorithm;
    }

    public URI getPostLogoutRedirectUri() {
        return postLogoutRedirectUri;
    }

    public ClientAuthenticationMethod getClientAuthenticationMethod() {
        return clientAuthenticationMethod;
    }

    public JWSAlgorithm getClientAuthenticationJwtAlgorithm() {
        return clientAuthenticationJwtAlgorithm;
    }
}
