/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.oauth2;

import com.nimbusds.oauth2.sdk.ClientCredentialsGrant;
import com.nimbusds.oauth2.sdk.Scope;
import com.nimbusds.oauth2.sdk.TokenRequest;
import com.nimbusds.oauth2.sdk.TokenResponse;
import com.nimbusds.oauth2.sdk.auth.ClientSecretBasic;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.id.ClientID;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.InferencePlugin;

import java.net.URI;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * Fetches OAuth2 bearer tokens via the client-credentials grant against a
 * customer-controlled IdP token endpoint, using the Nimbus
 * {@code oauth2-oidc-sdk}.
 *
 * <p>The blocking Nimbus {@code TokenRequest.send()} is dispatched to
 * {@link InferencePlugin#UTILITY_THREAD_POOL_NAME}; the result is delivered
 * asynchronously via {@link ActionListener}.
 *
 * <p>Hardcodes {@code client_secret_basic} per
 * <a href="https://datatracker.ietf.org/doc/html/rfc6749#section-2.3.1">RFC 6749 §2.3.1 default</a>.
 */
public class OAuth2TokenFetcher implements OAuth2TokenSupplier {

    private final URI tokenUri;
    private final ClientID clientId;
    private final Secret clientSecret;
    private final Scope scope;
    private final ThreadPool threadPool;
    private final String inferenceId;
    private final Clock clock;
    private final OAuth2ClusterSettings oauth2ClusterSettings;

    public OAuth2TokenFetcher(
        String inferenceId,
        URI tokenUri,
        String clientId,
        String clientSecret,
        List<String> scopes,
        ThreadPool threadPool,
        OAuth2ClusterSettings oauth2ClusterSettings
    ) {
        this(inferenceId, tokenUri, clientId, clientSecret, scopes, threadPool, oauth2ClusterSettings, Clock.systemUTC());
    }

    OAuth2TokenFetcher(
        String inferenceId,
        URI tokenUri,
        String clientId,
        String clientSecret,
        List<String> scopes,
        ThreadPool threadPool,
        OAuth2ClusterSettings oauth2ClusterSettings,
        Clock clock
    ) {
        this.inferenceId = Objects.requireNonNull(inferenceId);
        this.tokenUri = Objects.requireNonNull(tokenUri);
        this.clientId = new ClientID(Objects.requireNonNull(clientId));
        this.clientSecret = new Secret(Objects.requireNonNull(clientSecret));
        this.scope = new Scope(Objects.requireNonNull(scopes).toArray(String[]::new));
        this.threadPool = Objects.requireNonNull(threadPool);
        this.oauth2ClusterSettings = Objects.requireNonNull(oauth2ClusterSettings);
        this.clock = Objects.requireNonNull(clock);
    }

    @Override
    public void fetch(ActionListener<CachedToken> listener) {
        try {
            threadPool.executor(InferencePlugin.UTILITY_THREAD_POOL_NAME).execute(() -> fetchToken(listener));
        } catch (Exception e) {
            listener.onFailure(
                new ElasticsearchException(Strings.format("Failed to execute token fetch for inference id [%s]", inferenceId), e)
            );
        }
    }

    private void fetchToken(ActionListener<CachedToken> listener) {
        try {
            var clientAuth = new ClientSecretBasic(clientId, clientSecret);
            var request = new TokenRequest.Builder(tokenUri, clientAuth, new ClientCredentialsGrant()).scope(scope).build();
            var httpRequest = request.toHTTPRequest();
            httpRequest.setConnectTimeout(oauth2ClusterSettings.connectTimeoutMillis());
            httpRequest.setReadTimeout(oauth2ClusterSettings.readTimeoutMillis());
            var response = TokenResponse.parse(httpRequest.send());
            if (response.indicatesSuccess() == false) {
                var errorObject = response.toErrorResponse().getErrorObject();
                listener.onFailure(
                    new ElasticsearchException(
                        Strings.format(
                            "Failed to retrieve access token for request for inference id [%s]: [%s] %s",
                            inferenceId,
                            errorObject.getCode(),
                            errorObject.getDescription()
                        )
                    )
                );
                return;
            }

            var accessToken = response.toSuccessResponse().getTokens().getAccessToken();
            var expiresAt = Instant.now(clock).plusSeconds(accessToken.getLifetime());
            listener.onResponse(new CachedToken(accessToken.getValue(), expiresAt));
        } catch (Exception e) {
            listener.onFailure(
                new ElasticsearchException(
                    Strings.format("Failed to retrieve access token for request for inference id [%s]", inferenceId),
                    e
                )
            );
        }
    }
}
