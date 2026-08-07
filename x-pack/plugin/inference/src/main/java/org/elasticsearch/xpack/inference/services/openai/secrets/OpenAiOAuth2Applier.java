/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.secrets;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpRequestBase;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.common.oauth2.OAuth2ClusterSettings;
import org.elasticsearch.xpack.inference.common.oauth2.OAuth2TokenFetcher;
import org.elasticsearch.xpack.inference.common.oauth2.OAuth2TokenSupplier;
import org.elasticsearch.xpack.inference.common.oauth2.TokenCache;
import org.elasticsearch.xpack.inference.common.secrets.SecretsApplier;
import org.elasticsearch.xpack.inference.services.openai.OpenAiOAuth2Settings;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.bearerToken;

/**
 * Stamps a bearer access token on outgoing OpenAI HTTP requests. Delegates to a
 * {@link TokenCache} for the token itself, supplying an {@link OAuth2TokenFetcher} that
 * the cache invokes on miss / expiry.
 */
public record OpenAiOAuth2Applier(String inferenceId, TokenCache cache, OAuth2TokenSupplier fetcher) implements SecretsApplier {

    public static OpenAiOAuth2Applier of(
        String inferenceId,
        ThreadPool threadPool,
        TokenCache cache,
        OpenAiOAuth2SecretsSettings oauth2Secrets,
        OpenAiOAuth2Settings oauth2Settings,
        OAuth2ClusterSettings oauth2ClusterSettings
    ) {
        var fetcher = new OAuth2TokenFetcher(
            inferenceId,
            oauth2Settings.tokenUrl(),
            oauth2Settings.clientId(),
            oauth2Secrets.clientSecret().toString(),
            oauth2Settings.scopes(),
            threadPool,
            oauth2ClusterSettings
        );
        return new OpenAiOAuth2Applier(inferenceId, cache, fetcher);
    }

    @Override
    public void applyTo(HttpRequestBase request, ActionListener<HttpRequestBase> listener) {
        cache.getToken(inferenceId, fetcher, listener.delegateFailureAndWrap((l, token) -> {
            request.setHeader(HttpHeaders.AUTHORIZATION, bearerToken(token.bearer()));
            l.onResponse(request);
        }));
    }

    @Override
    public void onAuthenticationFailure() {
        cache.invalidateOnlLocalNode(inferenceId);
    }
}
