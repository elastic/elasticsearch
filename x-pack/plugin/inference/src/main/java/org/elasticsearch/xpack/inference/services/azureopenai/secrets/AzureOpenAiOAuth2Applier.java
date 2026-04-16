/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.secrets;

import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpRequestBase;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceSettings;

import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;
import static org.elasticsearch.xpack.inference.external.request.RequestUtils.bearerToken;
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiOAuth2Settings.REQUIRED_FIELDS_DESCRIPTION;

/**
 * Applies OAuth2 credentials to Azure OpenAI requests. Uses the Azure Identity library to
 * retrieve an access token using the client credentials flow.
 * @param inferenceId The ID of the inference entity
 * @param clientSecretCredential The Azure Identity credential used to retrieve access tokens for Azure OpenAI requests
 * @param tokenRequestContext The context used when requesting tokens, contains the scopes to request for the token
 */
public record AzureOpenAiOAuth2Applier(
    String inferenceId,
    ClientSecretCredential clientSecretCredential,
    TokenRequestContext tokenRequestContext
) implements AzureOpenAiSecretsApplier {
    public static AzureOpenAiOAuth2Applier of(
        String inferenceId,
        ThreadPool threadPool,
        AzureOpenAiOAuth2Secrets oauth2Secrets,
        AzureOpenAiServiceSettings serviceSettings
    ) {
        if (serviceSettings.oAuth2Settings() == null) {
            throw new ValidationException().addValidationError(REQUIRED_FIELDS_DESCRIPTION);
        }

        var credential = new ClientSecretCredentialBuilder().tenantId(serviceSettings.oAuth2Settings().tenantId())
            .clientId(serviceSettings.oAuth2Settings().clientId())
            .clientSecret(oauth2Secrets.getClientSecret().toString())
            .executorService(threadPool.executor(UTILITY_THREAD_POOL_NAME))
            .build();

        var tokenRequestContext = new TokenRequestContext().setScopes(serviceSettings.oAuth2Settings().scopes());

        return new AzureOpenAiOAuth2Applier(inferenceId, credential, tokenRequestContext);
    }

    @Override
    public void applyTo(HttpRequestBase request, ActionListener<HttpRequestBase> listener) {
        try {
            clientSecretCredential.getToken(tokenRequestContext).subscribe(token -> {
                request.setHeader(HttpHeaders.AUTHORIZATION, bearerToken(token.getToken()));
                listener.onResponse(request);
            }, e -> onFailure(listener, e));
        } catch (Exception e) {
            onFailure(listener, e);
        }
    }

    private void onFailure(ActionListener<HttpRequestBase> listener, Throwable e) {
        listener.onFailure(
            new ElasticsearchException(
                Strings.format("Failed attempting to retrieve access token for Azure OpenAI request for inference id: [%s]", inferenceId),
                e
            )
        );
    }
}
