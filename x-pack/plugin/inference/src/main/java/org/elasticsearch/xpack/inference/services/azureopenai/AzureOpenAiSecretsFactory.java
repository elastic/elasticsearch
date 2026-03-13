/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai;

import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;

import org.apache.hc.core5.http.HttpHeaders;
import org.apache.http.Header;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.message.BasicHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.services.azureopenai.oauth2.AzureOpenAiOAuth2Secrets;

import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;
import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiSecretSettings.EXACTLY_ONE_SECRETS_FIELD_ERROR;
import static org.elasticsearch.xpack.inference.services.azureopenai.oauth2.AzureOpenAiOAuth2Secrets.USE_CLIENT_SECRET_ERROR;
import static org.elasticsearch.xpack.inference.services.azureopenai.oauth2.AzureOpenAiOAuth2Settings.REQUIRED_FIELDS_DESCRIPTION;
import static org.elasticsearch.xpack.inference.services.azureopenai.request.AzureOpenAiUtils.API_KEY_HEADER;

public final class AzureOpenAiSecretsFactory {

    private static final Logger logger = LogManager.getLogger(AzureOpenAiSecretsFactory.class);

    private AzureOpenAiSecretsFactory() {}

    public interface Applier {
        void applyTo(HttpRequestBase request, ActionListener<HttpRequestBase> listener);
    }

    public record SimpleHeaderApplier(Supplier<Header> headerSupplier) implements Applier {

        public SimpleHeaderApplier(Supplier<Header> headerSupplier) {
            this.headerSupplier = Objects.requireNonNull(headerSupplier);
        }

        @Override
        public void applyTo(HttpRequestBase request, ActionListener<HttpRequestBase> listener) {
            request.setHeader(headerSupplier.get());
            listener.onResponse(request);
        }
    }

    public record OAuth2Applier(String inferenceId, ClientSecretCredential clientSecretCredential, TokenRequestContext tokenRequestContext)
        implements
            Applier {

        public static OAuth2Applier of(
            String inferenceId,
            ThreadPool threadPool,
            AzureOpenAiOAuth2Secrets oauth2Secrets,
            AzureOpenAiServiceSettings serviceSettings
        ) {
            if (serviceSettings.oAuth2Settings() == null) {
                throw new ValidationException().addValidationError(REQUIRED_FIELDS_DESCRIPTION);
            }

            var credential = new ClientSecretCredentialBuilder().tenantId(serviceSettings.oAuth2Settings().getTenantId())
                .clientId(serviceSettings.oAuth2Settings().getClientId())
                .clientSecret(oauth2Secrets.getClientSecret().toString())
                .executorService(threadPool.executor(UTILITY_THREAD_POOL_NAME))
                .build();

            var tokenRequestContext = new TokenRequestContext().setScopes(serviceSettings.oAuth2Settings().getScopes());

            return new OAuth2Applier(inferenceId, credential, tokenRequestContext);
        }

        @Override
        public void applyTo(HttpRequestBase request, ActionListener<HttpRequestBase> listener) {
            try {
                long start = System.currentTimeMillis();
                clientSecretCredential.getToken(tokenRequestContext).subscribe(token -> {
                    long end = System.currentTimeMillis();
                    long duration = end - start;
                    logger.warn("Successfully retrieved access token for inference id: [{}] in [{}] ms", inferenceId, duration);
                    String authorizationHeader = "Bearer " + token.getToken();

                    request.setHeader(HttpHeaders.AUTHORIZATION, authorizationHeader);
                    listener.onResponse(request);
                },
                    e -> listener.onFailure(
                        new ElasticsearchException(
                            Strings.format("Failed to retrieve access token for Azure OpenAI request for inference id: [%s]", inferenceId),
                            e
                        )
                    )
                );
            } catch (Exception e) {
                listener.onFailure(
                    new ElasticsearchException(
                        Strings.format(
                            "Failed attempting to retrieve access token for Azure OpenAI request for inference id: [%s]",
                            inferenceId
                        ),
                        e
                    )
                );
            }
        }
    }

    public static Applier createSecretsApplier(
        String inferenceId,
        ThreadPool threadPool,
        AzureOpenAiSecretSettings secretSettings,
        AzureOpenAiServiceSettings serviceSettings
    ) {
        return switch (secretSettings) {
            // This will be called with null if the model is being retrieved without the secrets (e.g. for a GET request)
            case null -> null;
            case AzureOpenAiEntraIdApiKeySecrets apiKeySecretSettings -> {
                if (serviceSettings.oAuth2Settings() != null) {
                    throw new ValidationException().addValidationError(USE_CLIENT_SECRET_ERROR);
                }

                if (isDefined(apiKeySecretSettings.apiKey())) {
                    yield new SimpleHeaderApplier(() -> new BasicHeader(API_KEY_HEADER, apiKeySecretSettings.apiKey().toString()));
                } else if (isDefined(apiKeySecretSettings.entraId())) {
                    yield new SimpleHeaderApplier(() -> createAuthBearerHeader(apiKeySecretSettings.entraId()));
                }

                throw new IllegalArgumentException(EXACTLY_ONE_SECRETS_FIELD_ERROR);
            }
            case AzureOpenAiOAuth2Secrets oauth2Secrets -> {
                if (serviceSettings.oAuth2Settings() == null) {
                    throw new ValidationException().addValidationError(REQUIRED_FIELDS_DESCRIPTION);
                }

                yield OAuth2Applier.of(inferenceId, threadPool, oauth2Secrets, serviceSettings);
            }
            default -> throw new IllegalArgumentException(
                "Unsupported Azure OpenAI secret settings type: " + secretSettings.getClass().getName()
            );
        };
    }

    private static boolean isDefined(SecureString str) {
        return str != null && str.isEmpty() == false;
    }
}
