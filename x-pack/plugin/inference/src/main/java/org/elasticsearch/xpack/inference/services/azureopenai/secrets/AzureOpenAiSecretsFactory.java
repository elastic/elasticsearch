/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.secrets;

import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiServiceSettings;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiOAuth2Settings.REQUIRED_FIELDS_DESCRIPTION;
import static org.elasticsearch.xpack.inference.services.azureopenai.request.AzureOpenAiUtils.API_KEY_HEADER;
import static org.elasticsearch.xpack.inference.services.azureopenai.secrets.AzureOpenAiOAuth2Secrets.USE_CLIENT_SECRET_ERROR;
import static org.elasticsearch.xpack.inference.services.azureopenai.secrets.AzureOpenAiSecretSettings.EXACTLY_ONE_SECRETS_FIELD_ERROR;

/**
 * Factory for creating {@link AzureOpenAiSecretsApplier}s based on the provided {@link AzureOpenAiSecretSettings}.
 */
public final class AzureOpenAiSecretsFactory {

    private static final class NoopSecretsApplier implements AzureOpenAiSecretsApplier {
        @Override
        public void applyTo(HttpRequestBase request, ActionListener<HttpRequestBase> listener) {
            listener.onResponse(request);
        }
    }

    static final NoopSecretsApplier NOOP_SECRETS_APPLIER = new NoopSecretsApplier();

    private AzureOpenAiSecretsFactory() {}

    public static AzureOpenAiSecretsApplier createSecretsApplier(
        String inferenceId,
        ThreadPool threadPool,
        AzureOpenAiSecretSettings secretSettings,
        AzureOpenAiServiceSettings serviceSettings
    ) {
        return switch (secretSettings) {
            // This will be called with null if the model is being retrieved without the secrets (e.g. for a GET request)
            // The NOOP_SECRETS_APPLIER shouldn't actually be called but returning a non-null applier just in case
            case null -> NOOP_SECRETS_APPLIER;
            case AzureOpenAiEntraIdApiKeySecrets apiKeySecretSettings -> {
                if (serviceSettings.oAuth2Settings() != null) {
                    throw new ValidationException().addValidationError(USE_CLIENT_SECRET_ERROR);
                }

                if (isDefined(apiKeySecretSettings.apiKey())) {
                    yield new AzureOpenAiHeaderApplier(() -> new BasicHeader(API_KEY_HEADER, apiKeySecretSettings.apiKey().toString()));
                } else if (isDefined(apiKeySecretSettings.entraId())) {
                    yield new AzureOpenAiHeaderApplier(() -> createAuthBearerHeader(apiKeySecretSettings.entraId()));
                }

                throw new IllegalArgumentException(EXACTLY_ONE_SECRETS_FIELD_ERROR);
            }
            case AzureOpenAiOAuth2Secrets oauth2Secrets -> {
                if (serviceSettings.oAuth2Settings() == null) {
                    throw new ValidationException().addValidationError(REQUIRED_FIELDS_DESCRIPTION);
                }

                yield AzureOpenAiOAuth2Applier.of(inferenceId, threadPool, oauth2Secrets, serviceSettings);
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
