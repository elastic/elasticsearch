/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.secrets;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.SecretSettings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.common.oauth2.OAuth2ClusterSettings;
import org.elasticsearch.xpack.inference.common.oauth2.OAuth2Settings;
import org.elasticsearch.xpack.inference.common.oauth2.TokenCache;
import org.elasticsearch.xpack.inference.common.secrets.HeaderApplier;
import org.elasticsearch.xpack.inference.common.secrets.NoopSecretsApplier;
import org.elasticsearch.xpack.inference.common.secrets.SecretsApplier;
import org.elasticsearch.xpack.inference.services.openai.OpenAiServiceSettings;
import org.elasticsearch.xpack.inference.services.settings.ApiKeySecrets;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;
import static org.elasticsearch.xpack.inference.services.openai.OpenAiOAuth2Settings.REQUIRED_FIELDS;
import static org.elasticsearch.xpack.inference.services.openai.OpenAiOAuth2Settings.REQUIRED_FIELDS_DESCRIPTION;

/**
 * Builds the right {@link SecretsApplier} for an OpenAI model given its secret settings.
 *
 * <p>Three branches:
 * <ul>
 *   <li>null secrets (e.g. GET-without-secrets) -&gt; {@link NoopSecretsApplier}</li>
 *   <li>{@link DefaultSecretSettings} (api_key) -&gt; {@link HeaderApplier} with bearer header</li>
 *   <li>{@link OpenAiOAuth2SecretsSettings} -&gt; {@link OpenAiOAuth2Applier}</li>
 * </ul>
 *
 * <p>Cross-validates that the combination of secrets and service settings is consistent:
 * api_key alongside OAuth2 service settings, or an OAuth2 secret without OAuth2 service
 * settings, both raise a {@link ValidationException}.
 */
public final class OpenAiSecretsFactory {

    public static final String USE_CLIENT_SECRET_ERROR = OAuth2Settings.clientSecretRequiredError(REQUIRED_FIELDS);

    private OpenAiSecretsFactory() {}

    /** Simple api-key-only path; used by non-OAuth2 subclasses such as Groq. */
    public static SecretsApplier createSecretsApplier(@Nullable ApiKeySecrets apiKeySecrets) {
        return apiKeySecrets == null
            ? NoopSecretsApplier.INSTANCE
            : new HeaderApplier(() -> createAuthBearerHeader(apiKeySecrets.apiKey()));
    }

    public static SecretsApplier createSecretsApplier(
        String inferenceId,
        ThreadPool threadPool,
        TokenCache tokenCache,
        @Nullable SecretSettings secretSettings,
        OpenAiServiceSettings serviceSettings,
        OAuth2ClusterSettings oauth2ClusterSettings
    ) {
        return switch (secretSettings) {
            // null means the model is being retrieved without secrets (e.g. GET).
            // The Noop applier should never actually be invoked but we return non-null defensively.
            case null -> NoopSecretsApplier.INSTANCE;
            case DefaultSecretSettings apiKeySecrets -> {
                if (serviceSettings.oAuth2Settings() != null) {
                    throw new ValidationException().addValidationError(USE_CLIENT_SECRET_ERROR);
                }
                yield new HeaderApplier(() -> createAuthBearerHeader(apiKeySecrets.apiKey()));
            }
            case OpenAiOAuth2SecretsSettings oauth2Secrets -> {
                if (serviceSettings.oAuth2Settings() == null) {
                    throw new ValidationException().addValidationError(REQUIRED_FIELDS_DESCRIPTION);
                }
                yield OpenAiOAuth2Applier.of(
                    inferenceId,
                    threadPool,
                    tokenCache,
                    oauth2Secrets,
                    serviceSettings.oAuth2Settings(),
                    oauth2ClusterSettings
                );
            }
            default -> throw new IllegalArgumentException(
                "Unsupported OpenAI secret settings type: " + secretSettings.getClass().getName()
            );
        };
    }
}
