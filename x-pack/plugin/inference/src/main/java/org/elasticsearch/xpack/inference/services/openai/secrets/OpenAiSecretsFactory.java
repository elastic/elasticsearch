/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.secrets;

import org.elasticsearch.xpack.inference.common.secrets.HeaderApplier;
import org.elasticsearch.xpack.inference.common.secrets.NoopSecretsApplier;
import org.elasticsearch.xpack.inference.common.secrets.SecretsApplier;
import org.elasticsearch.xpack.inference.services.settings.ApiKeySecrets;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;

/**
 * Factory for creating {@link SecretsApplier}s for the OpenAI service based on the provided {@link ApiKeySecrets}.
 */
public final class OpenAiSecretsFactory {

    private OpenAiSecretsFactory() {}

    public static SecretsApplier createSecretsApplier(ApiKeySecrets secretSettings) {
        return switch (secretSettings) {
            // This will be called with null if the model is being retrieved without the secrets (e.g. for a GET request).
            // The NoopSecretsApplier shouldn't actually be called but returning a non-null applier just in case.
            case null -> NoopSecretsApplier.INSTANCE;
            case DefaultSecretSettings apiKeySecrets -> new HeaderApplier(() -> createAuthBearerHeader(apiKeySecrets.apiKey()));
            default -> throw new IllegalArgumentException(
                "Unsupported OpenAI secret settings type: " + secretSettings.getClass().getName()
            );
        };
    }
}
