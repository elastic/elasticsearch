/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.secrets;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SecretSettings;
import org.elasticsearch.inference.SettingsConfiguration;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.common.oauth2.OAuth2Secrets.CLIENT_SECRET_FIELD;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalSecureString;

/**
 * An abstract class representing the secrets required for Azure OpenAI authentication.
 * This class enforces that exactly one of the supported authentication methods is used.
 */
public abstract class AzureOpenAiSecretSettings implements SecretSettings {

    public static final String API_KEY = "api_key";
    public static final String ENTRA_ID = "entra_id";

    public static final String EXACTLY_ONE_SECRETS_FIELD_ERROR = format(
        "[secret_settings] must have exactly one of [%s], [%s], or [%s] field set",
        API_KEY,
        ENTRA_ID,
        CLIENT_SECRET_FIELD
    );

    public static final String EXACTLY_ONE_CONFIG_DESCRIPTION =
        "You must provide exactly one of API key, Entra ID, or OAuth2 client secret.";

    /**
     * Parses the map, validates exactly one auth field, returns the matching secret type.
     */
    public static AzureOpenAiSecretSettings fromMap(@Nullable Map<String, Object> map) {
        if (map == null) {
            return null;
        }

        ValidationException validationException = new ValidationException();
        var secureApiToken = extractOptionalSecureString(map, API_KEY, ModelSecrets.SECRET_SETTINGS, validationException);
        var secureEntraId = extractOptionalSecureString(map, ENTRA_ID, ModelSecrets.SECRET_SETTINGS, validationException);
        var clientSecret = extractOptionalSecureString(map, CLIENT_SECRET_FIELD, ModelSecrets.SECRET_SETTINGS, validationException);

        var provided = new HashMap<String, SecureString>();
        if (secureApiToken != null) {
            provided.put(API_KEY, secureApiToken);
        }
        if (secureEntraId != null) {
            provided.put(ENTRA_ID, secureEntraId);
        }
        if (clientSecret != null) {
            provided.put(CLIENT_SECRET_FIELD, clientSecret);
        }

        if (provided.isEmpty()) {
            validationException.addValidationError(EXACTLY_ONE_SECRETS_FIELD_ERROR);
        } else if (provided.size() > 1) {
            validationException.addValidationError(EXACTLY_ONE_SECRETS_FIELD_ERROR + ", received: " + provided.keySet());
        }

        validationException.throwIfValidationErrorsExist();

        if (secureApiToken != null) {
            return new AzureOpenAiEntraIdApiKeySecrets(secureApiToken, null);
        }
        if (secureEntraId != null) {
            return new AzureOpenAiEntraIdApiKeySecrets(null, secureEntraId);
        }
        return new AzureOpenAiOAuth2Secrets(clientSecret);
    }

    public static Map<String, SettingsConfiguration> configurations(EnumSet<TaskType> supportedTaskTypes) {
        var configurationMap = new HashMap<String, SettingsConfiguration>();
        configurationMap.put(
            API_KEY,
            new SettingsConfiguration.Builder(supportedTaskTypes).setDescription(EXACTLY_ONE_CONFIG_DESCRIPTION)
                .setLabel("API Key")
                .setRequired(false)
                .setSensitive(true)
                .setUpdatable(true)
                .setType(SettingsConfigurationFieldType.STRING)
                .build()
        );
        configurationMap.put(
            ENTRA_ID,
            new SettingsConfiguration.Builder(supportedTaskTypes).setDescription(EXACTLY_ONE_CONFIG_DESCRIPTION)
                .setLabel("Entra ID")
                .setRequired(false)
                .setSensitive(true)
                .setUpdatable(true)
                .setType(SettingsConfigurationFieldType.STRING)
                .build()
        );
        configurationMap.putAll(AzureOpenAiOAuth2Secrets.getClientSecretConfiguration(supportedTaskTypes));
        return Collections.unmodifiableMap(configurationMap);
    }

    @Override
    public SecretSettings newSecretSettings(Map<String, Object> newSecrets) {
        return AzureOpenAiSecretSettings.fromMap(new HashMap<>(newSecrets));
    }
}
