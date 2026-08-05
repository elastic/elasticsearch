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
import org.elasticsearch.inference.SecretSettings;
import org.elasticsearch.inference.SettingsConfiguration;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.inference.ModelConfigurations.SERVICE_SETTINGS;
import static org.elasticsearch.xpack.inference.common.oauth2.OAuth2Secrets.CLIENT_SECRET_FIELD;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalSecureString;

/**
 * An abstract class representing the secrets required for Azure OpenAI authentication.
 * This class enforces that exactly one of the supported authentication methods is used.
 */
public abstract class AzureOpenAiSecretSettings implements SecretSettings {

    public static final String API_KEY = "api_key";
    public static final String ENTRA_ID = "entra_id";

    private static final Set<String> SECRET_FIELDS = Set.of(API_KEY, ENTRA_ID, CLIENT_SECRET_FIELD);

    public static final String EXACTLY_ONE_SECRETS_FIELD_ERROR = SecretSettings.exactlyOneFieldError(SERVICE_SETTINGS, SECRET_FIELDS);

    public static final String EXACTLY_ONE_CONFIG_DESCRIPTION =
        "You must provide exactly one of API key, Entra ID, or OAuth2 client secret.";

    /**
     * Parses the map, validates exactly one auth field, returns the matching secret type.
     */
    public static AzureOpenAiSecretSettings fromMap(@Nullable Map<String, Object> map) {
        if (map == null) {
            return null;
        }

        var extractedSecretsMap = extractSecretsMap(map);

        SecretSettings.validateExactlyOneField(extractedSecretsMap, SERVICE_SETTINGS, SECRET_FIELDS);

        if (extractedSecretsMap.containsKey(API_KEY)) {
            return new AzureOpenAiEntraIdApiKeySecrets(extractedSecretsMap.get(API_KEY), null);
        }
        if (extractedSecretsMap.containsKey(ENTRA_ID)) {
            return new AzureOpenAiEntraIdApiKeySecrets(null, extractedSecretsMap.get(ENTRA_ID));
        }
        return new AzureOpenAiOAuth2Secrets(extractedSecretsMap.get(CLIENT_SECRET_FIELD));
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
    public AzureOpenAiSecretSettings newSecretSettings(Map<String, Object> newSecrets) {
        var extractedSecrets = extractSecretsMap(newSecrets);
        if (extractedSecrets.isEmpty()) {
            return this;
        }
        return updated(extractedSecrets);
    }

    /**
     * Extracts the supported secret fields from {@code map} into a typed map of non-null values.
     * Throws a {@link ValidationException} if any provided field is malformed (for example, an empty string).
     * The caller is responsible for validating which combinations of fields are allowed for the current flow.
     */
    private static Map<String, SecureString> extractSecretsMap(Map<String, Object> map) {
        var validationException = new ValidationException();
        var secureApiToken = extractOptionalSecureString(map, API_KEY, SERVICE_SETTINGS, validationException);
        var secureEntraId = extractOptionalSecureString(map, ENTRA_ID, SERVICE_SETTINGS, validationException);
        var clientSecret = extractOptionalSecureString(map, CLIENT_SECRET_FIELD, SERVICE_SETTINGS, validationException);
        validationException.throwIfValidationErrorsExist();

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
        return provided;
    }

    /** Apply a non-empty update; subclasses enforce which fields they allow. */
    protected abstract AzureOpenAiSecretSettings updated(Map<String, SecureString> provided);
}
