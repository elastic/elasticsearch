/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.secrets;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.SecretSettings;
import org.elasticsearch.inference.SettingsConfiguration;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.inference.ModelSecrets.SECRET_SETTINGS;
import static org.elasticsearch.xpack.inference.common.oauth2.OAuth2Secrets.CLIENT_SECRET_FIELD;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalSecureString;
import static org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings.API_KEY;

/**
 * Abstract base class for OpenAI secret settings. Subclasses hold the concrete authentication
 * credentials (OAuth2 client secret). The static {@link #fromMap} factory also handles the
 * api_key path by returning a {@link DefaultSecretSettings} directly.
 *
 * <p>Enforces that exactly one of {@code api_key} or {@code client_secret} is set when
 * a secrets map is supplied. Mirrors the pattern in {@code AzureOpenAiSecretSettings}.
 */
public abstract class OpenAiSecretSettings implements SecretSettings {

    private static final Set<String> SECRET_FIELDS = Set.of(API_KEY, CLIENT_SECRET_FIELD);

    public static final String EXACTLY_ONE_SECRETS_FIELD_ERROR = SecretSettings.exactlyOneFieldError(
        ModelConfigurations.SERVICE_SETTINGS,
        SECRET_FIELDS
    );

    public static final String EXACTLY_ONE_CONFIG_DESCRIPTION = "You must provide exactly one of API key or OAuth2 client secret.";

    /**
     * Parses the map, validates exactly one auth field, and returns either a
     * {@link DefaultSecretSettings} (api_key path) or {@link OpenAiOAuth2SecretsSettings} (client_secret path).
     */
    public static SecretSettings fromMap(@Nullable Map<String, Object> map) {
        if (map == null) {
            return null;
        }

        var extractedSecretsMap = extractSecretsMap(map);

        SecretSettings.validateExactlyOneField(extractedSecretsMap, ModelConfigurations.SERVICE_SETTINGS, SECRET_FIELDS);

        if (extractedSecretsMap.containsKey(API_KEY)) {
            return new DefaultSecretSettings(extractedSecretsMap.get(API_KEY));
        }
        return new OpenAiOAuth2SecretsSettings(extractedSecretsMap.get(CLIENT_SECRET_FIELD));
    }

    /**
     * Returns a merged configuration map containing both the api_key and client_secret
     * configuration entries, suitable for use in the inference services API.
     */
    public static Map<String, SettingsConfiguration> configurations(EnumSet<TaskType> supportedTaskTypes) {
        var configurationMap = new HashMap<String, SettingsConfiguration>();
        configurationMap.putAll(
            DefaultSecretSettings.toSettingsConfigurationWithDescription(
                Strings.format(
                    "The OpenAI API authentication key. For more details about generating OpenAI API keys, "
                        + "refer to the https://platform.openai.com/account/api-keys. %s",
                    EXACTLY_ONE_CONFIG_DESCRIPTION
                ),
                supportedTaskTypes,
                false
            )
        );
        configurationMap.putAll(OpenAiOAuth2SecretsSettings.getClientSecretConfiguration(supportedTaskTypes));
        return Collections.unmodifiableMap(configurationMap);
    }

    @Override
    public SecretSettings newSecretSettings(Map<String, Object> newSecrets) {
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
        var apiKey = DefaultSecretSettings.extractOptionalApiKey(map, SECRET_SETTINGS, validationException);
        var clientSecret = extractOptionalSecureString(map, CLIENT_SECRET_FIELD, SECRET_SETTINGS, validationException);
        validationException.throwIfValidationErrorsExist();

        var provided = new HashMap<String, SecureString>();
        if (apiKey != null) {
            provided.put(API_KEY, apiKey);
        }
        if (clientSecret != null) {
            provided.put(CLIENT_SECRET_FIELD, clientSecret);
        }
        return provided;
    }

    /** Apply a non-empty update; subclasses enforce which fields they allow. */
    protected abstract SecretSettings updated(Map<String, SecureString> provided);
}
