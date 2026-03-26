/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.secrets;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.inference.SettingsConfiguration;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.common.oauth2.OAuth2Secrets;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.common.oauth2.OAuth2Secrets.CLIENT_SECRET_FIELD;
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiOAuth2Settings.AZURE_OPENAI_OAUTH_SETTINGS;
import static org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiOAuth2Settings.REQUIRED_FIELDS;

/**
 * Represents the secrets required for Azure OpenAI OAuth2 client credentials flow authentication.
 */
public class AzureOpenAiOAuth2Secrets extends AzureOpenAiSecretSettings {

    public static final String NAME = "azure_openai_oauth2_client_secret";

    public static final String USE_CLIENT_SECRET_ERROR = Strings.format(
        "To use OAuth2 the [%1$s] field must be set, either remove fields [%2$s], or provide the [%1$s] field.",
        CLIENT_SECRET_FIELD,
        REQUIRED_FIELDS
    );

    private final OAuth2Secrets secrets;

    public AzureOpenAiOAuth2Secrets(SecureString clientSecrets) {
        this(new OAuth2Secrets(clientSecrets));
    }

    public AzureOpenAiOAuth2Secrets(StreamInput in) throws IOException {
        this(new OAuth2Secrets(in));
    }

    private AzureOpenAiOAuth2Secrets(OAuth2Secrets oAuth2Secrets) {
        this.secrets = Objects.requireNonNull(oAuth2Secrets);
    }

    public SecureString getClientSecret() {
        return secrets.clientSecret();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return AZURE_OPENAI_OAUTH_SETTINGS;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        secrets.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return secrets.toXContent(builder, params);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        AzureOpenAiOAuth2Secrets that = (AzureOpenAiOAuth2Secrets) o;
        return Objects.equals(secrets, that.secrets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(secrets);
    }

    public static Map<String, SettingsConfiguration> getClientSecretConfiguration(EnumSet<TaskType> supportedTaskTypes) {
        return Map.of(
            CLIENT_SECRET_FIELD,
            new SettingsConfiguration.Builder(supportedTaskTypes).setDescription(EXACTLY_ONE_CONFIG_DESCRIPTION)
                .setLabel("OAuth2 Client Secret")
                .setRequired(false)
                .setSensitive(true)
                .setUpdatable(true)
                .setType(SettingsConfigurationFieldType.STRING)
                .build()
        );
    }
}
