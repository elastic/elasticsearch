/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai.secrets;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.inference.SecretSettings;
import org.elasticsearch.inference.SettingsConfiguration;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.common.oauth2.OAuth2Secrets;
import org.elasticsearch.xpack.inference.services.openai.OpenAiOAuth2Settings;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.inference.ModelSecrets.SECRET_SETTINGS;
import static org.elasticsearch.xpack.inference.common.oauth2.OAuth2Secrets.CLIENT_SECRET_FIELD;

/**
 * OpenAI OAuth2 secret settings: holds the {@code client_secret} for the
 * client-credentials grant. Wraps {@link OAuth2Secrets} so the wire format is
 * shared with Azure's OAuth2 secrets type.
 */
public class OpenAiOAuth2SecretsSettings extends OpenAiSecretSettings {

    public static final String NAME = "openai_oauth2_client_secret";

    private final OAuth2Secrets secrets;

    public OpenAiOAuth2SecretsSettings(SecureString clientSecret) {
        this(new OAuth2Secrets(clientSecret));
    }

    public OpenAiOAuth2SecretsSettings(StreamInput in) throws IOException {
        this(new OAuth2Secrets(in));
    }

    private OpenAiOAuth2SecretsSettings(OAuth2Secrets secrets) {
        this.secrets = Objects.requireNonNull(secrets);
    }

    public SecureString clientSecret() {
        return secrets.clientSecret();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return OpenAiOAuth2Settings.OPENAI_OAUTH2_SETTINGS;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        secrets.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return secrets.toXContent(builder, params);
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

    @Override
    protected SecretSettings updated(Map<String, SecureString> provided) {
        return updateExactlyOneField(SECRET_SETTINGS, CLIENT_SECRET_FIELD, clientSecret(), provided, OpenAiOAuth2SecretsSettings::new);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        var that = (OpenAiOAuth2SecretsSettings) o;
        return Objects.equals(secrets, that.secrets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(secrets);
    }
}
