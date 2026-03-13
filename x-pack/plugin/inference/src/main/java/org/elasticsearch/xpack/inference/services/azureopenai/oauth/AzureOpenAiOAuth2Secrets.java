/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.oauth;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.inference.SettingsConfiguration;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.configuration.SettingsConfigurationFieldType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.azureopenai.AzureOpenAiSecretSettings;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.azureopenai.oauth.AzureOpenAiOAuth2Settings.AZURE_OPENAI_OAUTH_SETTINGS;
import static org.elasticsearch.xpack.inference.services.azureopenai.oauth.AzureOpenAiOAuth2Settings.REQUIRED_FIELDS;

public class AzureOpenAiOAuth2Secrets extends AzureOpenAiSecretSettings {

    public static final String NAME = "azure_openai_oauth2_client_secret";

    public static final String CLIENT_SECRET_FIELD = "client_secret";

    public static final String USE_CLIENT_SECRET_ERROR = Strings.format(
        "To use OAuth2 the [%s] field must be set, " + "either remove fields [%s], or provide the [%s] field.",
        CLIENT_SECRET_FIELD,
        REQUIRED_FIELDS,
        CLIENT_SECRET_FIELD
    );

    private final SecureString clientSecret;

    public AzureOpenAiOAuth2Secrets(String inferenceId, SecureString clientSecrets) {
        super(inferenceId);

        this.clientSecret = Objects.requireNonNull(clientSecrets);
    }

    public AzureOpenAiOAuth2Secrets(StreamInput in) throws IOException {
        this(in.readString(), in.readOptionalSecureString());
    }

    // TODO maybe make this default visibility only for testing
    public SecureString getClientSecret() {
        return clientSecret;
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
        out.writeString(inferenceId);
        out.writeSecureString(clientSecret);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CLIENT_SECRET_FIELD, clientSecret.toString());
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        AzureOpenAiOAuth2Secrets that = (AzureOpenAiOAuth2Secrets) o;
        return Objects.equals(clientSecret, that.clientSecret) && Objects.equals(inferenceId, that.inferenceId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientSecret, inferenceId);
    }

    public static Map<String, SettingsConfiguration> getClientSecretConfiguration() {
        return Map.of(
            AzureOpenAiOAuth2Secrets.CLIENT_SECRET_FIELD,
            new SettingsConfiguration.Builder(EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.COMPLETION, TaskType.CHAT_COMPLETION))
                .setDescription(EXACTLY_ONE_CONFIG_DESCRIPTION)
                .setLabel("OAuth2 Client Secret")
                .setRequired(false)
                .setSensitive(true)
                .setUpdatable(true)
                .setType(SettingsConfigurationFieldType.STRING)
                .build()
        );
    }
}
