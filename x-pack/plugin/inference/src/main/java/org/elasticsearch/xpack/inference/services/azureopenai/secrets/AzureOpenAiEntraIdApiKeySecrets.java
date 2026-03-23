/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai.secrets;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Azure OpenAI secret settings for API key or Entra ID only.
 * Holds exactly one of the two (the other is null).
 */
public class AzureOpenAiEntraIdApiKeySecrets extends AzureOpenAiSecretSettings {

    public static final String NAME = "azure_openai_secret_settings";

    private final SecureString entraId;
    private final SecureString apiKey;

    public AzureOpenAiEntraIdApiKeySecrets(@Nullable SecureString apiKey, @Nullable SecureString entraId) {
        Objects.requireNonNullElse(apiKey, entraId);

        if (apiKey != null && entraId != null) {
            throw new IllegalArgumentException("Only one of apiKey or entraId can be set, but both were provided");
        }
        this.apiKey = apiKey;
        this.entraId = entraId;
    }

    public AzureOpenAiEntraIdApiKeySecrets(StreamInput in) throws IOException {
        this(in.readOptionalSecureString(), in.readOptionalSecureString());
    }

    public SecureString apiKey() {
        return apiKey;
    }

    public SecureString entraId() {
        return entraId;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (apiKey != null) {
            builder.field(AzureOpenAiSecretSettings.API_KEY, apiKey.toString());
        } else if (entraId != null) {
            builder.field(AzureOpenAiSecretSettings.ENTRA_ID, entraId.toString());
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.minimumCompatible();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalSecureString(apiKey);
        out.writeOptionalSecureString(entraId);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        AzureOpenAiEntraIdApiKeySecrets that = (AzureOpenAiEntraIdApiKeySecrets) object;
        return Objects.equals(entraId, that.entraId) && Objects.equals(apiKey, that.apiKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entraId, apiKey);
    }
}
