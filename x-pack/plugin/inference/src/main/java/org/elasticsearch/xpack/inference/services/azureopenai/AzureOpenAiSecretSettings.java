/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureopenai;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.SecretSettings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalSecureString;

public record AzureOpenAiSecretSettings(@Nullable SecureString apiKey, @Nullable SecureString entraId) implements SecretSettings {

    public static final String NAME = "azure_openai_secret_settings";
    public static final String API_KEY = "api_key";
    public static final String ENTRA_ID = "entra_id";

    public static AzureOpenAiSecretSettings fromMap(@Nullable Map<String, Object> map) {
        if (map == null) {
            return null;
        }

        ValidationException validationException = new ValidationException();
        SecureString secureApiToken = extractOptionalSecureString(map, API_KEY, ModelSecrets.SECRET_SETTINGS, validationException);
        SecureString secureEntraId = extractOptionalSecureString(map, ENTRA_ID, ModelSecrets.SECRET_SETTINGS, validationException);

        if (secureApiToken == null && secureEntraId == null) {
            validationException.addValidationError(
                format("[secret_settings] must have either the [%s] or the [%s] key set", API_KEY, ENTRA_ID)
            );
        }

        if (secureApiToken != null && secureEntraId != null) {
            validationException.addValidationError(
                format("[secret_settings] must have only one of the [%s] or the [%s] key set", API_KEY, ENTRA_ID)
            );
        }

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new AzureOpenAiSecretSettings(secureApiToken, secureEntraId);
    }

    public AzureOpenAiSecretSettings {
        Objects.requireNonNullElse(apiKey, entraId);
    }

    public AzureOpenAiSecretSettings(StreamInput in) throws IOException {
        this(in.readOptionalSecureString(), in.readOptionalSecureString());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (apiKey != null) {
            builder.field(API_KEY, apiKey.toString());
        }

        if (entraId != null) {
            builder.field(ENTRA_ID, entraId.toString());
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
        return TransportVersions.ML_INFERENCE_AZURE_OPENAI_EMBEDDINGS;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalSecureString(apiKey);
        out.writeOptionalSecureString(entraId);
    }
}
