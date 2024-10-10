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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalSecureString;

public class AzureOpenAiSecretSettings implements SecretSettings {

    public static final String NAME = "azure_openai_secret_settings";
    public static final String API_KEY = "api_key";
    public static final String ENTRA_ID = "entra_id";

    private final SecureString entraId;

    private final SecureString apiKey;

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

    public AzureOpenAiSecretSettings(@Nullable SecureString apiKey, @Nullable SecureString entraId) {
        Objects.requireNonNullElse(apiKey, entraId);
        this.apiKey = apiKey;
        this.entraId = entraId;
    }

    public AzureOpenAiSecretSettings(StreamInput in) throws IOException {
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
        return TransportVersions.V_8_14_0;
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
        AzureOpenAiSecretSettings that = (AzureOpenAiSecretSettings) object;
        return Objects.equals(entraId, that.entraId) && Objects.equals(apiKey, that.apiKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entraId, apiKey);
    }

    @Override
    public SecretSettings newSecretSettings(Map<String, Object> newSecrets) {
        return AzureOpenAiSecretSettings.fromMap(new HashMap<>(newSecrets));
    }
}
