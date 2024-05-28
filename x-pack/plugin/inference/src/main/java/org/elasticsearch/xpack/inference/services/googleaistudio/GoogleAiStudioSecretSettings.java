/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googleaistudio;

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

public class GoogleAiStudioSecretSettings implements SecretSettings {

    public static final String NAME = "google_ai_studio_secret_settings";
    public static final String API_KEY = "api_key";

    private final SecureString apiKey;

    public static GoogleAiStudioSecretSettings fromMap(@Nullable Map<String, Object> map) {
        if (map == null) {
            return null;
        }

        ValidationException validationException = new ValidationException();
        SecureString secureApiKey = extractOptionalSecureString(map, API_KEY, ModelSecrets.SECRET_SETTINGS, validationException);

        if (secureApiKey == null) {
            validationException.addValidationError(format("[secret_settings] must have [%s] set", API_KEY));
        }

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new GoogleAiStudioSecretSettings(secureApiKey);
    }

    public GoogleAiStudioSecretSettings(SecureString apiKey) {
        Objects.requireNonNull(apiKey);
        this.apiKey = apiKey;
    }

    public GoogleAiStudioSecretSettings(StreamInput in) throws IOException {
        this(in.readOptionalSecureString());
    }

    public SecureString apiKey() {
        return apiKey;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (apiKey != null) {
            builder.field(API_KEY, apiKey.toString());
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
        return TransportVersions.ML_INFERENCE_GOOGLE_AI_STUDIO_COMPLETION_ADDED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalSecureString(apiKey);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        GoogleAiStudioSecretSettings that = (GoogleAiStudioSecretSettings) object;
        return Objects.equals(apiKey, that.apiKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(apiKey);
    }
}
