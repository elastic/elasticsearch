/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai;

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

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredSecureString;

public class GoogleVertexAiSecretSettings implements SecretSettings {

    public static final String NAME = "google_vertex_ai_secret_settings";

    public static final String SERVICE_ACCOUNT_JSON = "service_account_json";

    final SecureString serviceAccountJson;

    public static GoogleVertexAiSecretSettings fromMap(@Nullable Map<String, Object> map) {
        if (map == null) {
            return null;
        }

        ValidationException validationException = new ValidationException();
        SecureString secureServiceAccountJson = extractRequiredSecureString(
            map,
            SERVICE_ACCOUNT_JSON,
            ModelSecrets.SECRET_SETTINGS,
            validationException
        );

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new GoogleVertexAiSecretSettings(secureServiceAccountJson);
    }

    public GoogleVertexAiSecretSettings(SecureString serviceAccountJson) {
        this.serviceAccountJson = Objects.requireNonNull(serviceAccountJson);
    }

    public GoogleVertexAiSecretSettings(StreamInput in) throws IOException {
        this(in.readSecureString());
    }

    public SecureString serviceAccountJson() {
        return serviceAccountJson;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field(SERVICE_ACCOUNT_JSON, serviceAccountJson.toString());

        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_15_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeSecureString(serviceAccountJson);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        GoogleVertexAiSecretSettings that = (GoogleVertexAiSecretSettings) object;
        return Objects.equals(serviceAccountJson, that.serviceAccountJson);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serviceAccountJson);
    }

    @Override
    public SecretSettings newSecretSettings(Map<String, Object> newSecrets) {
        return GoogleVertexAiSecretSettings.fromMap(new HashMap<>(newSecrets));
    }
}
