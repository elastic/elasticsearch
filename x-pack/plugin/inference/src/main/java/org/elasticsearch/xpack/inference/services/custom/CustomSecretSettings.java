/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.SecretSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.settings.SerializableSecureString;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.convertMapStringsToSecureString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalMap;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeNullValues;

public class CustomSecretSettings implements SecretSettings {
    public static final String NAME = "custom_secret_settings";
    public static final String SECRET_PARAMETERS = "secret_parameters";

    public static CustomSecretSettings fromMap(@Nullable Map<String, Object> map) {
        if (map == null) {
            return null;
        }

        ValidationException validationException = new ValidationException();

        Map<String, Object> requestSecretParamsMap = extractOptionalMap(map, SECRET_PARAMETERS, NAME, validationException);
        removeNullValues(requestSecretParamsMap);
        var secureStringMap = convertMapStringsToSecureString(requestSecretParamsMap, SECRET_PARAMETERS, validationException);

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new CustomSecretSettings(secureStringMap);
    }

    private final Map<String, SerializableSecureString> secretParameters;

    @Override
    public SecretSettings newSecretSettings(Map<String, Object> newSecrets) {
        return fromMap(new HashMap<>(newSecrets));
    }

    public CustomSecretSettings(@Nullable Map<String, SerializableSecureString> secretParameters) {
        this.secretParameters = Objects.requireNonNullElse(secretParameters, Map.of());
    }

    public CustomSecretSettings(StreamInput in) throws IOException {
        secretParameters = in.readImmutableMap(SerializableSecureString::new);
    }

    public Map<String, SerializableSecureString> getSecretParameters() {
        return secretParameters;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (secretParameters.isEmpty() == false) {
            builder.startObject(SECRET_PARAMETERS);
            {
                for (var entry : secretParameters.entrySet()) {
                    builder.field(entry.getKey(), entry.getValue());
                }
            }
            builder.endObject();
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
        return TransportVersions.INFERENCE_CUSTOM_SERVICE_ADDED_8_19;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(secretParameters, (streamOutput, v) -> { v.writeTo(streamOutput); });
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CustomSecretSettings that = (CustomSecretSettings) o;
        return Objects.equals(secretParameters, that.secretParameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(secretParameters);
    }
}
