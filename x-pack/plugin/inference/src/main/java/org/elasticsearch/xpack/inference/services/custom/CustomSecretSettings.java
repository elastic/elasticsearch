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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalMap;

public class CustomSecretSettings implements SecretSettings {
    public static final String NAME = "custom_secret_settings";
    public static final String SECRET_PARAMETERS = "secret_parameters";
    private final Map<String, Object> secretParameters;

    public static CustomSecretSettings fromMap(@Nullable Map<String, Object> map) {
        if (map == null) {
            return null;
        }

        ValidationException validationException = new ValidationException();

        Map<String, Object> requestSecretParamsMap = extractOptionalMap(map, SECRET_PARAMETERS, NAME, validationException);
        if (requestSecretParamsMap == null) {
            return null;
        } else {
            Map<String, Object> secureSecretParameters = new HashMap<>();
            for (String paramKey : requestSecretParamsMap.keySet()) {
                Object paramValue = requestSecretParamsMap.get(paramKey);
                secureSecretParameters.put(paramKey, paramValue);
            }
            return new CustomSecretSettings(secureSecretParameters);
        }
    }

    @Override
    public SecretSettings newSecretSettings(Map<String, Object> newSecrets) {
        return fromMap(new HashMap<>(newSecrets));
    }

    public CustomSecretSettings(@Nullable Map<String, Object> secretParameters) {
        this.secretParameters = secretParameters;
    }

    public CustomSecretSettings(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            secretParameters = in.readGenericMap();
        } else {
            secretParameters = null;
        }
    }

    public Map<String, Object> getSecretParameters() {
        return secretParameters;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (secretParameters != null) {
            builder.field(SECRET_PARAMETERS, secretParameters);
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
        return TransportVersions.V_8_15_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (secretParameters != null) {
            out.writeBoolean(true);
            out.writeGenericMap(secretParameters);
        } else {
            out.writeBoolean(false);
        }
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
