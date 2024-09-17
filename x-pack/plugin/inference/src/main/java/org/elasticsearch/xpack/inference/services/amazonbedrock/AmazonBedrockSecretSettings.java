/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock;

import org.elasticsearch.TransportVersion;
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

import static org.elasticsearch.TransportVersions.ML_INFERENCE_AMAZON_BEDROCK_ADDED;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredSecureString;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.ACCESS_KEY_FIELD;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockConstants.SECRET_KEY_FIELD;

public class AmazonBedrockSecretSettings implements SecretSettings {
    public static final String NAME = "amazon_bedrock_secret_settings";

    public final SecureString accessKey;
    public final SecureString secretKey;

    public static AmazonBedrockSecretSettings fromMap(@Nullable Map<String, Object> map) {
        if (map == null) {
            return null;
        }

        ValidationException validationException = new ValidationException();
        SecureString secureAccessKey = extractRequiredSecureString(
            map,
            ACCESS_KEY_FIELD,
            ModelSecrets.SECRET_SETTINGS,
            validationException
        );
        SecureString secureSecretKey = extractRequiredSecureString(
            map,
            SECRET_KEY_FIELD,
            ModelSecrets.SECRET_SETTINGS,
            validationException
        );

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new AmazonBedrockSecretSettings(secureAccessKey, secureSecretKey);
    }

    public AmazonBedrockSecretSettings(SecureString accessKey, SecureString secretKey) {
        this.accessKey = Objects.requireNonNull(accessKey);
        this.secretKey = Objects.requireNonNull(secretKey);
    }

    public AmazonBedrockSecretSettings(StreamInput in) throws IOException {
        this.accessKey = in.readSecureString();
        this.secretKey = in.readSecureString();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return ML_INFERENCE_AMAZON_BEDROCK_ADDED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeSecureString(accessKey);
        out.writeSecureString(secretKey);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field(ACCESS_KEY_FIELD, accessKey.toString());
        builder.field(SECRET_KEY_FIELD, secretKey.toString());

        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        AmazonBedrockSecretSettings that = (AmazonBedrockSecretSettings) object;
        return Objects.equals(accessKey, that.accessKey) && Objects.equals(secretKey, that.secretKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(accessKey, secretKey);
    }
}
