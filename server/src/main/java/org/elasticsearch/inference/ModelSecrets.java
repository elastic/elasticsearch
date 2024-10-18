/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Represents the portion of a model that contains sensitive data
 */
public class ModelSecrets implements ToXContentObject, VersionedNamedWriteable {
    public static final String SECRET_SETTINGS = "secret_settings";
    private static final String NAME = "inference_model_secrets";
    private final SecretSettings secretSettings;

    public ModelSecrets() {
        this.secretSettings = null;
    }

    public ModelSecrets(@Nullable SecretSettings secretSettings) {
        // allow the secrets to be null in cases where the service does not have any secrets
        this.secretSettings = secretSettings;
    }

    public ModelSecrets(StreamInput in) throws IOException {
        this(in.readOptionalNamedWriteable(SecretSettings.class));
    }

    public SecretSettings getSecretSettings() {
        return secretSettings;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalNamedWriteable(secretSettings);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (secretSettings != null) {
            builder.field(SECRET_SETTINGS, secretSettings);
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
        return TransportVersions.V_8_11_X;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ModelSecrets that = (ModelSecrets) o;
        return Objects.equals(secretSettings, that.secretSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(secretSettings);
    }
}
