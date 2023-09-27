/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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

public class ModelSecrets implements ToXContentObject, VersionedNamedWriteable {
    public static final String SECRETS = "secrets";
    private static final String NAME = "inference_model_secrets";
    private final SecretSettings secrets;

    public ModelSecrets() {
        this.secrets = null;
    }

    public ModelSecrets(@Nullable SecretSettings secrets) {
        // allow the secrets to be null in cases where the service does not have any secrets
        this.secrets = secrets;
    }

    public ModelSecrets(StreamInput in) throws IOException {
        this(in.readOptionalNamedWriteable(SecretSettings.class));
    }

    public SecretSettings getSecrets() {
        return secrets;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalNamedWriteable(secrets);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (secrets != null) {
            builder.field(SECRETS, secrets);
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
        return TransportVersions.INFERENCE_MODEL_SECRETS_ADDED;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ModelSecrets that = (ModelSecrets) o;
        return Objects.equals(secrets, that.secrets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(secrets);
    }
}
