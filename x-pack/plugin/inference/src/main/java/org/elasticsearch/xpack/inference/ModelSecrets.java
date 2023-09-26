/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class ModelSecrets implements ToXContentObject, VersionedNamedWriteable {
    public static final String MODEL_ID = "model_id";
    public static final String SERVICE = "service";
    public static final String SECRETS = "secrets";
    private static final String NAME = "inference_model_secrets";
    private final String modelId;
    private final TaskType taskType;
    private final String service;
    private final SecretSettings secrets;

    public ModelSecrets(String modelId, TaskType taskType, String service, SecretSettings secrets) {
        this.modelId = Objects.requireNonNull(modelId);
        this.taskType = Objects.requireNonNull(taskType);
        this.service = Objects.requireNonNull(service);
        this.secrets = secrets;
    }

    public ModelSecrets(ModelConfigurations configurations) {
        this(configurations.getModelId(), configurations.getTaskType(), configurations.getService(), null);
    }

    public ModelSecrets(StreamInput in) throws IOException {
        this(in.readString(), in.readEnum(TaskType.class), in.readString(), in.readOptionalNamedWriteable(SecretSettings.class));
    }

    public String getModelId() {
        return modelId;
    }

    public TaskType getTaskType() {
        return taskType;
    }

    public String getService() {
        return service;
    }

    public SecretSettings getSecrets() {
        return secrets;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(modelId);
        out.writeEnum(taskType);
        out.writeString(service);
        out.writeOptionalNamedWriteable(secrets);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(MODEL_ID, modelId);
        builder.field(TaskType.NAME, taskType.toString());
        builder.field(SERVICE, service);

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
        return Objects.equals(modelId, that.modelId)
            && taskType == that.taskType
            && Objects.equals(service, that.service)
            && Objects.equals(secrets, that.secrets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, taskType, service, secrets);
    }
}
