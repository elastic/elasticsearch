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
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class ModelConfigurations implements ToFilteredXContentObject, VersionedNamedWriteable {

    // Due to refactoring, we now have different field names for the inference ID when it is serialized and stored to an index vs when it
    // is returned as part of a GetInferenceModelAction
    public static final String INDEX_ONLY_ID_FIELD_NAME = "model_id";
    public static final String INFERENCE_ID_FIELD_NAME = "inference_id";
    public static final String USE_ID_FOR_INDEX = "for_index";
    public static final String SERVICE = "service";
    public static final String SERVICE_SETTINGS = "service_settings";
    public static final String TASK_SETTINGS = "task_settings";
    public static final String CHUNKING_SETTINGS = "chunking_settings";
    private static final String NAME = "inference_model";

    public static ModelConfigurations of(Model model, TaskSettings taskSettings) {
        Objects.requireNonNull(model);
        Objects.requireNonNull(taskSettings);

        return new ModelConfigurations(
            model.getConfigurations().getInferenceEntityId(),
            model.getConfigurations().getTaskType(),
            model.getConfigurations().getService(),
            model.getServiceSettings(),
            taskSettings,
            model.getConfigurations().getChunkingSettings()
        );
    }

    public static ModelConfigurations of(Model model, ServiceSettings serviceSettings) {
        Objects.requireNonNull(model);
        Objects.requireNonNull(serviceSettings);

        return new ModelConfigurations(
            model.getConfigurations().getInferenceEntityId(),
            model.getConfigurations().getTaskType(),
            model.getConfigurations().getService(),
            serviceSettings,
            model.getTaskSettings(),
            model.getConfigurations().getChunkingSettings()
        );
    }

    private final String inferenceEntityId;
    private final TaskType taskType;
    private final String service;
    private final ServiceSettings serviceSettings;
    private final TaskSettings taskSettings;
    private final ChunkingSettings chunkingSettings;

    /**
     * Allows no task settings to be defined. This will default to the {@link EmptyTaskSettings} object.
     */
    public ModelConfigurations(String inferenceEntityId, TaskType taskType, String service, ServiceSettings serviceSettings) {
        this(inferenceEntityId, taskType, service, serviceSettings, EmptyTaskSettings.INSTANCE);
    }

    public ModelConfigurations(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        ServiceSettings serviceSettings,
        ChunkingSettings chunkingSettings
    ) {
        this(inferenceEntityId, taskType, service, serviceSettings, EmptyTaskSettings.INSTANCE, chunkingSettings);
    }

    public ModelConfigurations(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        ServiceSettings serviceSettings,
        TaskSettings taskSettings
    ) {
        this.inferenceEntityId = Objects.requireNonNull(inferenceEntityId);
        this.taskType = Objects.requireNonNull(taskType);
        this.service = Objects.requireNonNull(service);
        this.serviceSettings = Objects.requireNonNull(serviceSettings);
        this.taskSettings = Objects.requireNonNull(taskSettings);
        this.chunkingSettings = null;
    }

    public ModelConfigurations(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        ServiceSettings serviceSettings,
        TaskSettings taskSettings,
        ChunkingSettings chunkingSettings
    ) {
        this.inferenceEntityId = Objects.requireNonNull(inferenceEntityId);
        this.taskType = Objects.requireNonNull(taskType);
        this.service = Objects.requireNonNull(service);
        this.serviceSettings = Objects.requireNonNull(serviceSettings);
        this.taskSettings = Objects.requireNonNull(taskSettings);
        this.chunkingSettings = chunkingSettings;
    }

    public ModelConfigurations(StreamInput in) throws IOException {
        this.inferenceEntityId = in.readString();
        this.taskType = in.readEnum(TaskType.class);
        this.service = in.readString();
        this.serviceSettings = in.readNamedWriteable(ServiceSettings.class);
        this.taskSettings = in.readNamedWriteable(TaskSettings.class);
        this.chunkingSettings = in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)
            ? in.readOptionalNamedWriteable(ChunkingSettings.class)
            : null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(inferenceEntityId);
        out.writeEnum(taskType);
        out.writeString(service);
        out.writeNamedWriteable(serviceSettings);
        out.writeNamedWriteable(taskSettings);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            out.writeOptionalNamedWriteable(chunkingSettings);
        }
    }

    public String getInferenceEntityId() {
        return inferenceEntityId;
    }

    public TaskType getTaskType() {
        return taskType;
    }

    public String getService() {
        return service;
    }

    public ServiceSettings getServiceSettings() {
        return serviceSettings;
    }

    public TaskSettings getTaskSettings() {
        return taskSettings;
    }

    public ChunkingSettings getChunkingSettings() {
        return chunkingSettings;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (params.paramAsBoolean(USE_ID_FOR_INDEX, false)) {
            builder.field(INDEX_ONLY_ID_FIELD_NAME, inferenceEntityId);
        } else {
            builder.field(INFERENCE_ID_FIELD_NAME, inferenceEntityId);
        }
        builder.field(TaskType.NAME, taskType.toString());
        builder.field(SERVICE, service);
        builder.field(SERVICE_SETTINGS, serviceSettings);
        // Always write task settings to the index even if empty.
        // But do not show empty settings in the response
        if (params.paramAsBoolean(USE_ID_FOR_INDEX, false) || (taskSettings != null && taskSettings.isEmpty() == false)) {
            builder.field(TASK_SETTINGS, taskSettings);
        }
        if (chunkingSettings != null) {
            builder.field(CHUNKING_SETTINGS, chunkingSettings);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public XContentBuilder toFilteredXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (params.paramAsBoolean(USE_ID_FOR_INDEX, false)) {
            builder.field(INDEX_ONLY_ID_FIELD_NAME, inferenceEntityId);
        } else {
            builder.field(INFERENCE_ID_FIELD_NAME, inferenceEntityId);
        }
        builder.field(TaskType.NAME, taskType.toString());
        builder.field(SERVICE, service);
        builder.field(SERVICE_SETTINGS, serviceSettings.getFilteredXContentObject());
        // Always write task settings to the index even if empty.
        // But do not show empty settings in the response
        if (params.paramAsBoolean(USE_ID_FOR_INDEX, false) || (taskSettings != null && taskSettings.isEmpty() == false)) {
            builder.field(TASK_SETTINGS, taskSettings);
        }
        if (chunkingSettings != null) {
            builder.field(CHUNKING_SETTINGS, chunkingSettings);
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
        ModelConfigurations model = (ModelConfigurations) o;
        return Objects.equals(inferenceEntityId, model.inferenceEntityId)
            && taskType == model.taskType
            && Objects.equals(service, model.service)
            && Objects.equals(serviceSettings, model.serviceSettings)
            && Objects.equals(taskSettings, model.taskSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(inferenceEntityId, taskType, service, serviceSettings, taskSettings);
    }
}
