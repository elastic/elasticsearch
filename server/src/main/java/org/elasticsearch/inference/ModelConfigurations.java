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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.metadata.EndpointMetadata;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.inference.metadata.EndpointMetadata.INFERENCE_ENDPOINT_METADATA_FIELDS_ADDED;

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
    private final EndpointMetadata endpointMetadata;

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
        this(inferenceEntityId, taskType, service, serviceSettings, taskSettings, null);
    }

    public ModelConfigurations(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        ServiceSettings serviceSettings,
        TaskSettings taskSettings,
        @Nullable ChunkingSettings chunkingSettings
    ) {
        this(inferenceEntityId, taskType, service, serviceSettings, taskSettings, chunkingSettings, null);
    }

    public ModelConfigurations(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        ServiceSettings serviceSettings,
        TaskSettings taskSettings,
        ChunkingSettings chunkingSettings,
        @Nullable EndpointMetadata endpointMetadata
    ) {
        this.inferenceEntityId = Objects.requireNonNull(inferenceEntityId);
        this.taskType = Objects.requireNonNull(taskType);
        this.service = Objects.requireNonNull(service);
        this.serviceSettings = Objects.requireNonNull(serviceSettings);
        this.taskSettings = Objects.requireNonNull(taskSettings);
        this.chunkingSettings = chunkingSettings;
        this.endpointMetadata = endpointMetadata;
    }

    public ModelConfigurations(StreamInput in) throws IOException {
        this.inferenceEntityId = in.readString();
        this.taskType = in.readEnum(TaskType.class);
        this.service = in.readString();
        this.serviceSettings = in.readNamedWriteable(ServiceSettings.class);
        this.taskSettings = in.readNamedWriteable(TaskSettings.class);
        this.chunkingSettings = in.readOptionalNamedWriteable(ChunkingSettings.class);

        // I'm leaving endpoint metadata as optional because in many cases it will be null. Defaulting it to the empty instance would
        // also work here but that will cause the object to be serialized when we can get away with a null instead.
        if (in.getTransportVersion().supports(INFERENCE_ENDPOINT_METADATA_FIELDS_ADDED)) {
            this.endpointMetadata = in.readOptionalWriteable(EndpointMetadata::new);
        } else {
            this.endpointMetadata = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(inferenceEntityId);
        out.writeEnum(taskType);
        out.writeString(service);
        out.writeNamedWriteable(serviceSettings);
        out.writeNamedWriteable(taskSettings);
        out.writeOptionalNamedWriteable(chunkingSettings);

        if (out.getTransportVersion().supports(INFERENCE_ENDPOINT_METADATA_FIELDS_ADDED)) {
            out.writeOptionalWriteable(endpointMetadata);
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

    public EndpointMetadata getEndpointMetadata() {
        // Outside of this class, we'll use the empty instance so callers can avoid the null check
        return Objects.requireNonNullElse(endpointMetadata, EndpointMetadata.EMPTY_INSTANCE);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return toXContent(builder, params, true);
    }

    private XContentBuilder toXContent(XContentBuilder builder, Params params, boolean includeFilteredFields) throws IOException {
        builder.startObject();
        if (params.paramAsBoolean(USE_ID_FOR_INDEX, false)) {
            builder.field(INDEX_ONLY_ID_FIELD_NAME, inferenceEntityId);
        } else {
            builder.field(INFERENCE_ID_FIELD_NAME, inferenceEntityId);
        }
        builder.field(TaskType.NAME, taskType.toString());
        builder.field(SERVICE, service);

        if (includeFilteredFields) {
            builder.field(SERVICE_SETTINGS, serviceSettings);
        } else {
            builder.field(SERVICE_SETTINGS, serviceSettings.getFilteredXContentObject());
        }

        // Always write task settings to the index even if empty.
        // But do not show empty settings in the response
        if (params.paramAsBoolean(USE_ID_FOR_INDEX, false) || (taskSettings != null && taskSettings.isEmpty() == false)) {
            builder.field(TASK_SETTINGS, taskSettings);
        }
        if (chunkingSettings != null) {
            builder.field(CHUNKING_SETTINGS, chunkingSettings);
        }

        // If the endpoint metadata is null or empty it means the instance is the default empty state.
        // This will happen for endpoints other than EIS ones. By not serializing the empty metadata,
        // we avoid causing a StrictDynamicMappingException while an upgrade is in progress.
        if (endpointMetadata != null && endpointMetadata.isEmpty() == false) {
            if (includeFilteredFields) {
                builder.field(EndpointMetadata.METADATA_FIELD_NAME, endpointMetadata);
            } else {
                builder.field(
                    EndpointMetadata.METADATA_FIELD_NAME,
                    endpointMetadata,
                    endpointMetadata.getXContentParamsExcludeInternalFields()
                );
            }
        }

        builder.endObject();
        return builder;
    }

    @Override
    public XContentBuilder toFilteredXContent(XContentBuilder builder, Params params) throws IOException {
        return toXContent(builder, params, false);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.minimumCompatible();
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
            && Objects.equals(taskSettings, model.taskSettings)
            && Objects.equals(chunkingSettings, model.chunkingSettings)
            && Objects.equals(endpointMetadata, model.endpointMetadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(inferenceEntityId, taskType, service, serviceSettings, taskSettings, chunkingSettings, endpointMetadata);
    }
}
