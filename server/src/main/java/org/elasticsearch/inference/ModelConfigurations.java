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
    public static final String FOR_INDEX = "for_index"; // true if writing to index
    public static final String SERVICE = "service";
    public static final String SERVICE_SETTINGS = "service_settings";
    public static final String TASK_SETTINGS = "task_settings";
    public static final String PARAMETERS = "parameters";
    public static final String CHUNKING_SETTINGS = "chunking_settings";
    public static final String INCLUDE_PARAMETERS = "include_parameters";
    public static final String ENDPOINT_VERSION_FIELD_NAME = "endpoint_version";
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
            model.getConfigurations().getChunkingSettings(),
            model.getConfigurations().getEndpointVersion()
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
            model.getConfigurations().getChunkingSettings(),
            model.getConfigurations().getEndpointVersion()
        );
    }

    private final String inferenceEntityId;
    private final TaskType taskType;
    private final String service;
    private final ServiceSettings serviceSettings;
    private final TaskSettings taskSettings;
    private final ChunkingSettings chunkingSettings;
    private final EndpointVersions endpointVersion;

    /**
     * Allows no task settings to be defined. This will default to the {@link EmptyTaskSettings} object.
     */
    public ModelConfigurations(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        ServiceSettings serviceSettings,
        EndpointVersions endpointVersion
    ) {
        this(inferenceEntityId, taskType, service, serviceSettings, EmptyTaskSettings.INSTANCE, endpointVersion);
    }

    public ModelConfigurations(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        ServiceSettings serviceSettings,
        ChunkingSettings chunkingSettings,
        EndpointVersions endpointVersion
    ) {
        this(inferenceEntityId, taskType, service, serviceSettings, EmptyTaskSettings.INSTANCE, chunkingSettings, endpointVersion);
    }

    public ModelConfigurations(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        ServiceSettings serviceSettings,
        TaskSettings taskSettings,
        EndpointVersions endpointVersion
    ) {
        this.inferenceEntityId = Objects.requireNonNull(inferenceEntityId);
        this.taskType = Objects.requireNonNull(taskType);
        this.service = Objects.requireNonNull(service);
        this.serviceSettings = Objects.requireNonNull(serviceSettings);
        this.taskSettings = Objects.requireNonNull(taskSettings);
        this.endpointVersion = endpointVersion;
        this.chunkingSettings = null;
    }

    public ModelConfigurations(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        ServiceSettings serviceSettings,
        TaskSettings taskSettings,
        ChunkingSettings chunkingSettings,
        EndpointVersions endpointVersion
    ) {
        this.inferenceEntityId = Objects.requireNonNull(inferenceEntityId);
        this.taskType = Objects.requireNonNull(taskType);
        this.service = Objects.requireNonNull(service);
        this.serviceSettings = Objects.requireNonNull(serviceSettings);
        this.taskSettings = Objects.requireNonNull(taskSettings);
        this.chunkingSettings = chunkingSettings;
        this.endpointVersion = endpointVersion;
    }

    public ModelConfigurations(StreamInput in) throws IOException {
        this.inferenceEntityId = in.readString();
        this.taskType = in.readEnum(TaskType.class);
        this.service = in.readString();
        this.serviceSettings = in.readNamedWriteable(ServiceSettings.class);
        this.taskSettings = in.readNamedWriteable(TaskSettings.class);
        this.chunkingSettings = in.getTransportVersion().onOrAfter(TransportVersions.ML_INFERENCE_CHUNKING_SETTINGS)
            ? in.readOptionalNamedWriteable(ChunkingSettings.class)
            : null;
        this.endpointVersion = in.getTransportVersion().onOrAfter(TransportVersions.INFERENCE_API_PARAMATERS_INTRODUCED)
            ? Objects.requireNonNullElse(in.readEnum(EndpointVersions.class), EndpointVersions.FIRST_ENDPOINT_VERSION)
            : EndpointVersions.FIRST_ENDPOINT_VERSION;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(inferenceEntityId);
        out.writeEnum(taskType);
        out.writeString(service);
        out.writeNamedWriteable(serviceSettings);
        out.writeNamedWriteable(taskSettings);
        if (out.getTransportVersion().onOrAfter(TransportVersions.ML_INFERENCE_CHUNKING_SETTINGS)) {
            out.writeOptionalNamedWriteable(chunkingSettings);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.INFERENCE_API_PARAMATERS_INTRODUCED)) {
            out.writeEnum(endpointVersion); // not nullable after 9.0
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

    public EndpointVersions getEndpointVersion() {
        return endpointVersion;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (params.paramAsBoolean(FOR_INDEX, false)) {
            builder.field(INDEX_ONLY_ID_FIELD_NAME, inferenceEntityId);
        } else {
            builder.field(INFERENCE_ID_FIELD_NAME, inferenceEntityId);
        }
        builder.field(TaskType.NAME, taskType.toString());
        builder.field(SERVICE, service);
        builder.field(SERVICE_SETTINGS, serviceSettings);
        builder.field(TASK_SETTINGS, taskSettings);
        if (chunkingSettings != null) {
            builder.field(CHUNKING_SETTINGS, chunkingSettings);
        }
        if (params.paramAsBoolean(INCLUDE_PARAMETERS, true)) { // default true so that REST requests get parameters
            builder.field(PARAMETERS, taskSettings);
        }
        if (params.paramAsBoolean(FOR_INDEX, false)) {
            builder.field(ENDPOINT_VERSION_FIELD_NAME, endpointVersion); // only write endpoint version for index, not for REST
        }
        builder.endObject();
        return builder;
    }

    @Override
    public XContentBuilder toFilteredXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (params.paramAsBoolean(FOR_INDEX, false)) {
            builder.field(INDEX_ONLY_ID_FIELD_NAME, inferenceEntityId);
        } else {
            builder.field(INFERENCE_ID_FIELD_NAME, inferenceEntityId);
        }
        builder.field(TaskType.NAME, taskType.toString());
        builder.field(SERVICE, service);
        builder.field(SERVICE_SETTINGS, serviceSettings.getFilteredXContentObject());
        builder.field(TASK_SETTINGS, taskSettings);
        if (chunkingSettings != null) {
            builder.field(CHUNKING_SETTINGS, chunkingSettings);
        }
        if (params.paramAsBoolean(INCLUDE_PARAMETERS, true)) { // default true so that REST requests get parameters
            builder.field(PARAMETERS, taskSettings);
        }
        if (params.paramAsBoolean(FOR_INDEX, false)) {
            builder.field(ENDPOINT_VERSION_FIELD_NAME, endpointVersion);
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
            && Objects.equals(taskSettings, model.taskSettings)
            && Objects.equals(endpointVersion, model.endpointVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(inferenceEntityId, taskType, service, serviceSettings, taskSettings, endpointVersion);
    }
}
