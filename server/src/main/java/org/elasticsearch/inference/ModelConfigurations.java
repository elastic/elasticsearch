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
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class ModelConfigurations implements ToFilteredXContentObject, VersionedNamedWriteable {

    public static final String MODEL_ID = "model_id";
    public static final String SERVICE = "service";
    public static final String SERVICE_SETTINGS = "service_settings";
    public static final String TASK_SETTINGS = "task_settings";
    private static final String NAME = "inference_model";

    public static ModelConfigurations of(Model model, TaskSettings taskSettings) {
        Objects.requireNonNull(model);
        Objects.requireNonNull(taskSettings);

        return new ModelConfigurations(
            model.getConfigurations().getInferenceEntityId(),
            model.getConfigurations().getTaskType(),
            model.getConfigurations().getService(),
            model.getServiceSettings(),
            taskSettings
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
            model.getTaskSettings()
        );
    }

    private final String inferenceEntityId;
    private final TaskType taskType;
    private final String service;
    private final ServiceSettings serviceSettings;
    private final TaskSettings taskSettings;

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
        TaskSettings taskSettings
    ) {
        this.inferenceEntityId = Objects.requireNonNull(inferenceEntityId);
        this.taskType = Objects.requireNonNull(taskType);
        this.service = Objects.requireNonNull(service);
        this.serviceSettings = Objects.requireNonNull(serviceSettings);
        this.taskSettings = Objects.requireNonNull(taskSettings);
    }

    public ModelConfigurations(StreamInput in) throws IOException {
        this.inferenceEntityId = in.readString();
        this.taskType = in.readEnum(TaskType.class);
        this.service = in.readString();
        this.serviceSettings = in.readNamedWriteable(ServiceSettings.class);
        this.taskSettings = in.readNamedWriteable(TaskSettings.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(inferenceEntityId);
        out.writeEnum(taskType);
        out.writeString(service);
        out.writeNamedWriteable(serviceSettings);
        out.writeNamedWriteable(taskSettings);
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

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(MODEL_ID, inferenceEntityId);
        builder.field(TaskType.NAME, taskType.toString());
        builder.field(SERVICE, service);
        builder.field(SERVICE_SETTINGS, serviceSettings);
        builder.field(TASK_SETTINGS, taskSettings);
        builder.endObject();
        return builder;
    }

    @Override
    public XContentBuilder toFilteredXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(MODEL_ID, inferenceEntityId);
        builder.field(TaskType.NAME, taskType.toString());
        builder.field(SERVICE, service);
        builder.field(SERVICE_SETTINGS, serviceSettings.getFilteredXContentObject());
        builder.field(TASK_SETTINGS, taskSettings);
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
