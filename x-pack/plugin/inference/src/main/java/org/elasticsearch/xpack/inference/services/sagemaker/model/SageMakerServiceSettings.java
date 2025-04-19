/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker.model;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerSchemas;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerStoredServiceSchema;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalPositiveInteger;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;

record SageMakerServiceSettings(
    String endpointName,
    String region,
    String api,
    @Nullable String targetModel,
    @Nullable String targetContainerHostname,
    @Nullable String inferenceComponentName,
    @Nullable Integer batchSize,
    SageMakerStoredServiceSchema extraServiceSettings
) implements ServiceSettings {

    static final String NAME = "sage_maker_service_settings";
    private static final String API = "api";
    private static final String ENDPOINT_NAME = "endpoint_name";
    private static final String REGION = "region";
    private static final String TARGET_MODEL = "target_model";
    private static final String TARGET_CONTAINER_HOSTNAME = "target_container_hostname";
    private static final String INFERENCE_COMPONENT_NAME = "inference_component_name";
    private static final String BATCH_SIZE = "batch_size";

    SageMakerServiceSettings {
        Objects.requireNonNull(endpointName);
        Objects.requireNonNull(region);
        Objects.requireNonNull(api);
        Objects.requireNonNull(extraServiceSettings);
    }

    SageMakerServiceSettings(StreamInput in) throws IOException {
        this(
            in.readString(),
            in.readString(),
            in.readString(),
            in.readOptionalString(),
            in.readOptionalString(),
            in.readOptionalString(),
            in.readOptionalInt(),
            in.readNamedWriteable(SageMakerStoredServiceSchema.class)
        );
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.current();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(endpointName());
        out.writeString(region());
        out.writeString(api());
        out.writeOptionalString(targetModel());
        out.writeOptionalString(targetContainerHostname());
        out.writeOptionalString(inferenceComponentName());
        out.writeOptionalInt(batchSize());
        out.writeNamedWriteable(extraServiceSettings);
    }

    @Override
    public ToXContentObject getFilteredXContentObject() {
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.field(ENDPOINT_NAME, endpointName());
        builder.field(REGION, region());
        builder.field(API, api());
        optionalField(TARGET_MODEL, targetModel(), builder);
        optionalField(TARGET_CONTAINER_HOSTNAME, targetContainerHostname(), builder);
        optionalField(INFERENCE_COMPONENT_NAME, inferenceComponentName(), builder);
        optionalField(BATCH_SIZE, batchSize(), builder);
        extraServiceSettings.toXContent(builder, params);

        return builder.endObject();
    }

    private static <T> void optionalField(String name, T value, XContentBuilder builder) throws IOException {
        if (value != null) {
            builder.field(name, value);
        }
    }

    /**
     * For SageMaker, the unique and required identifier is the endpointName. The modelId is only required for multi-model endpoints.
     */
    @Override
    public String modelId() {
        return endpointName();
    }

    static SageMakerServiceSettings fromMap(SageMakerSchemas schemas, TaskType taskType, Map<String, Object> serviceSettingsMap) {
        ValidationException validationException = new ValidationException();

        var endpointName = extractRequiredString(
            serviceSettingsMap,
            ENDPOINT_NAME,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );
        var region = extractRequiredString(serviceSettingsMap, REGION, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var api = extractRequiredString(serviceSettingsMap, API, ModelConfigurations.SERVICE_SETTINGS, validationException);
        var targetModel = extractOptionalString(
            serviceSettingsMap,
            TARGET_MODEL,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );
        var targetContainerHostname = extractOptionalString(
            serviceSettingsMap,
            TARGET_CONTAINER_HOSTNAME,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );
        var inferenceComponentName = extractOptionalString(
            serviceSettingsMap,
            INFERENCE_COMPONENT_NAME,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );
        var batchSize = extractOptionalPositiveInteger(
            serviceSettingsMap,
            BATCH_SIZE,
            ModelConfigurations.SERVICE_SETTINGS,
            validationException
        );

        validationException.throwIfValidationErrorsExist();

        var schema = schemas.schemaFor(taskType, api);
        var extraServiceSettings = schema.extraServiceSettings(serviceSettingsMap, validationException);

        validationException.throwIfValidationErrorsExist();

        return new SageMakerServiceSettings(
            endpointName,
            region,
            api,
            targetModel,
            targetContainerHostname,
            inferenceComponentName,
            batchSize,
            extraServiceSettings
        );
    }
}
