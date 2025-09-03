/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker.model;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.common.amazon.AwsSecretSettings;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerSchemas;

import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrDefaultEmpty;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.removeFromMapOrThrowIfNull;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.throwIfNotEmptyMap;

public class SageMakerModelBuilder {

    private final SageMakerSchemas schemas;

    public SageMakerModelBuilder(SageMakerSchemas schemas) {
        this.schemas = schemas;
    }

    public SageMakerModel fromRequest(String inferenceEntityId, TaskType taskType, String service, Map<String, Object> requestMap) {
        var validationException = new ValidationException();
        var serviceSettingsMap = removeFromMapOrThrowIfNull(requestMap, ModelConfigurations.SERVICE_SETTINGS);
        var awsSecretSettings = AwsSecretSettings.fromMap(serviceSettingsMap);
        var serviceSettings = SageMakerServiceSettings.fromMap(schemas, taskType, serviceSettingsMap);

        var schema = schemas.schemaFor(taskType, serviceSettings.api());

        var taskSettingsMap = removeFromMapOrDefaultEmpty(requestMap, ModelConfigurations.TASK_SETTINGS);
        var taskSettings = SageMakerTaskSettings.fromMap(
            taskSettingsMap,
            schema.apiTaskSettings(taskSettingsMap, validationException),
            validationException
        );

        validationException.throwIfValidationErrorsExist();
        throwIfNotEmptyMap(serviceSettingsMap, service);
        throwIfNotEmptyMap(taskSettingsMap, service);

        var modelConfigurations = new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings);
        return new SageMakerModel(
            modelConfigurations,
            new ModelSecrets(awsSecretSettings),
            serviceSettings,
            taskSettings,
            awsSecretSettings
        );
    }

    public SageMakerModel fromStorage(
        String inferenceEntityId,
        TaskType taskType,
        String service,
        Map<String, Object> config,
        Map<String, Object> secrets
    ) {
        var validationException = new ValidationException();
        var awsSecretSettings = secrets != null
            ? AwsSecretSettings.fromMap(removeFromMapOrThrowIfNull(secrets, ModelSecrets.SECRET_SETTINGS))
            : null;

        var serviceSettings = SageMakerServiceSettings.fromMap(
            schemas,
            taskType,
            removeFromMapOrThrowIfNull(config, ModelConfigurations.SERVICE_SETTINGS)
        );

        var schema = schemas.schemaFor(taskType, serviceSettings.api());
        var taskSettingsMap = removeFromMapOrThrowIfNull(config, ModelConfigurations.TASK_SETTINGS);

        var taskSettings = SageMakerTaskSettings.fromMap(
            taskSettingsMap,
            schema.apiTaskSettings(taskSettingsMap, validationException),
            validationException
        );

        validationException.throwIfValidationErrorsExist();
        throwIfNotEmptyMap(config, service);
        throwIfNotEmptyMap(taskSettingsMap, service);
        throwIfNotEmptyMap(secrets, service);

        var modelConfigurations = new ModelConfigurations(inferenceEntityId, taskType, service, serviceSettings, taskSettings);
        return new SageMakerModel(
            modelConfigurations,
            new ModelSecrets(awsSecretSettings),
            serviceSettings,
            taskSettings,
            awsSecretSettings
        );
    }

    public SageMakerModel updateModelWithEmbeddingDetails(SageMakerModel model, int embeddingSize) {
        var updatedApiServiceSettings = model.apiServiceSettings().updateModelWithEmbeddingDetails(embeddingSize);

        if (updatedApiServiceSettings == model.apiServiceSettings()) {
            return model;
        }

        var updatedServiceSettings = new SageMakerServiceSettings(
            model.serviceSettings().endpointName(),
            model.serviceSettings().region(),
            model.serviceSettings().api(),
            model.serviceSettings().targetModel(),
            model.serviceSettings().targetContainerHostname(),
            model.serviceSettings().inferenceComponentName(),
            model.serviceSettings().batchSize(),
            updatedApiServiceSettings
        );

        var modelConfigurations = new ModelConfigurations(
            model.getInferenceEntityId(),
            model.getTaskType(),
            model.getConfigurations().getService(),
            updatedServiceSettings,
            model.taskSettings()
        );
        return new SageMakerModel(
            modelConfigurations,
            model.getSecrets(),
            updatedServiceSettings,
            model.taskSettings(),
            model.awsSecretSettings().orElse(null)
        );
    }
}
