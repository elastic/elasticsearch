/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker.model;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ModelSecrets;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xpack.inference.common.amazon.AwsSecretSettings;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerStoredServiceSchema;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerStoredTaskSchema;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * This model represents all models in SageMaker. SageMaker maintains a base set of settings and configurations, and this model manages
 * those. Any settings that are required for a specific model are stored in the {@link SageMakerStoredServiceSchema} and
 * {@link SageMakerStoredTaskSchema}.
 * Design:
 * - Region is stored in ServiceSettings and is used to create the SageMaker client.
 * - RateLimiting is based on AWS Service Quota, metered by account and region. The SDK client handles rate limiting internally. In order to
 *   rate limit appropriately, use the same AWS Access ID, Secret Key, and Region for each inference endpoint calling the same AWS account.
 * - Within Elastic, you cannot change the model behind the endpoint, because we require the embedding shape remain consistent between
 *   invocations. So anything "model selection" related must remain consistent through the lifetime of the Elastic endpoint. SageMaker
 *   allows model changes via TargetModel, InferenceComponentName, and TargetContainerHostname, but these will remain static in Elastic
 *   within ServiceSettings.
 * - CustomAttributes, EnableExplanations, InferenceId, SessionId, and TargetVariant are all request-time fields that can be saved.
 * - SageMaker returns 4 headers, which Elastic will forward as-is: x-Amzn-Invoked-Production-Variant, X-Amzn-SageMaker-Custom-Attributes,
 *   X-Amzn-SageMaker-New-Session-Id, X-Amzn-SageMaker-Closed-Session-Id
 */
public class SageMakerModel extends Model {
    private final SageMakerServiceSettings serviceSettings;
    private final SageMakerTaskSettings taskSettings;
    private final AwsSecretSettings awsSecretSettings;

    SageMakerModel(
        ModelConfigurations configurations,
        ModelSecrets secrets,
        SageMakerServiceSettings serviceSettings,
        SageMakerTaskSettings taskSettings,
        AwsSecretSettings awsSecretSettings
    ) {
        super(configurations, secrets);
        this.serviceSettings = serviceSettings;
        this.taskSettings = taskSettings;
        this.awsSecretSettings = awsSecretSettings;
    }

    public Optional<AwsSecretSettings> awsSecretSettings() {
        return Optional.ofNullable(awsSecretSettings);
    }

    public String region() {
        return serviceSettings.region();
    }

    public String endpointName() {
        return serviceSettings.endpointName();
    }

    public String api() {
        return serviceSettings.api();
    }

    public Optional<String> customAttributes() {
        return Optional.ofNullable(taskSettings.customAttributes());
    }

    public Optional<String> enableExplanations() {
        return Optional.ofNullable(taskSettings.enableExplanations());
    }

    public Optional<String> inferenceComponentName() {
        return Optional.ofNullable(serviceSettings.inferenceComponentName());
    }

    public Optional<String> inferenceIdForDataCapture() {
        return Optional.ofNullable(taskSettings.inferenceIdForDataCapture());
    }

    public Optional<String> sessionId() {
        return Optional.ofNullable(taskSettings.sessionId());
    }

    public Optional<String> targetContainerHostname() {
        return Optional.ofNullable(serviceSettings.targetContainerHostname());
    }

    public Optional<String> targetModel() {
        return Optional.ofNullable(serviceSettings.targetModel());
    }

    public Optional<String> targetVariant() {
        return Optional.ofNullable(taskSettings.targetVariant());
    }

    public Optional<Integer> batchSize() {
        return Optional.ofNullable(serviceSettings.batchSize());
    }

    public SageMakerModel override(Map<String, Object> taskSettingsOverride) {
        if (taskSettingsOverride == null || taskSettingsOverride.isEmpty()) {
            return this;
        }

        return new SageMakerModel(
            getConfigurations(),
            getSecrets(),
            serviceSettings,
            taskSettings.updatedTaskSettings(taskSettingsOverride),
            awsSecretSettings
        );
    }

    public static List<NamedWriteableRegistry.Entry> namedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(ServiceSettings.class, SageMakerServiceSettings.NAME, SageMakerServiceSettings::new),
            new NamedWriteableRegistry.Entry(TaskSettings.class, SageMakerTaskSettings.NAME, SageMakerTaskSettings::new)
        );
    }

    public SageMakerStoredServiceSchema apiServiceSettings() {
        return serviceSettings.apiServiceSettings();
    }

    public SageMakerStoredTaskSchema apiTaskSettings() {
        return taskSettings.apiTaskSettings();
    }

    SageMakerServiceSettings serviceSettings() {
        return serviceSettings;
    }

    SageMakerTaskSettings taskSettings() {
        return taskSettings;
    }
}
