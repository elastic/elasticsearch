/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker.model;

import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.inference.SettingsConfiguration;
import org.elasticsearch.xpack.inference.common.amazon.AwsSecretSettings;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerSchemas;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SageMakerConfiguration implements CheckedSupplier<Map<String, SettingsConfiguration>, RuntimeException> {
    private final SageMakerSchemas schemas;

    public SageMakerConfiguration(SageMakerSchemas schemas) {
        this.schemas = schemas;
    }

    @Override
    public Map<String, SettingsConfiguration> get() {
        return Stream.of(
            AwsSecretSettings.configuration(schemas.supportedTaskTypes()),
            SageMakerServiceSettings.configuration(schemas.supportedTaskTypes()),
            SageMakerTaskSettings.configuration(schemas.supportedTaskTypes())
        ).flatMap(Function.identity()).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
