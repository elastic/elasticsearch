/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker.model;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.services.InferenceSettingsTestCase;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerSchemasTests;
import org.elasticsearch.xpack.inference.services.sagemaker.schema.SageMakerStoredServiceSchema;

import java.util.Map;

public class SageMakerServiceSettingsTests extends InferenceSettingsTestCase<SageMakerServiceSettings> {

    @Override
    protected Writeable.Reader<SageMakerServiceSettings> instanceReader() {
        return SageMakerServiceSettings::new;
    }

    @Override
    protected SageMakerServiceSettings createTestInstance() {
        return new SageMakerServiceSettings(
            randomString(),
            randomString(),
            randomString(),
            randomOptionalString(),
            randomOptionalString(),
            randomOptionalString(),
            randomBoolean() ? randomIntBetween(1, 1000) : null,
            SageMakerStoredServiceSchema.NO_OP
        );
    }

    @Override
    protected SageMakerServiceSettings fromMutableMap(Map<String, Object> mutableMap) {
        return SageMakerServiceSettings.fromMap(SageMakerSchemasTests.mockSchemas(), randomFrom(TaskType.values()), mutableMap);
    }
}
