/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.sagemaker.schema.elastic;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.inference.services.InferenceSettingsTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SageMakerElasticTaskSettingsTests extends InferenceSettingsTestCase<SageMakerElasticTaskSettings> {
    @Override
    protected SageMakerElasticTaskSettings fromMutableMap(Map<String, Object> mutableMap) {
        return new SageMakerElasticTaskSettings(mutableMap);
    }

    @Override
    protected Writeable.Reader<SageMakerElasticTaskSettings> instanceReader() {
        return SageMakerElasticTaskSettings::new;
    }

    @Override
    protected SageMakerElasticTaskSettings createTestInstance() {
        return randomInstance();
    }

    @Override
    protected SageMakerElasticTaskSettings mutateInstance(SageMakerElasticTaskSettings instance) throws IOException {
        Map<String, Object> passthroughSettings = instance.passthroughSettings();
        if (passthroughSettings == null || passthroughSettings.isEmpty()) {
            return new SageMakerElasticTaskSettings(
                randomMap(1, 3, () -> Tuple.tuple(randomAlphanumericOfLength(4), randomAlphanumericOfLength(4)))
            );
        }
        var mutated = new HashMap<>(passthroughSettings);
        String key = randomFrom(mutated.keySet());
        mutated.put(
            key,
            randomValueOtherThan(String.valueOf(mutated.get(key)), () -> randomAlphanumericOfLength(4))
        );
        return new SageMakerElasticTaskSettings(mutated);
    }

    static SageMakerElasticTaskSettings randomInstance() {
        return randomBoolean()
            ? SageMakerElasticTaskSettings.empty()
            : new SageMakerElasticTaskSettings(
                randomMap(1, 3, () -> Tuple.tuple(randomAlphanumericOfLength(4), randomAlphanumericOfLength(4)))
            );
    }
}
