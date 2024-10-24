/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.inference.ChunkingSettingsFeatureFlag;
import org.elasticsearch.xpack.inference.chunking.ChunkingSettingsTests;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElserInternalServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElserMlNodeTaskSettings;

public class ModelConfigurationsTests extends AbstractWireSerializingTestCase<ModelConfigurations> {

    public static ModelConfigurations createRandomInstance() {
        var taskType = randomFrom(TaskType.values());
        return new ModelConfigurations(
            randomAlphaOfLength(6),
            taskType,
            randomAlphaOfLength(6),
            randomServiceSettings(),
            randomTaskSettings(taskType),
            ChunkingSettingsFeatureFlag.isEnabled() && randomBoolean() ? ChunkingSettingsTests.createRandomChunkingSettings() : null
        );
    }

    public static ModelConfigurations mutateTestInstance(ModelConfigurations instance) {
        switch (randomIntBetween(0, 2)) {
            case 0 -> new ModelConfigurations(
                instance.getInferenceEntityId() + "foo",
                instance.getTaskType(),
                instance.getService(),
                instance.getServiceSettings(),
                instance.getTaskSettings()
            );
            case 1 -> new ModelConfigurations(
                instance.getInferenceEntityId(),
                TaskType.values()[(instance.getTaskType().ordinal() + 1) % TaskType.values().length],
                instance.getService(),
                instance.getServiceSettings(),
                instance.getTaskSettings()
            );
            case 2 -> new ModelConfigurations(
                instance.getInferenceEntityId(),
                instance.getTaskType(),
                instance.getService() + "bar",
                instance.getServiceSettings(),
                instance.getTaskSettings()
            );
            default -> throw new IllegalStateException();
        }
        return null;
    }

    private static ServiceSettings randomServiceSettings() {
        return ElserInternalServiceSettingsTests.createRandom();
    }

    private static TaskSettings randomTaskSettings(TaskType taskType) {
        return ElserMlNodeTaskSettings.DEFAULT; // only 1 implementation
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(InferenceNamedWriteablesProvider.getNamedWriteables());
    }

    @Override
    protected Writeable.Reader<ModelConfigurations> instanceReader() {
        return ModelConfigurations::new;
    }

    @Override
    protected ModelConfigurations createTestInstance() {
        return createRandomInstance();
    }

    @Override
    protected ModelConfigurations mutateInstance(ModelConfigurations instance) {
        return mutateTestInstance(instance);
    }
}
