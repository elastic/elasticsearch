/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.inference.services.elser.ElserMlNodeServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.elser.ElserMlNodeTaskSettings;

public class ModelTests extends AbstractWireSerializingTestCase<Model> {

    public static Model createRandomInstance() {
        // TODO randomise task types and settings
        var taskType = TaskType.SPARSE_EMBEDDING;
        return new Model(randomAlphaOfLength(6), taskType, randomAlphaOfLength(6), randomServiceSettings(), randomTaskSettings(taskType));
    }

    public static Model mutateTestInstance(Model instance) {
        switch (randomIntBetween(0, 2)) {
            case 0 -> new Model(
                instance.getModelId() + "foo",
                instance.getTaskType(),
                instance.getService(),
                instance.getServiceSettings(),
                instance.getTaskSettings()
            );
            case 1 -> new Model(
                instance.getModelId(),
                TaskType.values()[(instance.getTaskType().ordinal() + 1) % TaskType.values().length],
                instance.getService(),
                instance.getServiceSettings(),
                instance.getTaskSettings()
            );
            case 2 -> new Model(
                instance.getModelId(),
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
        return ElserMlNodeServiceSettingsTests.createRandom();
    }

    private static TaskSettings randomTaskSettings(TaskType taskType) {
        return ElserMlNodeTaskSettings.DEFAULT; // only 1 implementation
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(InferenceNamedWriteablesProvider.getNamedWriteables());
    }

    @Override
    protected Writeable.Reader<Model> instanceReader() {
        return Model::new;
    }

    @Override
    protected Model createTestInstance() {
        return createRandomInstance();
    }

    @Override
    protected Model mutateInstance(Model instance) {
        return mutateTestInstance(instance);
    }
}
