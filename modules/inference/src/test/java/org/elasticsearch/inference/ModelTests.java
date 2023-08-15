/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.inference;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.services.elser.ElserServiceSettings;
import org.elasticsearch.inference.services.elser.ElserSparseEmbeddingTaskSettings;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class ModelTests extends AbstractWireSerializingTestCase<Model> {

    public static Model createRandomInstance() {
        var taskType = randomFrom(TaskType.values());
        return new Model(randomAlphaOfLength(6), taskType, randomAlphaOfLength(6), randomServiceSettings(), randomTaskSettings(taskType));
    }

    private static ServiceSettings randomServiceSettings() {
        return new ElserServiceSettings();
    }

    private static TaskSettings randomTaskSettings(TaskType taskType) {
        return new ElserSparseEmbeddingTaskSettings();
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
}
