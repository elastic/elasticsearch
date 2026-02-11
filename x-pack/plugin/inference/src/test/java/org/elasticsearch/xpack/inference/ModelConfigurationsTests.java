/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.XPackClientPlugin;
import org.elasticsearch.xpack.core.inference.chunking.ChunkingSettingsTests;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.services.elasticsearch.ElserInternalServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.jinaai.rerank.JinaAIRerankTaskSettingsTests;

import java.util.ArrayList;
import java.util.List;

public class ModelConfigurationsTests extends AbstractBWCWireSerializationTestCase<ModelConfigurations> {

    public static ModelConfigurations createRandomInstance() {
        var taskType = randomFrom(TaskType.values());
        return new ModelConfigurations(
            randomAlphaOfLength(6),
            taskType,
            randomAlphaOfLength(6),
            randomServiceSettings(),
            randomTaskSettings(),
            randomBoolean() ? ChunkingSettingsTests.createRandomChunkingSettings() : null
        );
    }

    public static ModelConfigurations mutateTestInstance(ModelConfigurations instance) {
        return switch (randomIntBetween(0, 5)) {
            case 0 -> new ModelConfigurations(
                instance.getInferenceEntityId() + "foo",
                instance.getTaskType(),
                instance.getService(),
                instance.getServiceSettings(),
                instance.getTaskSettings(),
                instance.getChunkingSettings()
            );
            case 1 -> new ModelConfigurations(
                instance.getInferenceEntityId(),
                TaskType.values()[(instance.getTaskType().ordinal() + 1) % TaskType.values().length],
                instance.getService(),
                instance.getServiceSettings(),
                instance.getTaskSettings(),
                instance.getChunkingSettings()
            );
            case 2 -> new ModelConfigurations(
                instance.getInferenceEntityId(),
                instance.getTaskType(),
                instance.getService() + "bar",
                instance.getServiceSettings(),
                instance.getTaskSettings(),
                instance.getChunkingSettings()
            );
            case 3 -> new ModelConfigurations(
                instance.getInferenceEntityId(),
                instance.getTaskType(),
                instance.getService(),
                randomValueOtherThan(instance.getServiceSettings(), ModelConfigurationsTests::randomServiceSettings),
                instance.getTaskSettings(),
                instance.getChunkingSettings()
            );
            case 4 -> new ModelConfigurations(
                instance.getInferenceEntityId(),
                instance.getTaskType(),
                instance.getService(),
                instance.getServiceSettings(),
                randomValueOtherThan(instance.getTaskSettings(), ModelConfigurationsTests::randomTaskSettings),
                instance.getChunkingSettings()
            );
            case 5 -> {
                var chunkingSettings = instance.getChunkingSettings();
                // Mutate chunkingSettings: if null, create a random one; if non-null, toggle to null or create a different one
                if (chunkingSettings == null) {
                    chunkingSettings = ChunkingSettingsTests.createRandomChunkingSettings();
                } else {
                    chunkingSettings = randomBoolean()
                        ? null
                        : randomValueOtherThan(chunkingSettings, ChunkingSettingsTests::createRandomChunkingSettings);
                }
                yield new ModelConfigurations(
                    instance.getInferenceEntityId(),
                    instance.getTaskType(),
                    instance.getService(),
                    instance.getServiceSettings(),
                    instance.getTaskSettings(),
                    chunkingSettings
                );
            }
            default -> throw new IllegalStateException();
        };
    }

    private static ServiceSettings randomServiceSettings() {
        return ElserInternalServiceSettingsTests.createRandom();
    }

    private static TaskSettings randomTaskSettings() {
        return JinaAIRerankTaskSettingsTests.createRandom();
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>(InferenceNamedWriteablesProvider.getNamedWriteables());
        namedWriteables.addAll(XPackClientPlugin.getChunkingSettingsNamedWriteables());

        return new NamedWriteableRegistry(namedWriteables);
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

    @Override
    protected ModelConfigurations mutateInstanceForVersion(ModelConfigurations instance, TransportVersion version) {
        return instance;
    }
}
