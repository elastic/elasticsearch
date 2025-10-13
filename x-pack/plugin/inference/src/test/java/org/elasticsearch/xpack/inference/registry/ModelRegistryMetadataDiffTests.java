/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.registry;

import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.MinimalServiceSettingsTests;
import org.elasticsearch.test.SimpleDiffableWireSerializationTestCase;

import java.util.Map;
import java.util.Set;

public class ModelRegistryMetadataDiffTests extends SimpleDiffableWireSerializationTestCase<Metadata.ProjectCustom> {
    @Override
    protected Metadata.ProjectCustom createTestInstance() {
        return ModelRegistryMetadataTests.randomInstance();
    }

    @Override
    protected Writeable.Reader<Metadata.ProjectCustom> instanceReader() {
        return ModelRegistryMetadata::new;
    }

    @Override
    protected Metadata.ProjectCustom makeTestChanges(Metadata.ProjectCustom testInstance) {
        return mutateInstance((ModelRegistryMetadata) testInstance);
    }

    @Override
    protected Writeable.Reader<Diff<Metadata.ProjectCustom>> diffReader() {
        return ModelRegistryMetadata::readDiffFrom;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(ClusterModule.getNamedWriteables());
    }

    @Override
    protected Metadata.ProjectCustom mutateInstance(Metadata.ProjectCustom instance) {
        return mutateInstance((ModelRegistryMetadata) instance);
    }

    private static ModelRegistryMetadata mutateInstance(ModelRegistryMetadata instance) {
        if (instance.isUpgraded() == false && randomBoolean()) {
            return instance.withUpgradedModels(Map.of(randomAlphaOfLength(10), MinimalServiceSettingsTests.randomInstance()));
        }
        if (randomBoolean() || instance.getModelMap().isEmpty()) {
            return instance.withAddedModel(randomAlphaOfLength(10), MinimalServiceSettingsTests.randomInstance());
        } else {
            return instance.withRemovedModel(Set.of(randomFrom(instance.getModelMap().keySet())));
        }
    }
}
