/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.registry;

import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.MinimalServiceSettingsTests;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ModelRegistryMetadataTests extends AbstractChunkedSerializingTestCase<ModelRegistryMetadata> {
    public static ModelRegistryMetadata randomInstance() {
        return randomInstance(randomBoolean(), true);
    }

    public static ModelRegistryMetadata randomInstance(boolean isUpgraded, boolean acceptsEmpty) {
        if (rarely() && acceptsEmpty) {
            return isUpgraded ? ModelRegistryMetadata.EMPTY_UPGRADED : ModelRegistryMetadata.EMPTY_NOT_UPGRADED;
        }
        int size = randomIntBetween(1, 5);

        Map<String, MinimalServiceSettings> models = new HashMap<>();
        for (int i = 0; i < size; i++) {
            models.put(randomAlphaOfLength(10), MinimalServiceSettingsTests.randomInstance());
        }

        if (isUpgraded) {
            return new ModelRegistryMetadata(ImmutableOpenMap.builder(models).build());
        }

        Set<String> deletedIDs = new HashSet<>();
        size = randomIntBetween(0, 3);
        for (int i = 0; i < size; i++) {
            deletedIDs.add(randomAlphaOfLength(10));
        }
        return new ModelRegistryMetadata(ImmutableOpenMap.builder(models).build(), deletedIDs);
    }

    @Override
    protected ModelRegistryMetadata createTestInstance() {
        return randomInstance();
    }

    @Override
    protected ModelRegistryMetadata mutateInstance(ModelRegistryMetadata instance) {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            Collections.singletonList(
                new NamedWriteableRegistry.Entry(ModelRegistryMetadata.class, ModelRegistryMetadata.TYPE, ModelRegistryMetadata::new)
            )
        );
    }

    @Override
    protected ModelRegistryMetadata doParseInstance(XContentParser parser) throws IOException {
        return ModelRegistryMetadata.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<ModelRegistryMetadata> instanceReader() {
        return ModelRegistryMetadata::new;
    }

    public void testUpgrade() {
        var metadata = randomInstance(false, false);
        var metadataWithTombstones = metadata.withRemovedModel(Set.of(randomFrom(metadata.getModelMap().keySet())));

        var indexMetadata = metadata.withAddedModel(randomAlphanumericOfLength(10), MinimalServiceSettingsTests.randomInstance());
        var upgraded = metadataWithTombstones.withUpgradedModels(indexMetadata.getModelMap());

        Map<String, MinimalServiceSettings> expectedModelMap = new HashMap<>(metadataWithTombstones.getModelMap());
        expectedModelMap.putAll(indexMetadata.getModelMap());
        for (var id : metadataWithTombstones.getTombstones()) {
            expectedModelMap.remove(id);
        }

        assertTrue(upgraded.isUpgraded());
        assertThat(upgraded.getModelMap(), equalTo(expectedModelMap));
    }

    public void testAlreadyUpgraded() {
        var metadata = randomInstance(true, true);
        var indexMetadata = randomInstance(true, true);

        var exc = expectThrows(IllegalArgumentException.class, () -> metadata.withUpgradedModels(indexMetadata.getModelMap()));
        assertThat(exc.getMessage(), containsString("upgraded"));
    }
}
