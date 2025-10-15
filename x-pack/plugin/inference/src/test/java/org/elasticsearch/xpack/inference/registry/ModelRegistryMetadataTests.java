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
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

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

    public void testWithAddedModel_ReturnsSameMetadataInstance() {
        var inferenceId = "id";
        var settings = MinimalServiceSettingsTests.randomInstance();

        var models = new HashMap<>(Map.of(inferenceId, settings));
        var metadata = new ModelRegistryMetadata(ImmutableOpenMap.builder(models).build());

        var newMetadata = metadata.withAddedModel(inferenceId, settings);
        assertThat(newMetadata, sameInstance(metadata));
        assertThat(newMetadata, is(new ModelRegistryMetadata(ImmutableOpenMap.builder(models).build())));
    }

    public void testWithAddedModel_ReturnsNewMetadataInstance_ForNewInferenceId() {
        var inferenceId = "id";
        var settings = MinimalServiceSettingsTests.randomInstance();

        var models = new HashMap<>(Map.of(inferenceId, settings));
        var metadata = new ModelRegistryMetadata(ImmutableOpenMap.builder(models).build());

        var newInferenceId = "new_id";
        var newSettings = MinimalServiceSettingsTests.randomInstance();
        var newMetadata = metadata.withAddedModel(newInferenceId, newSettings);
        assertThat(
            newMetadata,
            is(new ModelRegistryMetadata(ImmutableOpenMap.builder(Map.of(inferenceId, settings, newInferenceId, newSettings)).build()))
        );
    }

    public void testWithAddedModel_ReturnsNewMetadataInstance_ForNewInferenceId_WithTombstoneRemoved() {
        var inferenceId = "id";
        var newInferenceId = "new_id";
        var settings = MinimalServiceSettingsTests.randomInstance();

        var models = new HashMap<>(Map.of(inferenceId, settings));
        var metadata = new ModelRegistryMetadata(ImmutableOpenMap.builder(models).build(), Set.of(newInferenceId));

        var newSettings = MinimalServiceSettingsTests.randomInstance();
        var newMetadata = metadata.withAddedModel(newInferenceId, newSettings);
        assertThat(
            newMetadata,
            is(
                new ModelRegistryMetadata(
                    ImmutableOpenMap.builder(Map.of(inferenceId, settings, newInferenceId, newSettings)).build(),
                    new HashSet<>()
                )
            )
        );
    }

    public void testWithAddedModels_ReturnsSameMetadataInstance() {
        var inferenceId = "id";
        var settings = MinimalServiceSettingsTests.randomInstance();

        var models = new HashMap<>(Map.of(inferenceId, settings));
        var metadata = new ModelRegistryMetadata(ImmutableOpenMap.builder(models).build());

        var newMetadata = metadata.withAddedModels(
            List.of(new ModelRegistry.ModelAndSettings(inferenceId, settings), new ModelRegistry.ModelAndSettings(inferenceId, settings))
        );
        assertThat(newMetadata, sameInstance(metadata));
        assertThat(newMetadata, is(new ModelRegistryMetadata(ImmutableOpenMap.builder(models).build())));
    }

    public void testWithAddedModels_ReturnsSameMetadataInstance_MultipleEntriesInMap() {
        var inferenceId = "id";
        var inferenceId2 = "id2";
        var settings = MinimalServiceSettingsTests.randomInstance();

        var models = new HashMap<>(Map.of(inferenceId, settings, inferenceId2, settings));
        var metadata = new ModelRegistryMetadata(ImmutableOpenMap.builder(models).build());

        var newMetadata = metadata.withAddedModels(
            List.of(
                new ModelRegistry.ModelAndSettings(inferenceId, settings),
                new ModelRegistry.ModelAndSettings(inferenceId, settings),
                new ModelRegistry.ModelAndSettings(inferenceId2, settings)
            )
        );
        assertThat(newMetadata, sameInstance(metadata));
        assertThat(newMetadata, is(new ModelRegistryMetadata(ImmutableOpenMap.builder(models).build())));
    }

    public void testWithAddedModels_ReturnsNewMetadataInstance_ForNewInferenceId() {
        var inferenceId = "id";
        var settings = MinimalServiceSettingsTests.randomInstance();

        var models = new HashMap<>(Map.of(inferenceId, settings));
        var metadata = new ModelRegistryMetadata(ImmutableOpenMap.builder(models).build());

        var inferenceId2 = "new_id";
        var settings2 = MinimalServiceSettingsTests.randomInstance();
        var inferenceId3 = "new_id2";
        var settings3 = MinimalServiceSettingsTests.randomInstance();
        var newMetadata = metadata.withAddedModels(
            List.of(
                new ModelRegistry.ModelAndSettings(inferenceId2, settings2),
                // This should be ignored since it's a duplicate
                new ModelRegistry.ModelAndSettings(inferenceId2, settings2),
                new ModelRegistry.ModelAndSettings(inferenceId3, settings3)
            )
        );
        assertThat(
            newMetadata,
            is(
                new ModelRegistryMetadata(
                    ImmutableOpenMap.builder(Map.of(inferenceId, settings, inferenceId2, settings2, inferenceId3, settings3)).build()
                )
            )
        );
    }

    public void testWithAddedModels_ReturnsNewMetadataInstance_ForNewInferenceId_WithTombstoneRemoved() {
        var inferenceId = "id";
        var newInferenceId = "new_id";
        var newInferenceId2 = "new_id2";
        var settings = MinimalServiceSettingsTests.randomInstance();

        var models = new HashMap<>(Map.of(inferenceId, settings));
        var metadata = new ModelRegistryMetadata(ImmutableOpenMap.builder(models).build(), Set.of(newInferenceId));

        var newSettings = MinimalServiceSettingsTests.randomInstance();
        var newMetadata = metadata.withAddedModels(
            List.of(
                // This will cause the new settings to be used for inferenceId
                new ModelRegistry.ModelAndSettings(inferenceId, newSettings),
                new ModelRegistry.ModelAndSettings(newInferenceId, newSettings),
                new ModelRegistry.ModelAndSettings(newInferenceId2, newSettings)
            )
        );
        assertThat(
            newMetadata,
            is(
                new ModelRegistryMetadata(
                    ImmutableOpenMap.builder(Map.of(inferenceId, newSettings, newInferenceId, newSettings, newInferenceId2, newSettings))
                        .build(),
                    new HashSet<>()
                )
            )
        );
    }
}
