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
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class ModelRegistryClusterStateMetadataTests extends AbstractChunkedSerializingTestCase<ModelRegistryClusterStateMetadata> {
    public static ModelRegistryClusterStateMetadata randomInstance() {
        return randomInstance(randomBoolean(), true);
    }

    public static ModelRegistryClusterStateMetadata randomInstance(boolean isUpgraded, boolean acceptsEmpty) {
        if (rarely() && acceptsEmpty) {
            return isUpgraded ? ModelRegistryClusterStateMetadata.EMPTY_UPGRADED : ModelRegistryClusterStateMetadata.EMPTY_NOT_UPGRADED;
        }
        int size = randomIntBetween(1, 5);

        Map<String, MinimalServiceSettings> models = new HashMap<>();
        for (int i = 0; i < size; i++) {
            models.put(randomAlphaOfLength(10), MinimalServiceSettingsTests.randomInstance());
        }

        if (isUpgraded) {
            return new ModelRegistryClusterStateMetadata(ImmutableOpenMap.builder(models).build());
        }

        Set<String> deletedIDs = new HashSet<>();
        size = randomIntBetween(0, 3);
        for (int i = 0; i < size; i++) {
            deletedIDs.add(randomAlphaOfLength(10));
        }
        return new ModelRegistryClusterStateMetadata(ImmutableOpenMap.builder(models).build(), deletedIDs);
    }

    @Override
    protected ModelRegistryClusterStateMetadata createTestInstance() {
        return randomInstance();
    }

    @Override
    protected ModelRegistryClusterStateMetadata mutateInstance(ModelRegistryClusterStateMetadata instance) {
        int choice = randomIntBetween(0, 2);
        switch (choice) {
            case 0: // Mutate modelMap
                var models = new HashMap<>(instance.getModelMap());
                models.put(randomAlphaOfLength(10), MinimalServiceSettingsTests.randomInstance());
                if (instance.isUpgraded()) {
                    return new ModelRegistryClusterStateMetadata(ImmutableOpenMap.builder(models).build());
                } else {
                    return new ModelRegistryClusterStateMetadata(
                        ImmutableOpenMap.builder(models).build(),
                        new HashSet<>(instance.getTombstones())
                    );
                }
            case 1: // Mutate tombstones
                if (instance.getTombstones() == null) {
                    return new ModelRegistryClusterStateMetadata(instance.getModelMap(), Set.of(randomAlphaOfLength(10)));
                } else {
                    var tombstones = new HashSet<>(instance.getTombstones());
                    tombstones.add(randomAlphaOfLength(10));
                    return new ModelRegistryClusterStateMetadata(instance.getModelMap(), tombstones);
                }
            case 2: // Mutate isUpgraded
                if (instance.isUpgraded()) {
                    return new ModelRegistryClusterStateMetadata(instance.getModelMap(), new HashSet<>());
                } else {
                    return new ModelRegistryClusterStateMetadata(instance.getModelMap());
                }
            default:
                throw new IllegalStateException("Unexpected value: " + choice);
        }
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            Collections.singletonList(
                new NamedWriteableRegistry.Entry(
                    ModelRegistryClusterStateMetadata.class,
                    ModelRegistryClusterStateMetadata.TYPE,
                    ModelRegistryClusterStateMetadata::new
                )
            )
        );
    }

    @Override
    protected ModelRegistryClusterStateMetadata doParseInstance(XContentParser parser) throws IOException {
        return ModelRegistryClusterStateMetadata.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<ModelRegistryClusterStateMetadata> instanceReader() {
        return ModelRegistryClusterStateMetadata::new;
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
        var metadata = new ModelRegistryClusterStateMetadata(ImmutableOpenMap.builder(models).build());

        var newMetadata = metadata.withAddedModel(inferenceId, settings);
        assertThat(newMetadata, sameInstance(metadata));
    }

    public void testWithAddedModel_ReturnsNewMetadataInstance_ForNewInferenceId() {
        var inferenceId = "id";
        var settings = MinimalServiceSettingsTests.randomInstance();

        var models = new HashMap<>(Map.of(inferenceId, settings));
        var metadata = new ModelRegistryClusterStateMetadata(ImmutableOpenMap.builder(models).build());

        var newInferenceId = "new_id";
        var newSettings = MinimalServiceSettingsTests.randomInstance();
        var newMetadata = metadata.withAddedModel(newInferenceId, newSettings);
        // ensure metadata hasn't changed
        assertThat(metadata, is(new ModelRegistryClusterStateMetadata(ImmutableOpenMap.builder(models).build())));
        assertThat(newMetadata, not(is(metadata)));
        assertThat(
            newMetadata,
            is(
                new ModelRegistryClusterStateMetadata(
                    ImmutableOpenMap.builder(Map.of(inferenceId, settings, newInferenceId, newSettings)).build()
                )
            )
        );
    }

    public void testWithAddedModel_ReturnsNewMetadataInstance_ForNewInferenceId_WithTombstoneRemoved() {
        var inferenceId = "id";
        var newInferenceId = "new_id";
        var settings = MinimalServiceSettingsTests.randomInstance();

        var models = new HashMap<>(Map.of(inferenceId, settings));
        var metadata = new ModelRegistryClusterStateMetadata(ImmutableOpenMap.builder(models).build(), Set.of(newInferenceId));

        var newSettings = MinimalServiceSettingsTests.randomInstance();
        var newMetadata = metadata.withAddedModel(newInferenceId, newSettings);
        // ensure metadata hasn't changed
        assertThat(metadata, is(new ModelRegistryClusterStateMetadata(ImmutableOpenMap.builder(models).build(), Set.of(newInferenceId))));
        assertThat(newMetadata, not(is(metadata)));
        assertThat(
            newMetadata,
            is(
                new ModelRegistryClusterStateMetadata(
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
        var metadata = new ModelRegistryClusterStateMetadata(ImmutableOpenMap.builder(models).build());

        var newMetadata = metadata.withAddedModels(
            List.of(
                new ModelRegistryMetadataTask.ModelAndSettings(inferenceId, settings),
                new ModelRegistryMetadataTask.ModelAndSettings(inferenceId, settings)
            )
        );
        assertThat(newMetadata, sameInstance(metadata));
    }

    public void testWithAddedModels_ReturnsSameMetadataInstance_MultipleEntriesInMap() {
        var inferenceId = "id";
        var inferenceId2 = "id2";
        var settings = MinimalServiceSettingsTests.randomInstance();

        var models = new HashMap<>(Map.of(inferenceId, settings, inferenceId2, settings));
        var metadata = new ModelRegistryClusterStateMetadata(ImmutableOpenMap.builder(models).build());

        var newMetadata = metadata.withAddedModels(
            List.of(
                new ModelRegistryMetadataTask.ModelAndSettings(inferenceId, settings),
                new ModelRegistryMetadataTask.ModelAndSettings(inferenceId, settings),
                new ModelRegistryMetadataTask.ModelAndSettings(inferenceId2, settings)
            )
        );
        assertThat(newMetadata, sameInstance(metadata));
    }

    public void testWithAddedModels_ReturnsNewMetadataInstance_ForNewInferenceId() {
        var inferenceId = "id";
        var settings = MinimalServiceSettingsTests.randomInstance();

        var models = new HashMap<>(Map.of(inferenceId, settings));
        var metadata = new ModelRegistryClusterStateMetadata(ImmutableOpenMap.builder(models).build());

        var inferenceId2 = "new_id";
        var settings2 = MinimalServiceSettingsTests.randomInstance();
        var inferenceId3 = "new_id2";
        var settings3 = MinimalServiceSettingsTests.randomInstance();
        var newMetadata = metadata.withAddedModels(
            List.of(
                new ModelRegistryMetadataTask.ModelAndSettings(inferenceId2, settings2),
                // This should be ignored since it's a duplicate
                new ModelRegistryMetadataTask.ModelAndSettings(inferenceId2, settings2),
                new ModelRegistryMetadataTask.ModelAndSettings(inferenceId3, settings3)
            )
        );
        // ensure metadata hasn't changed
        assertThat(metadata, is(new ModelRegistryClusterStateMetadata(ImmutableOpenMap.builder(models).build())));
        assertThat(newMetadata, not(is(metadata)));
        assertThat(
            newMetadata,
            is(
                new ModelRegistryClusterStateMetadata(
                    ImmutableOpenMap.builder(Map.of(inferenceId, settings, inferenceId2, settings2, inferenceId3, settings3)).build()
                )
            )
        );
    }

    public void testWithAddedModels_ReturnsNewMetadataInstance_UsesOverridingSettings() {
        var inferenceId = "id";
        var settings = MinimalServiceSettingsTests.randomInstance();

        var models = new HashMap<>(Map.of(inferenceId, settings));
        var metadata = new ModelRegistryClusterStateMetadata(ImmutableOpenMap.builder(models).build());

        var inferenceId2 = "new_id";
        var settings2 = MinimalServiceSettingsTests.randomInstance();
        var settings3 = MinimalServiceSettingsTests.randomInstance();
        var newMetadata = metadata.withAddedModels(
            List.of(
                new ModelRegistryMetadataTask.ModelAndSettings(inferenceId2, settings2),
                // This should be ignored since it's a duplicate inference id
                new ModelRegistryMetadataTask.ModelAndSettings(inferenceId2, settings2),
                // This should replace the existing settings for inferenceId2
                new ModelRegistryMetadataTask.ModelAndSettings(inferenceId2, settings3)
            )
        );
        // ensure metadata hasn't changed
        assertThat(metadata, is(new ModelRegistryClusterStateMetadata(ImmutableOpenMap.builder(models).build())));
        assertThat(newMetadata, not(is(metadata)));
        assertThat(
            newMetadata,
            is(
                new ModelRegistryClusterStateMetadata(
                    ImmutableOpenMap.builder(Map.of(inferenceId, settings, inferenceId2, settings3)).build()
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
        var metadata = new ModelRegistryClusterStateMetadata(ImmutableOpenMap.builder(models).build(), Set.of(newInferenceId));

        var newSettings = MinimalServiceSettingsTests.randomInstance();
        var newMetadata = metadata.withAddedModels(
            List.of(
                // This will cause the new settings to be used for inferenceId
                new ModelRegistryMetadataTask.ModelAndSettings(inferenceId, newSettings),
                new ModelRegistryMetadataTask.ModelAndSettings(newInferenceId, newSettings),
                new ModelRegistryMetadataTask.ModelAndSettings(newInferenceId2, newSettings)
            )
        );
        // ensure metadata hasn't changed
        assertThat(metadata, is(new ModelRegistryClusterStateMetadata(ImmutableOpenMap.builder(models).build(), Set.of(newInferenceId))));
        assertThat(newMetadata, not(is(metadata)));
        assertThat(
            newMetadata,
            is(
                new ModelRegistryClusterStateMetadata(
                    ImmutableOpenMap.builder(Map.of(inferenceId, newSettings, newInferenceId, newSettings, newInferenceId2, newSettings))
                        .build(),
                    new HashSet<>()
                )
            )
        );
    }

    public void testGetServiceInferenceIds_ReturnsCorrectIdsForKnownService() {
        var serviceA = "service_a";
        var endpointId1 = "endpointId1";
        var endpointId2 = "endpointId2";

        var settings1 = MinimalServiceSettings.chatCompletion(serviceA);
        var settings2 = MinimalServiceSettings.sparseEmbedding(serviceA);
        var models = Map.of(endpointId1, settings1, endpointId2, settings2);
        var metadata = new ModelRegistryClusterStateMetadata(ImmutableOpenMap.builder(models).build());

        var serviceEndpoints = metadata.getServiceInferenceIds(serviceA);
        assertThat(serviceEndpoints, is(Set.of(endpointId1, endpointId2)));
    }

    public void testGetServiceInferenceIds_AcceptsNullKeys() {
        var serviceA = "service_a";
        var endpointId1 = "endpointId1";
        var endpointId2 = "endpointId2";
        var nullEndpoint1 = "nullEndpoint1";
        var nullEndpoint2 = "nullEndpoint2";

        var settings1 = MinimalServiceSettings.chatCompletion(serviceA);
        var settings2 = MinimalServiceSettings.sparseEmbedding(serviceA);
        // I'm not sure why minimal service settings would have a null service name, but testing it anyway
        var nullServiceNameSettings1 = MinimalServiceSettings.sparseEmbedding(null);
        var nullServiceNameSettings2 = MinimalServiceSettings.sparseEmbedding(null);
        var models = Map.of(
            endpointId1,
            settings1,
            endpointId2,
            settings2,
            nullEndpoint1,
            nullServiceNameSettings1,
            nullEndpoint2,
            nullServiceNameSettings2
        );
        var metadata = new ModelRegistryClusterStateMetadata(ImmutableOpenMap.builder(models).build());

        var serviceEndpoints = metadata.getServiceInferenceIds(serviceA);
        assertThat(serviceEndpoints, is(Set.of(endpointId1, endpointId2)));
        assertThat(metadata.getServiceInferenceIds(null), is(Set.of(nullEndpoint1, nullEndpoint2)));
    }

    public void testGetServiceInferenceIds_ReturnsEmptySetForUnknownService() {
        var serviceA = "service_a";
        var serviceB = "service_b";
        var endpointId = "endpointId1";

        var settings = MinimalServiceSettings.chatCompletion(serviceA);
        var models = Map.of(endpointId, settings);
        var metadata = new ModelRegistryClusterStateMetadata(ImmutableOpenMap.builder(models).build());

        var serviceEndpoints = metadata.getServiceInferenceIds(serviceB);
        assertThat(serviceEndpoints, is(empty()));
    }

    public void testGetServiceInferenceIds_ReturnsEmptySetForEmptyModelMap() {
        var serviceA = "service_a";
        var metadata = new ModelRegistryClusterStateMetadata(ImmutableOpenMap.of());

        var serviceEndpoints = metadata.getServiceInferenceIds(serviceA);
        assertThat(serviceEndpoints, is(empty()));
    }

    public void testGetServiceInferenceIds_ReturnedSetIsImmutable_WhenAttemptingToModifyIt() {
        var serviceA = "service_a";
        var endpointId = "endpointId1";

        var settings = MinimalServiceSettings.chatCompletion(serviceA);
        var models = Map.of(endpointId, settings);
        var metadata = new ModelRegistryClusterStateMetadata(ImmutableOpenMap.builder(models).build());

        var serviceEndpoints = metadata.getServiceInferenceIds(serviceA);
        expectThrows(UnsupportedOperationException.class, () -> serviceEndpoints.add("newId"));
    }
}
