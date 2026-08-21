/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.common.io.stream.Writeable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.elasticsearch.ElserModelsTests.randomElserModel;
import static org.hamcrest.Matchers.is;

public class ElserInternalServiceSettingsTests extends AbstractElasticsearchInternalServiceSettingsTests<ElserInternalServiceSettings> {

    public static ElserInternalServiceSettings createRandom() {
        return new ElserInternalServiceSettings(ElasticsearchInternalServiceSettingsTests.validInstance(randomElserModel()));
    }

    @Override
    protected Writeable.Reader<ElserInternalServiceSettings> instanceReader() {
        return ElserInternalServiceSettings::new;
    }

    @Override
    protected ElserInternalServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected ElserInternalServiceSettings mutateInstance(ElserInternalServiceSettings instance) {
        return switch (randomIntBetween(0, 2)) {
            case 0 -> new ElserInternalServiceSettings(
                new ElasticsearchInternalServiceSettings(
                    instance.getNumAllocations() == null ? 1 : instance.getNumAllocations() + 1,
                    instance.getNumThreads(),
                    instance.modelId(),
                    null,
                    null
                )
            );
            case 1 -> new ElserInternalServiceSettings(
                new ElasticsearchInternalServiceSettings(
                    instance.getNumAllocations(),
                    instance.getNumThreads() + 1,
                    instance.modelId(),
                    null,
                    null
                )
            );
            case 2 -> {
                var versions = new HashSet<>(ElserModels.VALID_ELSER_MODEL_IDS);
                versions.remove(instance.modelId());
                yield new ElserInternalServiceSettings(
                    new ElasticsearchInternalServiceSettings(
                        instance.getNumAllocations(),
                        instance.getNumThreads(),
                        versions.iterator().next(),
                        null,
                        null
                    )
                );
            }
            default -> throw new IllegalStateException();
        };
    }

    /**
     * Mirrors the persisted path taken by {@code ElserInternalModelCreator}: the base settings are parsed leniently from the
     * stored config and wrapped in the ELSER-specific type.
     */
    public void testFromPersistedMap_CreatesElserSettings_IgnoringUnknownFields() {
        var modelId = randomElserModel();
        var map = new HashMap<String, Object>(
            Map.of(
                ElasticsearchInternalServiceSettings.NUM_ALLOCATIONS,
                1,
                ElasticsearchInternalServiceSettings.NUM_THREADS,
                4,
                ElasticsearchInternalServiceSettings.MODEL_ID,
                modelId,
                "unknown_field_from_a_future_version",
                "value"
            )
        );

        var serviceSettings = new ElserInternalServiceSettings(ElasticsearchInternalServiceSettings.fromPersistedMap(map));

        assertThat(
            serviceSettings,
            is(new ElserInternalServiceSettings(new ElasticsearchInternalServiceSettings(1, 4, modelId, null, null)))
        );
    }

    /**
     * Mirrors the request path taken by {@code elserCase} in {@code ElasticsearchInternalService}: the base settings builder is
     * parsed strictly from the request and the ELSER default model id is applied before wrapping.
     */
    public void testFromRequestMap_BuilderWrapsIntoElserSettings_AfterModelDefaulting() {
        var map = new HashMap<String, Object>(
            Map.of(ElasticsearchInternalServiceSettings.NUM_ALLOCATIONS, 1, ElasticsearchInternalServiceSettings.NUM_THREADS, 4)
        );
        var defaultModelId = randomElserModel();

        var builder = ElasticsearchInternalServiceSettings.fromRequestMap(map);
        assertNull(builder.getModelId());
        builder.setModelId(defaultModelId);

        var serviceSettings = new ElserInternalServiceSettings(builder.build());

        assertThat(
            serviceSettings,
            is(new ElserInternalServiceSettings(new ElasticsearchInternalServiceSettings(1, 4, defaultModelId, null, null)))
        );
    }

    @Override
    protected void assertUpdated(ElserInternalServiceSettings original, ElserInternalServiceSettings updated) {
        // Nothing to do as there are no additional properties
    }
}
