/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings;
import org.elasticsearch.xpack.inference.services.elser.ElserInternalServiceSettings;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class ElasticsearchInternalServiceSettingsTests extends AbstractWireSerializingTestCase<ElasticsearchInternalServiceSettings> {

    public static ElasticsearchInternalServiceSettings validInstance(String modelId) {
        boolean useAdaptive = randomBoolean();
        if (useAdaptive) {
            var adaptive = new AdaptiveAllocationsSettings(true, 1, randomIntBetween(2, 8));
            return new ElasticsearchInternalServiceSettings(randomBoolean() ? 1 : null, randomIntBetween(1, 16), modelId, adaptive);
        } else {
            return new ElasticsearchInternalServiceSettings(randomIntBetween(1, 10), randomIntBetween(1, 16), modelId, null);
        }
    }

    @Override
    protected Writeable.Reader<ElasticsearchInternalServiceSettings> instanceReader() {
        return ElasticsearchInternalServiceSettings::new;
    }

    @Override
    protected ElasticsearchInternalServiceSettings createTestInstance() {
        return validInstance("my-model");
    }

    @Override
    protected ElasticsearchInternalServiceSettings mutateInstance(ElasticsearchInternalServiceSettings instance) throws IOException {
        return switch (randomIntBetween(0, 2)) {
            case 0 -> new ElserInternalServiceSettings(
                new ElasticsearchInternalServiceSettings(
                    instance.getNumAllocations() == null ? 1 : instance.getNumAllocations() + 1,
                    instance.getNumThreads(),
                    instance.modelId(),
                    instance.getAdaptiveAllocationsSettings()
                )
            );
            case 1 -> new ElserInternalServiceSettings(
                new ElasticsearchInternalServiceSettings(
                    instance.getNumAllocations(),
                    instance.getNumThreads() + 1,
                    instance.modelId(),
                    instance.getAdaptiveAllocationsSettings()
                )
            );
            case 2 -> new ElserInternalServiceSettings(
                new ElasticsearchInternalServiceSettings(
                    instance.getNumAllocations(),
                    instance.getNumThreads(),
                    instance.modelId() + "-bar",
                    instance.getAdaptiveAllocationsSettings()
                )
            );
            default -> throw new IllegalStateException();
        };
    }

    public void testFromRequestMap_NoDefaultModel() {
        var serviceSettingsBuilder = ElasticsearchInternalServiceSettings.fromRequestMap(
            new HashMap<>(
                Map.of(ElasticsearchInternalServiceSettings.NUM_ALLOCATIONS, 1, ElasticsearchInternalServiceSettings.NUM_THREADS, 4)
            )
        );
        assertNull(serviceSettingsBuilder.getModelId());
    }

    public void testFromMap() {
        var serviceSettings = ElasticsearchInternalServiceSettings.fromRequestMap(
            new HashMap<>(
                Map.of(
                    ElasticsearchInternalServiceSettings.NUM_ALLOCATIONS,
                    1,
                    ElasticsearchInternalServiceSettings.NUM_THREADS,
                    4,
                    ElasticsearchInternalServiceSettings.MODEL_ID,
                    ".elser_model_1"
                )
            )
        ).build();
        assertEquals(new ElasticsearchInternalServiceSettings(1, 4, ".elser_model_1", null), serviceSettings);
    }

    public void testFromMapMissingOptions() {
        var e = expectThrows(
            ValidationException.class,
            () -> ElasticsearchInternalServiceSettings.fromRequestMap(
                new HashMap<>(Map.of(ElasticsearchInternalServiceSettings.NUM_ALLOCATIONS, 1))
            )
        );

        assertThat(e.getMessage(), containsString("[service_settings] does not contain the required setting [num_threads]"));

        e = expectThrows(
            ValidationException.class,
            () -> ElasticsearchInternalServiceSettings.fromRequestMap(
                new HashMap<>(Map.of(ElasticsearchInternalServiceSettings.NUM_THREADS, 1))
            )
        );

        assertThat(
            e.getMessage(),
            containsString("[service_settings] does not contain one of the required settings [num_allocations, adaptive_allocations]")
        );
    }

    public void testFromMapInvalidSettings() {
        var settingsMap = new HashMap<String, Object>(
            Map.of(ElasticsearchInternalServiceSettings.NUM_ALLOCATIONS, 0, ElasticsearchInternalServiceSettings.NUM_THREADS, -1)
        );
        var e = expectThrows(ValidationException.class, () -> ElasticsearchInternalServiceSettings.fromRequestMap(settingsMap));

        assertThat(e.getMessage(), containsString("Invalid value [0]. [num_allocations] must be a positive integer"));
        assertThat(e.getMessage(), containsString("Invalid value [-1]. [num_threads] must be a positive integer"));
    }
}
