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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class MultilingualE5SmallInternalServiceSettingsTests extends AbstractWireSerializingTestCase<
    MultilingualE5SmallInternalServiceSettings> {

    public static MultilingualE5SmallInternalServiceSettings createRandom() {
        return new MultilingualE5SmallInternalServiceSettings(
            randomIntBetween(1, 4),
            randomIntBetween(1, 4),
            randomFrom(ElasticsearchInternalService.MULTILINGUAL_E5_SMALL_VALID_IDS)
        );
    }

    public void testFromMap_DefaultModelVersion() {
        var serviceSettingsBuilder = MultilingualE5SmallInternalServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    MultilingualE5SmallInternalServiceSettings.NUM_ALLOCATIONS,
                    1,
                    MultilingualE5SmallInternalServiceSettings.NUM_THREADS,
                    4
                )
            )
        );
        assertNull(serviceSettingsBuilder.getModelId());
    }

    public void testFromMap() {
        String randomModelVariant = randomFrom(ElasticsearchInternalService.MULTILINGUAL_E5_SMALL_VALID_IDS);
        var serviceSettings = MultilingualE5SmallInternalServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    MultilingualE5SmallInternalServiceSettings.NUM_ALLOCATIONS,
                    1,
                    MultilingualE5SmallInternalServiceSettings.NUM_THREADS,
                    4,
                    MultilingualE5SmallInternalServiceSettings.MODEL_ID,
                    randomModelVariant
                )
            )
        ).build();
        assertEquals(new MultilingualE5SmallInternalServiceSettings(1, 4, randomModelVariant), serviceSettings);
    }

    public void testFromMapInvalidVersion() {
        String randomModelVariant = randomAlphaOfLength(10);
        var e = expectThrows(
            ValidationException.class,
            () -> MultilingualE5SmallInternalServiceSettings.fromMap(
                new HashMap<>(
                    Map.of(
                        MultilingualE5SmallInternalServiceSettings.NUM_ALLOCATIONS,
                        1,
                        MultilingualE5SmallInternalServiceSettings.NUM_THREADS,
                        4,
                        "model_id",
                        randomModelVariant
                    )
                )
            )
        );
        assertThat(e.getMessage(), containsString("unknown Multilingual-E5-Small model ID [" + randomModelVariant + "]. Valid IDs are ["));
    }

    public void testFromMapMissingOptions() {
        var e = expectThrows(
            ValidationException.class,
            () -> MultilingualE5SmallInternalServiceSettings.fromMap(
                new HashMap<>(Map.of(MultilingualE5SmallInternalServiceSettings.NUM_ALLOCATIONS, 1))
            )
        );

        assertThat(e.getMessage(), containsString("[service_settings] does not contain the required setting [num_threads]"));

        e = expectThrows(
            ValidationException.class,
            () -> MultilingualE5SmallInternalServiceSettings.fromMap(
                new HashMap<>(Map.of(MultilingualE5SmallInternalServiceSettings.NUM_THREADS, 1))
            )
        );

        assertThat(e.getMessage(), containsString("[service_settings] does not contain the required setting [num_allocations]"));
    }

    public void testFromMapInvalidSettings() {
        var settingsMap = new HashMap<String, Object>(
            Map.of(
                MultilingualE5SmallInternalServiceSettings.NUM_ALLOCATIONS,
                0,
                MultilingualE5SmallInternalServiceSettings.NUM_THREADS,
                -1
            )
        );
        var e = expectThrows(ValidationException.class, () -> MultilingualE5SmallInternalServiceSettings.fromMap(settingsMap));

        assertThat(e.getMessage(), containsString("Invalid value [0]. [num_allocations] must be a positive integer"));
        assertThat(e.getMessage(), containsString("Invalid value [-1]. [num_threads] must be a positive integer"));
    }

    @Override
    protected Writeable.Reader<MultilingualE5SmallInternalServiceSettings> instanceReader() {
        return MultilingualE5SmallInternalServiceSettings::new;
    }

    @Override
    protected MultilingualE5SmallInternalServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected MultilingualE5SmallInternalServiceSettings mutateInstance(MultilingualE5SmallInternalServiceSettings instance) {
        return switch (randomIntBetween(0, 2)) {
            case 0 -> new MultilingualE5SmallInternalServiceSettings(
                instance.getNumAllocations() + 1,
                instance.getNumThreads(),
                instance.getModelId()
            );
            case 1 -> new MultilingualE5SmallInternalServiceSettings(
                instance.getNumAllocations(),
                instance.getNumThreads() + 1,
                instance.getModelId()
            );
            case 2 -> {
                var versions = new HashSet<>(ElasticsearchInternalService.MULTILINGUAL_E5_SMALL_VALID_IDS);
                versions.remove(instance.getModelId());
                yield new MultilingualE5SmallInternalServiceSettings(
                    instance.getNumAllocations(),
                    instance.getNumThreads(),
                    versions.iterator().next()
                );
            }
            default -> throw new IllegalStateException();
        };
    }

}
