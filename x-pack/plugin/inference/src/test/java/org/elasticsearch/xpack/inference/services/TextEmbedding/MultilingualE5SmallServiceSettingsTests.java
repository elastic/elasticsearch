/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.textembedding;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class MultilingualE5SmallServiceSettingsTests extends AbstractWireSerializingTestCase<MultilingualE5SmallMlNodeServiceSettings> {

    public static MultilingualE5SmallMlNodeServiceSettings createRandom() {
        return new MultilingualE5SmallMlNodeServiceSettings(
            randomIntBetween(1, 4),
            randomIntBetween(1, 4),
            randomFrom(MultilingualE5SmallMlNodeServiceSettings.MODEL_VARIANTS)
        );
    }

    public void testFromMap_DefaultModelVersion() {
        var serviceSettingsBuilder = MultilingualE5SmallMlNodeServiceSettings.fromMap(
            new HashMap<>(
                Map.of(MultilingualE5SmallMlNodeServiceSettings.NUM_ALLOCATIONS, 1, MultilingualE5SmallMlNodeServiceSettings.NUM_THREADS, 4)
            )
        );
        assertNull(serviceSettingsBuilder.getModelVariant());
    }

    public void testFromMap() {
        String randomModelVariant = randomFrom(MultilingualE5SmallMlNodeServiceSettings.MODEL_VARIANTS);
        var serviceSettings = MultilingualE5SmallMlNodeServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    MultilingualE5SmallMlNodeServiceSettings.NUM_ALLOCATIONS,
                    1,
                    MultilingualE5SmallMlNodeServiceSettings.NUM_THREADS,
                    4,
                    MultilingualE5SmallMlNodeServiceSettings.MODEL_VERSION,
                    randomModelVariant
                )
            )
        ).build();
        assertEquals(new MultilingualE5SmallMlNodeServiceSettings(1, 4, randomModelVariant), serviceSettings);
    }

    public void testFromMapInvalidVersion() {
        String randomModelVariant = randomAlphaOfLength(10);
        var e = expectThrows(
            ValidationException.class,
            () -> MultilingualE5SmallMlNodeServiceSettings.fromMap(
                new HashMap<>(
                    Map.of(
                        MultilingualE5SmallMlNodeServiceSettings.NUM_ALLOCATIONS,
                        1,
                        MultilingualE5SmallMlNodeServiceSettings.NUM_THREADS,
                        4,
                        "model_version",
                        randomModelVariant
                    )
                )
            )
        );
        assertThat(e.getMessage(), containsString("unknown Multilingual-E5-Small model version [" + randomModelVariant + "]"));
    }

    public void testFromMapMissingOptions() {
        var e = expectThrows(
            ValidationException.class,
            () -> MultilingualE5SmallMlNodeServiceSettings.fromMap(
                new HashMap<>(Map.of(MultilingualE5SmallMlNodeServiceSettings.NUM_ALLOCATIONS, 1))
            )
        );

        assertThat(e.getMessage(), containsString("[service_settings] does not contain the required setting [num_threads]"));

        e = expectThrows(
            ValidationException.class,
            () -> MultilingualE5SmallMlNodeServiceSettings.fromMap(
                new HashMap<>(Map.of(MultilingualE5SmallMlNodeServiceSettings.NUM_THREADS, 1))
            )
        );

        assertThat(e.getMessage(), containsString("[service_settings] does not contain the required setting [num_allocations]"));
    }

    public void testFromMapInvalidSettings() {
        var settingsMap = new HashMap<String, Object>(
            Map.of(MultilingualE5SmallMlNodeServiceSettings.NUM_ALLOCATIONS, 0, MultilingualE5SmallMlNodeServiceSettings.NUM_THREADS, -1)
        );
        var e = expectThrows(ValidationException.class, () -> MultilingualE5SmallMlNodeServiceSettings.fromMap(settingsMap));

        assertThat(e.getMessage(), containsString("Invalid value [0]. [num_allocations] must be a positive integer"));
        assertThat(e.getMessage(), containsString("Invalid value [-1]. [num_threads] must be a positive integer"));
    }

    @Override
    protected Writeable.Reader<MultilingualE5SmallMlNodeServiceSettings> instanceReader() {
        return MultilingualE5SmallMlNodeServiceSettings::new;
    }

    @Override
    protected MultilingualE5SmallMlNodeServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected MultilingualE5SmallMlNodeServiceSettings mutateInstance(MultilingualE5SmallMlNodeServiceSettings instance) {
        return switch (randomIntBetween(0, 2)) {
            case 0 -> new MultilingualE5SmallMlNodeServiceSettings(
                instance.getNumAllocations() + 1,
                instance.getNumThreads(),
                instance.getModelVariant()
            );
            case 1 -> new MultilingualE5SmallMlNodeServiceSettings(
                instance.getNumAllocations(),
                instance.getNumThreads() + 1,
                instance.getModelVariant()
            );
            case 2 -> {
                var versions = new HashSet<>(MultilingualE5SmallMlNodeServiceSettings.MODEL_VARIANTS);
                versions.remove(instance.getModelVariant());
                yield new MultilingualE5SmallMlNodeServiceSettings(
                    instance.getNumAllocations(),
                    instance.getNumThreads(),
                    versions.iterator().next()
                );
            }
            default -> throw new IllegalStateException();
        };
    }

}
