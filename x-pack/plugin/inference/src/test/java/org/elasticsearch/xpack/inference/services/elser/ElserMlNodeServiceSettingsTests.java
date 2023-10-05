/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elser;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class ElserMlNodeServiceSettingsTests extends AbstractWireSerializingTestCase<ElserMlNodeServiceSettings> {

    public static ElserMlNodeServiceSettings createRandom() {
        return new ElserMlNodeServiceSettings(
            randomIntBetween(1, 4),
            randomIntBetween(1, 2),
            randomFrom(ElserMlNodeServiceSettings.VALID_ELSER_MODELS)
        );
    }

    public void testFromMap_DefaultModelVersion() {
        var serviceSettings = ElserMlNodeServiceSettings.fromMap(
            new HashMap<>(Map.of(ElserMlNodeServiceSettings.NUM_ALLOCATIONS, 1, ElserMlNodeServiceSettings.NUM_THREADS, 4))
        );
        assertEquals(new ElserMlNodeServiceSettings(1, 4, ".elser_model_2"), serviceSettings);
    }

    public void testFromMap() {
        var serviceSettings = ElserMlNodeServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ElserMlNodeServiceSettings.NUM_ALLOCATIONS,
                    1,
                    ElserMlNodeServiceSettings.NUM_THREADS,
                    4,
                    "model_version",
                    ".elser_model_1"
                )
            )
        );
        assertEquals(new ElserMlNodeServiceSettings(1, 4, ".elser_model_1"), serviceSettings);
    }

    public void testFromMapInvalidVersion() {
        var e = expectThrows(
            ValidationException.class,
            () -> ElserMlNodeServiceSettings.fromMap(
                new HashMap<>(
                    Map.of(
                        ElserMlNodeServiceSettings.NUM_ALLOCATIONS,
                        1,
                        ElserMlNodeServiceSettings.NUM_THREADS,
                        4,
                        "model_version",
                        ".elser_model_27"
                    )
                )
            )
        );
        assertThat(e.getMessage(), containsString("faeafa"));
    }

    public void testFromMapMissingOptions() {
        var e = expectThrows(
            ValidationException.class,
            () -> ElserMlNodeServiceSettings.fromMap(new HashMap<>(Map.of(ElserMlNodeServiceSettings.NUM_ALLOCATIONS, 1)))
        );

        assertThat(e.getMessage(), containsString("[service_settings] does not contain the required setting [num_threads]"));

        e = expectThrows(
            ValidationException.class,
            () -> ElserMlNodeServiceSettings.fromMap(new HashMap<>(Map.of(ElserMlNodeServiceSettings.NUM_THREADS, 1)))
        );

        assertThat(e.getMessage(), containsString("[service_settings] does not contain the required setting [num_allocations]"));
    }

    public void testFromMapInvalidSettings() {
        var settingsMap = new HashMap<String, Object>(
            Map.of(ElserMlNodeServiceSettings.NUM_ALLOCATIONS, 0, ElserMlNodeServiceSettings.NUM_THREADS, -1)
        );
        var e = expectThrows(ValidationException.class, () -> ElserMlNodeServiceSettings.fromMap(settingsMap));

        assertThat(e.getMessage(), containsString("Invalid value [0]. [num_allocations] must be a positive integer"));
        assertThat(e.getMessage(), containsString("Invalid value [-1]. [num_threads] must be a positive integer"));
    }

    @Override
    protected Writeable.Reader<ElserMlNodeServiceSettings> instanceReader() {
        return ElserMlNodeServiceSettings::new;
    }

    @Override
    protected ElserMlNodeServiceSettings createTestInstance() {
        return createRandom();
    }

    @Override
    protected ElserMlNodeServiceSettings mutateInstance(ElserMlNodeServiceSettings instance) {
        return switch (randomIntBetween(0, 2)) {
            case 0 -> new ElserMlNodeServiceSettings(
                instance.getNumAllocations() + 1,
                instance.getNumThreads(),
                instance.getModelVariant()
            );
            case 1 -> new ElserMlNodeServiceSettings(
                instance.getNumAllocations(),
                instance.getNumThreads() + 1,
                instance.getModelVariant()
            );
            case 2 -> {
                var versions = new HashSet<>(ElserMlNodeServiceSettings.VALID_ELSER_MODELS);
                versions.remove(instance.getModelVariant());
                yield new ElserMlNodeServiceSettings(instance.getNumAllocations(), instance.getNumThreads(), versions.iterator().next());
            }
            default -> throw new IllegalStateException();
        };
    }
}
