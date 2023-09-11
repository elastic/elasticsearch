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
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class ElserMlNodeServiceSettingsTests extends AbstractWireSerializingTestCase<ElserMlNodeServiceSettings> {

    public static ElserMlNodeServiceSettings createRandom() {
        return new ElserMlNodeServiceSettings(randomIntBetween(1, 4), randomIntBetween(1, 2));
    }

    public void testFromMap() {
        var serviceSettings = ElserMlNodeServiceSettings.fromMap(
            new HashMap<>(Map.of(ElserMlNodeServiceSettings.NUM_ALLOCATIONS, 1, ElserMlNodeServiceSettings.NUM_THREADS, 4))
        );
        assertEquals(new ElserMlNodeServiceSettings(1, 4), serviceSettings);
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
        return switch (randomIntBetween(0, 1)) {
            case 0 -> new ElserMlNodeServiceSettings(instance.getNumAllocations() + 1, instance.getNumThreads());
            case 1 -> new ElserMlNodeServiceSettings(instance.getNumAllocations(), instance.getNumThreads() + 1);
            default -> throw new IllegalStateException();
        };
    }
}
