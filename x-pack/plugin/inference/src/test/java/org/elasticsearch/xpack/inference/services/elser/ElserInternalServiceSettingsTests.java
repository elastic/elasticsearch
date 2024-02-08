/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elser;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class ElserInternalServiceSettingsTests extends AbstractWireSerializingTestCase<ElserInternalServiceSettings> {

    public static ElserInternalServiceSettings createRandom() {
        return new ElserInternalServiceSettings(
            randomIntBetween(1, 4),
            randomIntBetween(1, 2),
            randomFrom(ElserInternalService.VALID_ELSER_MODEL_IDS)
        );
    }

    public void testFromMap_DefaultModelVersion() {
        var serviceSettingsBuilder = ElserInternalServiceSettings.fromMap(
            new HashMap<>(Map.of(ElserInternalServiceSettings.NUM_ALLOCATIONS, 1, ElserInternalServiceSettings.NUM_THREADS, 4))
        );
        assertNull(serviceSettingsBuilder.getModelId());
    }

    public void testFromMap() {
        var serviceSettings = ElserInternalServiceSettings.fromMap(
            new HashMap<>(
                Map.of(
                    ElserInternalServiceSettings.NUM_ALLOCATIONS,
                    1,
                    ElserInternalServiceSettings.NUM_THREADS,
                    4,
                    ElserInternalServiceSettings.MODEL_ID,
                    ".elser_model_1"
                )
            )
        ).build();
        assertEquals(new ElserInternalServiceSettings(1, 4, ".elser_model_1"), serviceSettings);
    }

    public void testFromMapInvalidVersion() {
        var e = expectThrows(
            ValidationException.class,
            () -> ElserInternalServiceSettings.fromMap(
                new HashMap<>(
                    Map.of(
                        ElserInternalServiceSettings.NUM_ALLOCATIONS,
                        1,
                        ElserInternalServiceSettings.NUM_THREADS,
                        4,
                        "model_id",
                        ".elser_model_27"
                    )
                )
            )
        );
        assertThat(e.getMessage(), containsString("unknown ELSER model id [.elser_model_27]"));
    }

    public void testFromMapMissingOptions() {
        var e = expectThrows(
            ValidationException.class,
            () -> ElserInternalServiceSettings.fromMap(new HashMap<>(Map.of(ElserInternalServiceSettings.NUM_ALLOCATIONS, 1)))
        );

        assertThat(e.getMessage(), containsString("[service_settings] does not contain the required setting [num_threads]"));

        e = expectThrows(
            ValidationException.class,
            () -> ElserInternalServiceSettings.fromMap(new HashMap<>(Map.of(ElserInternalServiceSettings.NUM_THREADS, 1)))
        );

        assertThat(e.getMessage(), containsString("[service_settings] does not contain the required setting [num_allocations]"));
    }

    public void testBwcWrite() throws IOException {
        {
            var settings = new ElserInternalServiceSettings(1, 1, ".elser_model_1");
            var copy = copyInstance(settings, TransportVersions.V_8_12_0);
            assertEquals(settings, copy);
        }
        {
            var settings = new ElserInternalServiceSettings(1, 1, ".elser_model_1");
            var copy = copyInstance(settings, TransportVersions.V_8_11_X);
            assertEquals(settings, copy);
        }
    }

    public void testFromMapInvalidSettings() {
        var settingsMap = new HashMap<String, Object>(
            Map.of(ElserInternalServiceSettings.NUM_ALLOCATIONS, 0, ElserInternalServiceSettings.NUM_THREADS, -1)
        );
        var e = expectThrows(ValidationException.class, () -> ElserInternalServiceSettings.fromMap(settingsMap));

        assertThat(e.getMessage(), containsString("Invalid value [0]. [num_allocations] must be a positive integer"));
        assertThat(e.getMessage(), containsString("Invalid value [-1]. [num_threads] must be a positive integer"));
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
            case 0 -> new ElserInternalServiceSettings(instance.getNumAllocations() + 1, instance.getNumThreads(), instance.getModelId());
            case 1 -> new ElserInternalServiceSettings(instance.getNumAllocations(), instance.getNumThreads() + 1, instance.getModelId());
            case 2 -> {
                var versions = new HashSet<>(ElserInternalService.VALID_ELSER_MODEL_IDS);
                versions.remove(instance.getModelId());
                yield new ElserInternalServiceSettings(instance.getNumAllocations(), instance.getNumThreads(), versions.iterator().next());
            }
            default -> throw new IllegalStateException();
        };
    }
}
