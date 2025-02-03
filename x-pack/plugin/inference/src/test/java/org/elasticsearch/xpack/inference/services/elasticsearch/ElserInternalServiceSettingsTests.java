/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.HashSet;

import static org.elasticsearch.xpack.inference.services.elasticsearch.ElserModelsTests.randomElserModel;

public class ElserInternalServiceSettingsTests extends AbstractWireSerializingTestCase<ElserInternalServiceSettings> {

    public static ElserInternalServiceSettings createRandom() {
        return new ElserInternalServiceSettings(ElasticsearchInternalServiceSettingsTests.validInstance(randomElserModel()));
    }

    public void testBwcWrite() throws IOException {
        {
            var settings = new ElserInternalServiceSettings(new ElasticsearchInternalServiceSettings(1, 1, ".elser_model_1", null, null));
            var copy = copyInstance(settings, TransportVersions.V_8_12_0);
            assertEquals(settings, copy);
        }
        {
            var settings = new ElserInternalServiceSettings(new ElasticsearchInternalServiceSettings(1, 1, ".elser_model_1", null, null));
            var copy = copyInstance(settings, TransportVersions.V_8_11_X);
            assertEquals(settings, copy);
        }
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
}
