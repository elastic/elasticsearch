/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.sampling;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.HashMap;
import java.util.Map;

public class GetAllSampleConfigurationActionResponseTests extends AbstractWireSerializingTestCase<
    GetAllSampleConfigurationAction.Response> {

    @Override
    protected Writeable.Reader<GetAllSampleConfigurationAction.Response> instanceReader() {
        return GetAllSampleConfigurationAction.Response::new;
    }

    @Override
    protected GetAllSampleConfigurationAction.Response createTestInstance() {
        int numIndices = randomIntBetween(0, 10);
        Map<String, SamplingConfiguration> indexToConfigMap = new HashMap<>();
        for (int i = 0; i < numIndices; i++) {
            String indexName = randomAlphaOfLengthBetween(1, 20);
            SamplingConfiguration config = createRandomSamplingConfiguration();
            indexToConfigMap.put(indexName, config);
        }
        return new GetAllSampleConfigurationAction.Response(indexToConfigMap);
    }

    @Override
    protected GetAllSampleConfigurationAction.Response mutateInstance(GetAllSampleConfigurationAction.Response instance) {
        Map<String, SamplingConfiguration> originalMap = instance.getIndexToSamplingConfigMap();
        Map<String, SamplingConfiguration> mutatedMap = new HashMap<>(originalMap);

        if (mutatedMap.isEmpty()) {
            // If empty, add a new entry
            mutatedMap.put(randomAlphaOfLengthBetween(1, 20), createRandomSamplingConfiguration());
        } else {
            // Otherwise, randomly mutate the map
            return switch (randomIntBetween(0, 2)) {
                case 0 -> {
                    // Add a new index
                    String newIndex = randomValueOtherThanMany(mutatedMap::containsKey, () -> randomAlphaOfLengthBetween(1, 20));
                    mutatedMap.put(newIndex, createRandomSamplingConfiguration());
                    yield new GetAllSampleConfigurationAction.Response(mutatedMap);
                }
                case 1 -> {
                    // Remove an index
                    String keyToRemove = randomFrom(mutatedMap.keySet());
                    mutatedMap.remove(keyToRemove);
                    yield new GetAllSampleConfigurationAction.Response(mutatedMap);
                }
                case 2 -> {
                    // Modify a configuration
                    String keyToModify = randomFrom(mutatedMap.keySet());
                    SamplingConfiguration newConfig = randomValueOtherThan(
                        mutatedMap.get(keyToModify),
                        this::createRandomSamplingConfiguration
                    );
                    mutatedMap.put(keyToModify, newConfig);
                    yield new GetAllSampleConfigurationAction.Response(mutatedMap);
                }
                default -> throw new IllegalStateException("Invalid mutation case");
            };
        }

        return new GetAllSampleConfigurationAction.Response(mutatedMap);
    }

    private SamplingConfiguration createRandomSamplingConfiguration() {
        return new SamplingConfiguration(
            randomDoubleBetween(0.1, 1.0, true),
            randomBoolean() ? randomIntBetween(1, 1000) : null,
            randomBoolean() ? null : ByteSizeValue.ofKb(randomIntBetween(50, 100)),
            randomBoolean() ? TimeValue.timeValueMinutes(randomIntBetween(1, 60)) : null,
            randomBoolean() ? "ctx?.field == 'test'" : null
        );
    }
}
