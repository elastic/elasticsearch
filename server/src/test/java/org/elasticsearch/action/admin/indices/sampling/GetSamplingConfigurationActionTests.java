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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

public class GetSamplingConfigurationActionTests extends AbstractWireSerializingTestCase<GetSamplingConfigurationAction.Response> {

    @Override
    protected Writeable.Reader<GetSamplingConfigurationAction.Response> instanceReader() {
        return GetSamplingConfigurationAction.Response::new;
    }

    @Override
    protected GetSamplingConfigurationAction.Response createTestInstance() {
        return createRandomResponse();
    }

    @Override
    protected GetSamplingConfigurationAction.Response mutateInstance(GetSamplingConfigurationAction.Response instance) {
        Map<String, SamplingConfiguration> originalMap = instance.getIndexToSamplingConfigMap();

        // Create a mutated map by either adding, removing, or changing entries
        Map<String, SamplingConfiguration> mutatedMap = new HashMap<>(originalMap);

        if (mutatedMap.isEmpty() || randomBoolean()) {
            // Add a new entry
            mutatedMap.put(randomAlphaOfLength(10), createRandomSamplingConfiguration());
        } else if (randomBoolean()) {
            // Remove an entry
            String keyToRemove = randomFrom(mutatedMap.keySet());
            mutatedMap.remove(keyToRemove);
        } else {
            // Change an existing entry
            String keyToChange = randomFrom(mutatedMap.keySet());
            mutatedMap.put(keyToChange, createRandomSamplingConfiguration());
        }

        return new GetSamplingConfigurationAction.Response(mutatedMap);
    }

    private GetSamplingConfigurationAction.Response createRandomResponse() {
        Map<String, SamplingConfiguration> indexToConfigMap = new HashMap<>();

        int numEntries = randomIntBetween(0, 5);
        for (int i = 0; i < numEntries; i++) {
            String indexName = randomAlphaOfLengthBetween(1, 20);
            SamplingConfiguration config = createRandomSamplingConfiguration();
            indexToConfigMap.put(indexName, config);
        }

        return new GetSamplingConfigurationAction.Response(indexToConfigMap);
    }

    private SamplingConfiguration createRandomSamplingConfiguration() {
        return new SamplingConfiguration(
            randomDoubleBetween(0.0, 1.0, true),
            randomBoolean() ? null : randomIntBetween(1, SamplingConfiguration.MAX_SAMPLES_LIMIT),
            randomBoolean() ? null : ByteSizeValue.ofGb(randomLongBetween(1, SamplingConfiguration.MAX_SIZE_LIMIT_GIGABYTES)),
            randomBoolean() ? null : TimeValue.timeValueDays(randomLongBetween(1, SamplingConfiguration.MAX_TIME_TO_LIVE_DAYS)),
            randomBoolean() ? null : randomAlphaOfLength(10)
        );
    }

    public void testActionName() {
        assertThat(GetSamplingConfigurationAction.NAME, equalTo("indices:admin/sampling/config/get"));
        assertThat(GetSamplingConfigurationAction.INSTANCE.name(), equalTo(GetSamplingConfigurationAction.NAME));
    }

    public void testActionInstance() {
        assertThat(GetSamplingConfigurationAction.INSTANCE, notNullValue());
        assertThat(GetSamplingConfigurationAction.INSTANCE, sameInstance(GetSamplingConfigurationAction.INSTANCE));
    }
}
