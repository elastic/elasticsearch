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

public class GetSamplingConfigurationsActionTests extends AbstractWireSerializingTestCase<GetSamplingConfigurationsAction.Response> {

    @Override
    protected Writeable.Reader<GetSamplingConfigurationsAction.Response> instanceReader() {
        return GetSamplingConfigurationsAction.Response::new;
    }

    @Override
    protected GetSamplingConfigurationsAction.Response createTestInstance() {
        return createRandomResponse();
    }

    @Override
    protected GetSamplingConfigurationsAction.Response mutateInstance(GetSamplingConfigurationsAction.Response instance) {
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

        return new GetSamplingConfigurationsAction.Response(mutatedMap);
    }

    private GetSamplingConfigurationsAction.Response createRandomResponse() {
        Map<String, SamplingConfiguration> indexToConfigMap = new HashMap<>();

        int numEntries = randomIntBetween(0, 5);
        for (int i = 0; i < numEntries; i++) {
            String indexName = randomAlphaOfLengthBetween(1, 20);
            SamplingConfiguration config = createRandomSamplingConfiguration();
            indexToConfigMap.put(indexName, config);
        }

        return new GetSamplingConfigurationsAction.Response(indexToConfigMap);
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
        assertThat(GetSamplingConfigurationsAction.NAME, equalTo("indices:admin/sampling/config/getAll"));
        assertThat(GetSamplingConfigurationsAction.INSTANCE.name(), equalTo(GetSamplingConfigurationsAction.NAME));
    }

    public void testActionInstance() {
        assertThat(GetSamplingConfigurationsAction.INSTANCE, notNullValue());
        assertThat(GetSamplingConfigurationsAction.INSTANCE, sameInstance(GetSamplingConfigurationsAction.INSTANCE));
    }
}
