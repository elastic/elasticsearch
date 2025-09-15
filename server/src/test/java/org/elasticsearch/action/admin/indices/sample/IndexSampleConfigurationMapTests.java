/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.admin.indices.sample;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class IndexSampleConfigurationMapTests extends AbstractXContentSerializingTestCase<IndexSampleConfigurationMap> {

    @Override
    protected IndexSampleConfigurationMap doParseInstance(XContentParser parser) throws IOException {
        return IndexSampleConfigurationMap.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<IndexSampleConfigurationMap> instanceReader() {
        return IndexSampleConfigurationMap::new;
    }

    @Override
    protected IndexSampleConfigurationMap createTestInstance() {
        return new IndexSampleConfigurationMap(randomSampleConfigMap());
    }

    @Override
    protected IndexSampleConfigurationMap mutateInstance(IndexSampleConfigurationMap instance) {
        Map<String, SampleConfiguration> map = new HashMap<>(instance.getIndexToSampleConfig());
        if (map.isEmpty() || randomBoolean()) {
            // Add a new entry
            map.put(randomAlphaOfLength(10), createRandomSampleConfig());
        } else {
            // Remove an entry
            map.remove(map.keySet().iterator().next());
        }
        return new IndexSampleConfigurationMap(map);
    }

    private Map<String, SampleConfiguration> randomSampleConfigMap() {
        Map<String, SampleConfiguration> map = new HashMap<>();
        int numConfigs = randomIntBetween(0, 5);
        for (int i = 0; i < numConfigs; i++) {
            map.put(randomAlphaOfLength(10), createRandomSampleConfig());
        }
        return map;
    }

    private static SampleConfiguration createRandomSampleConfig() {
        return new SampleConfiguration(
            randomDoubleBetween(0.0, 1.0, true),
            randomBoolean() ? null : randomIntBetween(1, 1000),
            randomBoolean() ? null : ByteSizeValue.ofGb(randomIntBetween(1, 5)),
            randomBoolean() ? null : new TimeValue(randomIntBetween(1, 30), TimeUnit.DAYS),
            randomBoolean() ? randomAlphaOfLength(10) : null
        );
    }
}
