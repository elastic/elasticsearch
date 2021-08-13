/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.datafeed;

import org.elasticsearch.client.ml.NodeAttributes;
import org.elasticsearch.client.ml.NodeAttributesTests;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

public class DatafeedStatsTests extends AbstractXContentTestCase<DatafeedStats> {

    public static DatafeedStats createRandomInstance() {
        String datafeedId = DatafeedConfigTests.randomValidDatafeedId();
        DatafeedState datafeedState =
            randomFrom(DatafeedState.STARTED, DatafeedState.STARTING, DatafeedState.STOPPED, DatafeedState.STOPPING);
        NodeAttributes nodeAttributes = null;
        if (randomBoolean()) {
            NodeAttributes randomAttributes = NodeAttributesTests.createRandom();
            int numberOfAttributes = randomIntBetween(1, 10);
            Map<String, String> attributes = new HashMap<>(numberOfAttributes);
            for(int i = 0; i < numberOfAttributes; i++) {
                String val = randomAlphaOfLength(10);
                attributes.put("ml.key-"+i, val);
            }
            nodeAttributes = new NodeAttributes(randomAttributes.getId(),
                randomAttributes.getName(),
                randomAttributes.getEphemeralId(),
                randomAttributes.getTransportAddress(),
                attributes);
        }
        String assignmentReason = randomBoolean() ? randomAlphaOfLength(10) : null;
        DatafeedTimingStats timingStats = DatafeedTimingStatsTests.createRandomInstance();
        return new DatafeedStats(datafeedId, datafeedState, nodeAttributes, assignmentReason, timingStats);
    }

    @Override
    protected DatafeedStats createTestInstance() {
        return createRandomInstance();
    }

    @Override
    protected DatafeedStats doParseInstance(XContentParser parser) throws IOException {
        return DatafeedStats.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.equals("node.attributes");
    }
}
