/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
