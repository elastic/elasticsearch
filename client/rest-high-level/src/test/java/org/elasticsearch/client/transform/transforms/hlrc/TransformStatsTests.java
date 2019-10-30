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

package org.elasticsearch.client.transform.transforms.hlrc;

import org.elasticsearch.client.AbstractHlrcXContentTestCase;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpointStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpointingInfo;
import org.elasticsearch.xpack.core.transform.transforms.TransformProgress;
import org.elasticsearch.xpack.core.transform.transforms.TransformStats;
import org.elasticsearch.xpack.core.transform.transforms.NodeAttributes;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

public class TransformStatsTests extends AbstractHlrcXContentTestCase<TransformStats,
    org.elasticsearch.client.transform.transforms.TransformStats> {

    public static NodeAttributes fromHlrc(org.elasticsearch.client.transform.transforms.NodeAttributes attributes) {
        return attributes == null ? null : new NodeAttributes(attributes.getId(),
            attributes.getName(),
            attributes.getEphemeralId(),
            attributes.getTransportAddress(),
            attributes.getAttributes());
    }

    public static TransformStats
        fromHlrc(org.elasticsearch.client.transform.transforms.TransformStats instance) {

        return new TransformStats(instance.getId(),
            TransformStats.State.fromString(instance.getState().value()),
            instance.getReason(),
            fromHlrc(instance.getNode()),
            TransformIndexerStatsTests.fromHlrc(instance.getIndexerStats()),
            TransformCheckpointingInfoTests.fromHlrc(instance.getCheckpointingInfo()));
    }

    @Override
    public org.elasticsearch.client.transform.transforms.TransformStats doHlrcParseInstance(XContentParser parser)
            throws IOException {
        return org.elasticsearch.client.transform.transforms.TransformStats.fromXContent(parser);
    }

    @Override
    public TransformStats convertHlrcToInternal(
        org.elasticsearch.client.transform.transforms.TransformStats instance) {
        return new TransformStats(instance.getId(),
                TransformStats.State.fromString(instance.getState().value()),
                instance.getReason(),
                fromHlrc(instance.getNode()),
                TransformIndexerStatsTests.fromHlrc(instance.getIndexerStats()),
                TransformCheckpointingInfoTests.fromHlrc(instance.getCheckpointingInfo()));
    }

    public static TransformStats randomTransformStats() {
        return new TransformStats(randomAlphaOfLength(10),
            randomFrom(TransformStats.State.values()),
            randomBoolean() ? null : randomAlphaOfLength(100),
            randomBoolean() ? null : randomNodeAttributes(),
            randomStats(),
            TransformCheckpointingInfoTests.randomTransformCheckpointingInfo());
    }

    @Override
    protected TransformStats createTestInstance() {
        return randomTransformStats();
    }

    @Override
    protected TransformStats doParseInstance(XContentParser parser) throws IOException {
        return TransformStats.PARSER.apply(parser, null);
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.contains("position") || field.equals("node.attributes");
    }

    public static TransformProgress randomTransformProgress() {
        Long totalDocs = randomBoolean() ? null : randomNonNegativeLong();
        Long docsRemaining = totalDocs != null ? randomLongBetween(0, totalDocs) : null;
        return new TransformProgress(
            totalDocs,
            docsRemaining,
            totalDocs != null ? totalDocs - docsRemaining : randomNonNegativeLong(),
            randomBoolean() ? null : randomNonNegativeLong());
    }

    public static TransformCheckpointingInfo randomTransformCheckpointingInfo() {
        return new TransformCheckpointingInfo(randomTransformCheckpointStats(),
            randomTransformCheckpointStats(), randomNonNegativeLong(),
            randomBoolean() ? null : Instant.ofEpochMilli(randomNonNegativeLong()));
    }

    public static TransformCheckpointStats randomTransformCheckpointStats() {
        return new TransformCheckpointStats(randomLongBetween(1, 1_000_000),
            TransformIndexerPositionTests.randomTransformIndexerPosition(),
            randomBoolean() ? null : TransformProgressTests.randomTransformProgress(),
            randomLongBetween(1, 1_000_000), randomLongBetween(0, 1_000_000));
    }

    public static NodeAttributes randomNodeAttributes() {
        int numberOfAttributes = randomIntBetween(1, 10);
        Map<String, String> attributes = new HashMap<>(numberOfAttributes);
        for(int i = 0; i < numberOfAttributes; i++) {
            String val = randomAlphaOfLength(10);
            attributes.put("key-"+i, val);
        }
        return new NodeAttributes(randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            attributes);
    }

    public static TransformIndexerStats randomStats() {
        return new TransformIndexerStats(randomLongBetween(10L, 10000L),
            randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L),
            randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L),
            randomLongBetween(0L, 10000L),
            randomBoolean() ? null : randomDouble(),
            randomBoolean() ? null : randomDouble(),
            randomBoolean() ? null : randomDouble());
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected String[] getShuffleFieldsExceptions() {
        return new String[] { "position" };
    }
}
