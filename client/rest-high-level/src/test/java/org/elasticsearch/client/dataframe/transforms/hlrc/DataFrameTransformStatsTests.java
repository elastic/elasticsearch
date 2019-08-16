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

package org.elasticsearch.client.dataframe.transforms.hlrc;

import org.elasticsearch.client.AbstractHlrcXContentTestCase;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameIndexerTransformStats;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformCheckpointStats;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformCheckpointingInfo;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformProgress;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformStats;
import org.elasticsearch.xpack.core.dataframe.transforms.NodeAttributes;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

public class DataFrameTransformStatsTests extends AbstractHlrcXContentTestCase<DataFrameTransformStats,
    org.elasticsearch.client.dataframe.transforms.DataFrameTransformStats> {

    public static NodeAttributes fromHlrc(org.elasticsearch.client.dataframe.transforms.NodeAttributes attributes) {
        return attributes == null ? null : new NodeAttributes(attributes.getId(),
            attributes.getName(),
            attributes.getEphemeralId(),
            attributes.getTransportAddress(),
            attributes.getAttributes());
    }

    public static DataFrameTransformStats
        fromHlrc(org.elasticsearch.client.dataframe.transforms.DataFrameTransformStats instance) {

        return new DataFrameTransformStats(instance.getId(),
            DataFrameTransformStats.State.fromString(instance.getState().value()),
            instance.getReason(),
            fromHlrc(instance.getNode()),
            DataFrameIndexerTransformStatsTests.fromHlrc(instance.getIndexerStats()),
            DataFrameTransformCheckpointingInfoTests.fromHlrc(instance.getCheckpointingInfo()));
    }

    @Override
    public org.elasticsearch.client.dataframe.transforms.DataFrameTransformStats doHlrcParseInstance(XContentParser parser)
            throws IOException {
        return org.elasticsearch.client.dataframe.transforms.DataFrameTransformStats.fromXContent(parser);
    }

    @Override
    public DataFrameTransformStats convertHlrcToInternal(
        org.elasticsearch.client.dataframe.transforms.DataFrameTransformStats instance) {
        return new DataFrameTransformStats(instance.getId(),
                DataFrameTransformStats.State.fromString(instance.getState().value()),
                instance.getReason(),
                fromHlrc(instance.getNode()),
                DataFrameIndexerTransformStatsTests.fromHlrc(instance.getIndexerStats()),
                DataFrameTransformCheckpointingInfoTests.fromHlrc(instance.getCheckpointingInfo()));
    }

    public static DataFrameTransformStats randomDataFrameTransformStats() {
        return new DataFrameTransformStats(randomAlphaOfLength(10),
            randomFrom(DataFrameTransformStats.State.values()),
            randomBoolean() ? null : randomAlphaOfLength(100),
            randomBoolean() ? null : randomNodeAttributes(),
            randomStats(),
            DataFrameTransformCheckpointingInfoTests.randomDataFrameTransformCheckpointingInfo());
    }

    @Override
    protected DataFrameTransformStats createTestInstance() {
        return randomDataFrameTransformStats();
    }

    @Override
    protected DataFrameTransformStats doParseInstance(XContentParser parser) throws IOException {
        return DataFrameTransformStats.PARSER.apply(parser, null);
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.contains("position") || field.equals("node.attributes");
    }

    public static DataFrameTransformProgress randomDataFrameTransformProgress() {
        Long totalDocs = randomBoolean() ? null : randomNonNegativeLong();
        Long docsRemaining = totalDocs != null ? randomLongBetween(0, totalDocs) : null;
        return new DataFrameTransformProgress(
            totalDocs,
            docsRemaining,
            totalDocs != null ? totalDocs - docsRemaining : randomNonNegativeLong(),
            randomBoolean() ? null : randomNonNegativeLong());
    }

    public static DataFrameTransformCheckpointingInfo randomDataFrameTransformCheckpointingInfo() {
        return new DataFrameTransformCheckpointingInfo(randomDataFrameTransformCheckpointStats(),
            randomDataFrameTransformCheckpointStats(), randomNonNegativeLong(),
            randomBoolean() ? null : Instant.ofEpochMilli(randomNonNegativeLong()));
    }

    public static DataFrameTransformCheckpointStats randomDataFrameTransformCheckpointStats() {
        return new DataFrameTransformCheckpointStats(randomLongBetween(1, 1_000_000),
            DataFrameIndexerPositionTests.randomDataFrameIndexerPosition(),
            randomBoolean() ? null : DataFrameTransformProgressTests.randomDataFrameTransformProgress(),
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

    public static DataFrameIndexerTransformStats randomStats() {
        return new DataFrameIndexerTransformStats(randomLongBetween(10L, 10000L),
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
