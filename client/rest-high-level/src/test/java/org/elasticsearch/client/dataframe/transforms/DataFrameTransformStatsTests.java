/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.dataframe.transforms;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class DataFrameTransformStatsTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        xContentTester(this::createParser,
            DataFrameTransformStatsTests::randomInstance,
            DataFrameTransformStatsTests::toXContent,
            DataFrameTransformStats::fromXContent)
                .supportsUnknownFields(true)
                .randomFieldsExcludeFilter(field -> field.equals("node.attributes") || field.contains("position"))
                .test();
    }

    public static DataFrameTransformStats randomInstance() {
        return new DataFrameTransformStats(randomAlphaOfLength(10),
            randomBoolean() ? null : randomFrom(DataFrameTransformStats.State.values()),
            randomBoolean() ? null : randomAlphaOfLength(100),
            randomBoolean() ? null : NodeAttributesTests.createRandom(),
            DataFrameIndexerTransformStatsTests.randomStats(),
            randomBoolean() ? null : DataFrameTransformCheckpointingInfoTests.randomDataFrameTransformCheckpointingInfo());
    }

    public static void toXContent(DataFrameTransformStats stats, XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.field(DataFrameTransformStats.ID.getPreferredName(), stats.getId());
        if (stats.getState() != null) {
            builder.field(DataFrameTransformStats.STATE_FIELD.getPreferredName(),
                stats.getState().value());
        }
        if (stats.getReason() != null) {
            builder.field(DataFrameTransformStats.REASON_FIELD.getPreferredName(), stats.getReason());
        }
        if (stats.getNode() != null) {
            builder.field(DataFrameTransformStats.NODE_FIELD.getPreferredName());
            stats.getNode().toXContent(builder, ToXContent.EMPTY_PARAMS);
        }
        builder.field(DataFrameTransformStats.STATS_FIELD.getPreferredName());
        DataFrameIndexerTransformStatsTests.toXContent(stats.getIndexerStats(), builder);
        if (stats.getCheckpointingInfo() != null) {
            builder.field(DataFrameTransformStats.CHECKPOINTING_INFO_FIELD.getPreferredName());
            DataFrameTransformCheckpointingInfoTests.toXContent(stats.getCheckpointingInfo(), builder);
        }
        builder.endObject();
    }
}
