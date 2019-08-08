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

package org.elasticsearch.client.dataframe.transforms;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class DataFrameTransformCheckpointStatsTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        xContentTester(this::createParser,
            DataFrameTransformCheckpointStatsTests::randomDataFrameTransformCheckpointStats,
            DataFrameTransformCheckpointStatsTests::toXContent,
            DataFrameTransformCheckpointStats::fromXContent)
                .supportsUnknownFields(true)
                .randomFieldsExcludeFilter(field -> field.startsWith("position"))
                .test();
    }

    public static DataFrameTransformCheckpointStats randomDataFrameTransformCheckpointStats() {
        return new DataFrameTransformCheckpointStats(randomLongBetween(1, 1_000_000),
            randomBoolean() ? null : DataFrameIndexerPositionTests.randomDataFrameIndexerPosition(),
            randomBoolean() ? null : DataFrameTransformProgressTests.randomInstance(),
            randomLongBetween(1, 1_000_000), randomLongBetween(0, 1_000_000));
    }

    public static void toXContent(DataFrameTransformCheckpointStats stats, XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.field(DataFrameTransformCheckpointStats.CHECKPOINT.getPreferredName(), stats.getCheckpoint());
        if (stats.getPosition() != null) {
            builder.field(DataFrameTransformCheckpointStats.POSITION.getPreferredName());
            DataFrameIndexerPositionTests.toXContent(stats.getPosition(), builder);
        }
        if (stats.getCheckpointProgress() != null) {
            builder.field(DataFrameTransformCheckpointStats.CHECKPOINT_PROGRESS.getPreferredName());
            DataFrameTransformProgressTests.toXContent(stats.getCheckpointProgress(), builder);
        }
        builder.field(DataFrameTransformCheckpointStats.TIMESTAMP_MILLIS.getPreferredName(), stats.getTimestampMillis());
        builder.field(DataFrameTransformCheckpointStats.TIME_UPPER_BOUND_MILLIS.getPreferredName(), stats.getTimeUpperBoundMillis());
        builder.endObject();
    }
}
