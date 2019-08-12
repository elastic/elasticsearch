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
import java.time.Instant;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class DataFrameTransformCheckpointingInfoTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        xContentTester(this::createParser,
            DataFrameTransformCheckpointingInfoTests::randomDataFrameTransformCheckpointingInfo,
            DataFrameTransformCheckpointingInfoTests::toXContent,
            DataFrameTransformCheckpointingInfo::fromXContent)
                .supportsUnknownFields(false)
                .test();
    }

    public static DataFrameTransformCheckpointingInfo randomDataFrameTransformCheckpointingInfo() {
        return new DataFrameTransformCheckpointingInfo(
            DataFrameTransformCheckpointStatsTests.randomDataFrameTransformCheckpointStats(),
            DataFrameTransformCheckpointStatsTests.randomDataFrameTransformCheckpointStats(),
            randomLongBetween(0, 10000),
            randomBoolean() ? null : Instant.ofEpochMilli(randomNonNegativeLong()));
    }

    public static void toXContent(DataFrameTransformCheckpointingInfo info, XContentBuilder builder) throws IOException {
        builder.startObject();
        if (info.getLast().getTimestampMillis() > 0) {
            builder.field(DataFrameTransformCheckpointingInfo.LAST_CHECKPOINT.getPreferredName());
            DataFrameTransformCheckpointStatsTests.toXContent(info.getLast(), builder);
        }
        if (info.getNext().getTimestampMillis() > 0) {
            builder.field(DataFrameTransformCheckpointingInfo.NEXT_CHECKPOINT.getPreferredName());
            DataFrameTransformCheckpointStatsTests.toXContent(info.getNext(), builder);
        }
        builder.field(DataFrameTransformCheckpointingInfo.OPERATIONS_BEHIND.getPreferredName(), info.getOperationsBehind());
        if (info.getChangesLastDetectedAt() != null) {
            builder.field(DataFrameTransformCheckpointingInfo.CHANGES_LAST_DETECTED_AT.getPreferredName(), info.getChangesLastDetectedAt());
        }
        builder.endObject();
    }
}
