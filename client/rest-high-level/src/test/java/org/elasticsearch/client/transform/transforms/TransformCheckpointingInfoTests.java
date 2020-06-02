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

package org.elasticsearch.client.transform.transforms;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.time.Instant;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class TransformCheckpointingInfoTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        xContentTester(this::createParser,
            TransformCheckpointingInfoTests::randomTransformCheckpointingInfo,
            TransformCheckpointingInfoTests::toXContent,
            TransformCheckpointingInfo::fromXContent)
                .supportsUnknownFields(false)
                .test();
    }

    public static TransformCheckpointingInfo randomTransformCheckpointingInfo() {
        return new TransformCheckpointingInfo(
            TransformCheckpointStatsTests.randomTransformCheckpointStats(),
            TransformCheckpointStatsTests.randomTransformCheckpointStats(),
            randomLongBetween(0, 10000),
            randomBoolean() ? null : Instant.ofEpochMilli(randomNonNegativeLong()));
    }

    public static void toXContent(TransformCheckpointingInfo info, XContentBuilder builder) throws IOException {
        builder.startObject();
        if (info.getLast().getTimestampMillis() > 0) {
            builder.field(TransformCheckpointingInfo.LAST_CHECKPOINT.getPreferredName());
            TransformCheckpointStatsTests.toXContent(info.getLast(), builder);
        }
        if (info.getNext().getTimestampMillis() > 0) {
            builder.field(TransformCheckpointingInfo.NEXT_CHECKPOINT.getPreferredName());
            TransformCheckpointStatsTests.toXContent(info.getNext(), builder);
        }
        builder.field(TransformCheckpointingInfo.OPERATIONS_BEHIND.getPreferredName(), info.getOperationsBehind());
        if (info.getChangesLastDetectedAt() != null) {
            builder.field(TransformCheckpointingInfo.CHANGES_LAST_DETECTED_AT.getPreferredName(), info.getChangesLastDetectedAt());
        }
        builder.endObject();
    }
}
