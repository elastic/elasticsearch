/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.time.Instant;

public class DataFrameTransformCheckpointingInfoTests extends AbstractSerializingDataFrameTestCase<DataFrameTransformCheckpointingInfo> {

    public static DataFrameTransformCheckpointingInfo randomDataFrameTransformCheckpointingInfo() {
        return new DataFrameTransformCheckpointingInfo(
            DataFrameTransformCheckpointStatsTests.randomDataFrameTransformCheckpointStats(),
            DataFrameTransformCheckpointStatsTests.randomDataFrameTransformCheckpointStats(),
            randomNonNegativeLong(),
            randomBoolean() ? null : Instant.ofEpochMilli(randomLongBetween(1, 100000)));
    }

    @Override
    protected DataFrameTransformCheckpointingInfo doParseInstance(XContentParser parser) throws IOException {
        return DataFrameTransformCheckpointingInfo.fromXContent(parser);
    }

    @Override
    protected DataFrameTransformCheckpointingInfo createTestInstance() {
        return randomDataFrameTransformCheckpointingInfo();
    }

    @Override
    protected Reader<DataFrameTransformCheckpointingInfo> instanceReader() {
        return DataFrameTransformCheckpointingInfo::new;
    }

    public void testBackwardsSerialization() throws IOException {
        DataFrameTransformCheckpointingInfo checkpointingInfo = new DataFrameTransformCheckpointingInfo(
            DataFrameTransformCheckpointStats.EMPTY,
            DataFrameTransformCheckpointStats.EMPTY,
            randomNonNegativeLong(),
            // changesLastDetectedAt is not serialized to past values, so when it is pulled back in, it will be null
            null);
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setVersion(Version.V_7_4_0);
            checkpointingInfo.writeTo(output);
            try (StreamInput in = output.bytes().streamInput()) {
                in.setVersion(Version.V_7_4_0);
                DataFrameTransformCheckpointingInfo streamedCheckpointingInfo = new DataFrameTransformCheckpointingInfo(in);
                assertEquals(checkpointingInfo, streamedCheckpointingInfo);
            }
        }
    }
}
