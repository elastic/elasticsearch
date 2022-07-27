/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.transform.AbstractSerializingTransformTestCase;

import java.io.IOException;
import java.time.Instant;

public class TransformCheckpointingInfoTests extends AbstractSerializingTransformTestCase<TransformCheckpointingInfo> {

    public static TransformCheckpointingInfo randomTransformCheckpointingInfo() {
        return new TransformCheckpointingInfo(
            TransformCheckpointStatsTests.randomTransformCheckpointStats(),
            TransformCheckpointStatsTests.randomTransformCheckpointStats(),
            randomNonNegativeLong(),
            randomBoolean() ? null : Instant.ofEpochMilli(randomLongBetween(1, 100000)),
            randomBoolean() ? null : Instant.ofEpochMilli(randomLongBetween(1, 100000))
        );
    }

    @Override
    protected TransformCheckpointingInfo doParseInstance(XContentParser parser) throws IOException {
        return TransformCheckpointingInfo.fromXContent(parser);
    }

    @Override
    protected TransformCheckpointingInfo createTestInstance() {
        return randomTransformCheckpointingInfo();
    }

    @Override
    protected Reader<TransformCheckpointingInfo> instanceReader() {
        return TransformCheckpointingInfo::new;
    }

    public void testBackwardsSerialization() throws IOException {
        TransformCheckpointingInfo checkpointingInfo = new TransformCheckpointingInfo(
            TransformCheckpointStats.EMPTY,
            TransformCheckpointStats.EMPTY,
            randomNonNegativeLong(),
            // changesLastDetectedAt, lastSearchTime is not serialized to past values, so when it is pulled back in, it will be null
            null,
            null
        );
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setVersion(Version.V_7_4_0);
            checkpointingInfo.writeTo(output);
            try (StreamInput in = output.bytes().streamInput()) {
                in.setVersion(Version.V_7_4_0);
                TransformCheckpointingInfo streamedCheckpointingInfo = new TransformCheckpointingInfo(in);
                assertEquals(checkpointingInfo, streamedCheckpointingInfo);
            }
        }
    }
}
