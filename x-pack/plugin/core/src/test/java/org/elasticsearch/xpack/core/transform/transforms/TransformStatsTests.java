/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.core.transform.transforms.TransformStats.State.STARTED;
import static org.hamcrest.Matchers.equalTo;

public class TransformStatsTests extends AbstractSerializingTestCase<TransformStats> {

    public static TransformStats randomDataFrameTransformStats() {
        return new TransformStats(randomAlphaOfLength(10),
            randomFrom(TransformStats.State.values()),
            randomBoolean() ? null : randomAlphaOfLength(100),
            randomBoolean() ? null : NodeAttributeTests.randomNodeAttributes(),
            TransformIndexerStatsTests.randomStats(),
            TransformCheckpointingInfoTests.randomDataFrameTransformCheckpointingInfo());
    }

    @Override
    protected TransformStats doParseInstance(XContentParser parser) throws IOException {
        return TransformStats.fromXContent(parser);
    }

    @Override
    protected TransformStats createTestInstance() {
        return randomDataFrameTransformStats();
    }

    @Override
    protected Reader<TransformStats> instanceReader() {
        return TransformStats::new;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected String[] getShuffleFieldsExceptions() {
        return new String[] { "position" };
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> !field.isEmpty();
    }

    public void testBwcWith73() throws IOException {
        for(int i = 0; i < NUMBER_OF_TEST_RUNS; i++) {
            TransformStats stats = new TransformStats("bwc-id",
                STARTED,
                randomBoolean() ? null : randomAlphaOfLength(100),
                randomBoolean() ? null : NodeAttributeTests.randomNodeAttributes(),
                new TransformIndexerStats(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
                new TransformCheckpointingInfo(
                    new TransformCheckpointStats(0, null, null, 10, 100),
                    new TransformCheckpointStats(0, null, null, 100, 1000),
                    // changesLastDetectedAt aren't serialized back
                    100, null));
            try (BytesStreamOutput output = new BytesStreamOutput()) {
                output.setVersion(Version.V_7_3_0);
                stats.writeTo(output);
                try (StreamInput in = output.bytes().streamInput()) {
                    in.setVersion(Version.V_7_3_0);
                    TransformStats statsFromOld = new TransformStats(in);
                    assertThat(statsFromOld, equalTo(stats));
                }
            }
        }
    }
}
