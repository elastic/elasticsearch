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
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformStats.State.STARTED;
import static org.hamcrest.Matchers.equalTo;

public class DataFrameTransformStatsTests extends AbstractSerializingTestCase<DataFrameTransformStats> {

    public static DataFrameTransformStats randomDataFrameTransformStats() {
        return new DataFrameTransformStats(randomAlphaOfLength(10),
            randomFrom(DataFrameTransformStats.State.values()),
            randomBoolean() ? null : randomAlphaOfLength(100),
            randomBoolean() ? null : NodeAttributeTests.randomNodeAttributes(),
            DataFrameIndexerTransformStatsTests.randomStats(),
            DataFrameTransformCheckpointingInfoTests.randomDataFrameTransformCheckpointingInfo());
    }

    @Override
    protected DataFrameTransformStats doParseInstance(XContentParser parser) throws IOException {
        return DataFrameTransformStats.fromXContent(parser);
    }

    @Override
    protected DataFrameTransformStats createTestInstance() {
        return randomDataFrameTransformStats();
    }

    @Override
    protected Reader<DataFrameTransformStats> instanceReader() {
        return DataFrameTransformStats::new;
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
            DataFrameTransformStats stats = new DataFrameTransformStats("bwc-id",
                STARTED,
                randomBoolean() ? null : randomAlphaOfLength(100),
                randomBoolean() ? null : NodeAttributeTests.randomNodeAttributes(),
                new DataFrameIndexerTransformStats(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
                new DataFrameTransformCheckpointingInfo(
                    new DataFrameTransformCheckpointStats(0, null, null, 10, 100),
                    new DataFrameTransformCheckpointStats(0, null, null, 100, 1000),
                    // changesLastDetectedAt aren't serialized back
                    100, null));
            try (BytesStreamOutput output = new BytesStreamOutput()) {
                output.setVersion(Version.V_7_3_0);
                stats.writeTo(output);
                try (StreamInput in = output.bytes().streamInput()) {
                    in.setVersion(Version.V_7_3_0);
                    DataFrameTransformStats statsFromOld = new DataFrameTransformStats(in);
                    assertThat(statsFromOld, equalTo(stats));
                }
            }
        }
    }
}
