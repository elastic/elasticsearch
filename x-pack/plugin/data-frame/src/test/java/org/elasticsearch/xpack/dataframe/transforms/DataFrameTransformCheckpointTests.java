/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transforms;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.dataframe.transforms.AbstractSerializingDataFrameTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.elasticsearch.test.TestMatchers.matchesPattern;

public class DataFrameTransformCheckpointTests extends AbstractSerializingDataFrameTestCase<DataFrameTransformCheckpoint> {

    public static DataFrameTransformCheckpoint randomDataFrameTransformCheckpoints() {
        return new DataFrameTransformCheckpoint(randomAlphaOfLengthBetween(1, 10), randomNonNegativeLong(), randomNonNegativeLong(),
                randomCheckpointsByIndex(), randomNonNegativeLong());
    }

    @Override
    protected DataFrameTransformCheckpoint doParseInstance(XContentParser parser) throws IOException {
        return DataFrameTransformCheckpoint.fromXContent(parser, false);
    }

    @Override
    protected DataFrameTransformCheckpoint createTestInstance() {
        return randomDataFrameTransformCheckpoints();
    }

    @Override
    protected Reader<DataFrameTransformCheckpoint> instanceReader() {
        return DataFrameTransformCheckpoint::new;
    }

    public void testXContentForInternalStorage() throws IOException {
        DataFrameTransformCheckpoint dataFrameTransformCheckpoints = randomDataFrameTransformCheckpoints();

        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
            XContentBuilder content = dataFrameTransformCheckpoints.toXContent(xContentBuilder, getToXContentParams());
            String doc = Strings.toString(content);

            assertThat(doc, matchesPattern(".*\"doc_type\"\\s*:\\s*\"data_frame_transform_checkpoint\".*"));
        }
    }

    public void testMatches() throws IOException {
        String id = randomAlphaOfLengthBetween(1, 10);
        long timestamp = randomNonNegativeLong();
        long checkpoint = randomNonNegativeLong();
        Map<String, long[]> checkpointsByIndex = randomCheckpointsByIndex();
        Map<String, long[]> otherCheckpointsByIndex = new TreeMap<>(checkpointsByIndex);
        otherCheckpointsByIndex.put(randomAlphaOfLengthBetween(1, 10), new long[] { 1, 2, 3 });
        long timeUpperBound = randomNonNegativeLong();

        DataFrameTransformCheckpoint dataFrameTransformCheckpoints = new DataFrameTransformCheckpoint(id, timestamp, checkpoint,
                checkpointsByIndex, timeUpperBound);

        // same
        assertTrue(dataFrameTransformCheckpoints.matches(dataFrameTransformCheckpoints));
        DataFrameTransformCheckpoint dataFrameTransformCheckpointsCopy = copyInstance(dataFrameTransformCheckpoints);

        // with copy
        assertTrue(dataFrameTransformCheckpoints.matches(dataFrameTransformCheckpointsCopy));
        assertTrue(dataFrameTransformCheckpointsCopy.matches(dataFrameTransformCheckpoints));

        // other id
        assertFalse(dataFrameTransformCheckpoints
                .matches(new DataFrameTransformCheckpoint(id + "-1", timestamp, checkpoint, checkpointsByIndex, timeUpperBound)));
        // other timestamp
        assertTrue(dataFrameTransformCheckpoints
                .matches(new DataFrameTransformCheckpoint(id, (timestamp / 2) + 1, checkpoint, checkpointsByIndex, timeUpperBound)));
        // other checkpoint
        assertTrue(dataFrameTransformCheckpoints
                .matches(new DataFrameTransformCheckpoint(id, timestamp, (checkpoint / 2) + 1, checkpointsByIndex, timeUpperBound)));
        // other index checkpoints
        assertFalse(dataFrameTransformCheckpoints
                .matches(new DataFrameTransformCheckpoint(id, timestamp, checkpoint, otherCheckpointsByIndex, timeUpperBound)));
        // other time upper bound
        assertTrue(dataFrameTransformCheckpoints
                .matches(new DataFrameTransformCheckpoint(id, timestamp, checkpoint, checkpointsByIndex, (timeUpperBound / 2) + 1)));
    }

    private static Map<String, long[]> randomCheckpointsByIndex() {
        Map<String, long[]> checkpointsByIndex = new TreeMap<>();
        for (int i = 0; i < randomIntBetween(1, 10); ++i) {
            List<Long> checkpoints = new ArrayList<>();
            for (int j = 0; j < randomIntBetween(1, 20); ++j) {
                checkpoints.add(randomNonNegativeLong());
            }
            checkpointsByIndex.put(randomAlphaOfLengthBetween(1, 10), checkpoints.stream().mapToLong(l -> l).toArray());
        }
        return checkpointsByIndex;
    }
}
