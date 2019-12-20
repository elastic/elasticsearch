/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.elasticsearch.test.TestMatchers.matchesPattern;

public class TransformCheckpointTests extends AbstractSerializingTransformTestCase<TransformCheckpoint> {

    public static TransformCheckpoint randomTransformCheckpoints() {
        return new TransformCheckpoint(
            randomAlphaOfLengthBetween(1, 10),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomCheckpointsByIndex(),
            randomNonNegativeLong()
        );
    }

    @Override
    protected TransformCheckpoint doParseInstance(XContentParser parser) throws IOException {
        return TransformCheckpoint.fromXContent(parser, false);
    }

    @Override
    protected TransformCheckpoint createTestInstance() {
        return randomTransformCheckpoints();
    }

    @Override
    protected Reader<TransformCheckpoint> instanceReader() {
        return TransformCheckpoint::new;
    }

    public void testXContentForInternalStorage() throws IOException {
        TransformCheckpoint dataFrameTransformCheckpoints = randomTransformCheckpoints();

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

        TransformCheckpoint dataFrameTransformCheckpoints = new TransformCheckpoint(
            id,
            timestamp,
            checkpoint,
            checkpointsByIndex,
            timeUpperBound
        );

        // same
        assertTrue(dataFrameTransformCheckpoints.matches(dataFrameTransformCheckpoints));
        TransformCheckpoint dataFrameTransformCheckpointsCopy = copyInstance(dataFrameTransformCheckpoints);

        // with copy
        assertTrue(dataFrameTransformCheckpoints.matches(dataFrameTransformCheckpointsCopy));
        assertTrue(dataFrameTransformCheckpointsCopy.matches(dataFrameTransformCheckpoints));

        // other id
        assertFalse(
            dataFrameTransformCheckpoints.matches(
                new TransformCheckpoint(id + "-1", timestamp, checkpoint, checkpointsByIndex, timeUpperBound)
            )
        );
        // other timestamp
        assertTrue(
            dataFrameTransformCheckpoints.matches(
                new TransformCheckpoint(id, (timestamp / 2) + 1, checkpoint, checkpointsByIndex, timeUpperBound)
            )
        );
        // other checkpoint
        assertTrue(
            dataFrameTransformCheckpoints.matches(
                new TransformCheckpoint(id, timestamp, (checkpoint / 2) + 1, checkpointsByIndex, timeUpperBound)
            )
        );
        // other index checkpoints
        assertFalse(
            dataFrameTransformCheckpoints.matches(
                new TransformCheckpoint(id, timestamp, checkpoint, otherCheckpointsByIndex, timeUpperBound)
            )
        );
        // other time upper bound
        assertTrue(
            dataFrameTransformCheckpoints.matches(
                new TransformCheckpoint(id, timestamp, checkpoint, checkpointsByIndex, (timeUpperBound / 2) + 1)
            )
        );
    }

    public void testEmpty() {
        assertTrue(TransformCheckpoint.EMPTY.isEmpty());
        assertFalse(new TransformCheckpoint("some_id", 0L, -1, Collections.emptyMap(), 0L).isEmpty());
    }

    public void testGetBehind() {
        String baseIndexName = randomAlphaOfLength(8);
        String id = randomAlphaOfLengthBetween(1, 10);
        long timestamp = randomNonNegativeLong();

        TreeMap<String, long[]> checkpointsByIndexOld = new TreeMap<>();
        TreeMap<String, long[]> checkpointsByIndexNew = new TreeMap<>();

        int indices = randomIntBetween(3, 10);
        int shards = randomIntBetween(1, 20);

        for (int i = 0; i < indices; ++i) {
            List<Long> checkpoints1 = new ArrayList<>();
            List<Long> checkpoints2 = new ArrayList<>();

            for (int j = 0; j < shards; ++j) {
                long shardCheckpoint = randomLongBetween(-1, 1_000_000);
                checkpoints1.add(shardCheckpoint);
                checkpoints2.add(shardCheckpoint + 10);
            }

            String indexName = baseIndexName + i;

            checkpointsByIndexOld.put(indexName, checkpoints1.stream().mapToLong(l -> l).toArray());
            checkpointsByIndexNew.put(indexName, checkpoints2.stream().mapToLong(l -> l).toArray());
        }

        long checkpoint = randomLongBetween(10, 100);

        TransformCheckpoint checkpointOld = new TransformCheckpoint(id, timestamp, checkpoint, checkpointsByIndexOld, 0L);
        TransformCheckpoint checkpointTransientNew = new TransformCheckpoint(id, timestamp, -1L, checkpointsByIndexNew, 0L);
        TransformCheckpoint checkpointNew = new TransformCheckpoint(id, timestamp, checkpoint + 1, checkpointsByIndexNew, 0L);
        TransformCheckpoint checkpointOlderButNewerShardsCheckpoint = new TransformCheckpoint(
            id,
            timestamp,
            checkpoint - 1,
            checkpointsByIndexNew,
            0L
        );

        assertEquals(indices * shards * 10L, TransformCheckpoint.getBehind(checkpointOld, checkpointTransientNew));
        assertEquals(indices * shards * 10L, TransformCheckpoint.getBehind(checkpointOld, checkpointNew));

        // no difference for same checkpoints, transient or not
        assertEquals(0L, TransformCheckpoint.getBehind(checkpointOld, checkpointOld));
        assertEquals(0L, TransformCheckpoint.getBehind(checkpointTransientNew, checkpointTransientNew));
        assertEquals(0L, TransformCheckpoint.getBehind(checkpointNew, checkpointNew));

        // new vs transient new: ok
        assertEquals(0L, TransformCheckpoint.getBehind(checkpointNew, checkpointTransientNew));

        // transient new vs new: illegal
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> TransformCheckpoint.getBehind(checkpointTransientNew, checkpointNew)
        );
        assertEquals("can not compare transient against a non transient checkpoint", e.getMessage());

        // new vs old: illegal
        e = expectThrows(IllegalArgumentException.class, () -> TransformCheckpoint.getBehind(checkpointNew, checkpointOld));
        assertEquals("old checkpoint is newer than new checkpoint", e.getMessage());

        // corner case: the checkpoint appears older but the inner shard checkpoints are newer
        assertEquals(-1L, TransformCheckpoint.getBehind(checkpointOlderButNewerShardsCheckpoint, checkpointOld));

        // test cases where indices sets do not match
        // remove something from old, so newer has 1 index more than old: should be equivalent to old index existing but empty
        checkpointsByIndexOld.remove(checkpointsByIndexOld.firstKey());
        long behind = TransformCheckpoint.getBehind(checkpointOld, checkpointTransientNew);
        assertTrue(
            "Expected behind (" + behind + ") => sum of shard checkpoint differences (" + indices * shards * 10L + ")",
            behind >= indices * shards * 10L
        );

        // remove same key: old and new should have equal indices again
        checkpointsByIndexNew.remove(checkpointsByIndexNew.firstKey());
        assertEquals((indices - 1) * shards * 10L, TransformCheckpoint.getBehind(checkpointOld, checkpointTransientNew));

        // remove 1st index from new, now old has 1 index more, which should be ignored
        checkpointsByIndexNew.remove(checkpointsByIndexNew.firstKey());

        assertEquals((indices - 2) * shards * 10L, TransformCheckpoint.getBehind(checkpointOld, checkpointTransientNew));
    }

    private static Map<String, long[]> randomCheckpointsByIndex() {
        Map<String, long[]> checkpointsByIndex = new TreeMap<>();
        int indices = randomIntBetween(1, 10);
        for (int i = 0; i < indices; ++i) {
            List<Long> checkpoints = new ArrayList<>();
            int shards = randomIntBetween(1, 20);
            for (int j = 0; j < shards; ++j) {
                checkpoints.add(randomLongBetween(0, 1_000_000));
            }
            checkpointsByIndex.put(randomAlphaOfLengthBetween(1, 10), checkpoints.stream().mapToLong(l -> l).toArray());
        }
        return checkpointsByIndex;
    }
}
