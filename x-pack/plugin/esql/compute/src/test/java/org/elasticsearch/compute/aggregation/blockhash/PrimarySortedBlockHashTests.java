/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.compute.aggregation.GroupRuns;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.ComputeTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * The int and long hashes built as the index's primary sort field find the runs of equal adjacent keys and hand them to the
 * aggregators as {@link GroupRuns}. The runs are only a statement about the shape of the group ids, so whatever the order of the
 * keys, expanding the runs must give exactly the group ids the plain hash gives.
 */
public class PrimarySortedBlockHashTests extends ComputeTestCase {

    /** What a hash handed to its {@link GroupingAggregatorFunction.AddInput}, for one page. */
    private record Seen(String how, int[] groupIds, List<int[]> runBounds) {}

    public void testLongRunsHaveTheBoundariesOfTheKeys() {
        long[] keys = { 5, 5, 5, 7, 7, 5, 9 };
        Seen seen = add(new LongBlockHashFactory(true), keys);
        assertThat(seen.how, equalTo("runs"));
        assertThat(seen.groupIds, equalTo(new int[] { 1, 1, 1, 2, 2, 1, 3 }));
        // a key that comes back later is a new run of the same group
        assertRuns(seen, new int[][] { { 0, 3, 1 }, { 3, 5, 2 }, { 5, 6, 1 }, { 6, 7, 3 } });
    }

    public void testIntRunsHaveTheBoundariesOfTheKeys() {
        int[] keys = { 5, 5, 5, 7, 7, 5, 9 };
        try (
            BlockHash hash = new IntBlockHash(0, blockFactory(), true);
            IntBlock block = blockFactory().newIntArrayVector(keys, keys.length).asBlock()
        ) {
            RecordingAddInput recorder = new RecordingAddInput();
            hash.add(new Page(block), recorder);
            Seen seen = recorder.seen();
            assertThat(seen.how, equalTo("runs"));
            assertThat(seen.groupIds, equalTo(new int[] { 1, 1, 1, 2, 2, 1, 3 }));
            assertRuns(seen, new int[][] { { 0, 3, 1 }, { 3, 5, 2 }, { 5, 6, 1 }, { 6, 7, 3 } });
        }
    }

    public void testASingleRunPage() {
        long[] keys = new long[between(1, 500)];
        Arrays.fill(keys, randomLong());
        Seen seen = add(new LongBlockHashFactory(true), keys);
        assertThat(seen.how, equalTo("runs"));
        assertThat(seen.runBounds.size(), equalTo(1));
        assertThat(seen.runBounds.get(0)[0], equalTo(0));
        assertThat(seen.runBounds.get(0)[1], equalTo(keys.length));
    }

    /** Sorted or not, expanding the runs gives the ids of the plain hash, and so does a hash that sees several pages. */
    public void testSameGroupIdsAsThePlainHash() {
        try (BlockHash sorted = new LongBlockHash(0, blockFactory(), true); BlockHash plain = new LongBlockHash(0, blockFactory(), false)) {
            for (int page = 0; page < between(2, 6); page++) {
                long[] keys = new long[between(1, 3000)];
                boolean sortedKeys = randomBoolean();
                long key = randomLongBetween(0, 5);
                for (int i = 0; i < keys.length; i++) {
                    keys[i] = sortedKeys ? (key += randomInt(3) == 0 ? randomIntBetween(1, 5) : 0) : randomLongBetween(0, 40);
                }
                int[] viaRuns = ids(sorted, keys);
                int[] viaPlain = ids(plain, keys);
                assertThat("sorted keys=" + sortedKeys, viaRuns, equalTo(viaPlain));
            }
        }
    }

    /**
     * Nearly unique keys make every run one row long, and enough of them make the hash table large enough for the prefetching
     * lookups. The result must still be the plain hash's.
     */
    public void testNearlyUniqueKeys() {
        try (BlockHash sorted = new LongBlockHash(0, blockFactory(), true); BlockHash plain = new LongBlockHash(0, blockFactory(), false)) {
            long key = 0;
            for (int page = 0; page < 20; page++) {
                long[] keys = new long[8192];
                for (int i = 0; i < keys.length; i++) {
                    keys[i] = key += randomIntBetween(1, 2);
                }
                assertThat("page " + page, ids(sorted, keys), equalTo(ids(plain, keys)));
            }
        }
    }

    /** A page with a null or a multivalue has ids that are not a vector, so it never takes the runs path. */
    public void testNullsAndMultivaluesFallBack() {
        try (
            BlockHash hash = new LongBlockHash(0, blockFactory(), true);
            LongBlock.Builder builder = blockFactory().newLongBlockBuilder(4)
        ) {
            builder.appendLong(1);
            builder.appendNull();
            builder.beginPositionEntry().appendLong(1).appendLong(2).endPositionEntry();
            builder.appendLong(2);
            try (LongBlock block = builder.build()) {
                RecordingAddInput recorder = new RecordingAddInput();
                hash.add(new Page(block), recorder);
                assertThat(recorder.seen().how, not(equalTo("runs")));
            }
        }
    }

    public void testPlainHashNeverUsesRuns() {
        long[] keys = { 1, 1, 2, 2 };
        assertThat(add(new LongBlockHashFactory(false), keys).how, equalTo("vector"));
    }

    public void testToStringSaysTheSortedPathWasTaken() {
        long[] keys = { 1, 1, 2 };
        try (BlockHash hash = new LongBlockHash(0, blockFactory(), true)) {
            assertThat(hash.toString(), containsString("primarySorted=true, sortedPages=0"));
            ids(hash, keys);
            ids(hash, keys);
            assertThat(hash.toString(), containsString("primarySorted=true, sortedPages=2"));
        }
        try (BlockHash hash = new LongBlockHash(0, blockFactory(), false)) {
            ids(hash, keys);
            assertThat(hash.toString(), not(containsString("primarySorted")));
        }
    }

    private interface HashFactory {
        BlockHash build();
    }

    private final class LongBlockHashFactory implements HashFactory {
        private final boolean primarySorted;

        LongBlockHashFactory(boolean primarySorted) {
            this.primarySorted = primarySorted;
        }

        @Override
        public BlockHash build() {
            return new LongBlockHash(0, blockFactory(), primarySorted);
        }
    }

    private Seen add(HashFactory factory, long[] keys) {
        try (BlockHash hash = factory.build(); LongBlock block = blockFactory().newLongArrayVector(keys, keys.length).asBlock()) {
            RecordingAddInput recorder = new RecordingAddInput();
            hash.add(new Page(block), recorder);
            return recorder.seen();
        }
    }

    /** The group id of every key of a page, however the hash handed them over. */
    private int[] ids(BlockHash hash, long[] keys) {
        try (LongBlock block = blockFactory().newLongArrayVector(keys.clone(), keys.length).asBlock()) {
            RecordingAddInput recorder = new RecordingAddInput();
            hash.add(new Page(block), recorder);
            return recorder.seen().groupIds;
        }
    }

    private static void assertRuns(Seen seen, int[][] expectedStartEndGroup) {
        assertThat(seen.runBounds.size(), equalTo(expectedStartEndGroup.length));
        for (int r = 0; r < expectedStartEndGroup.length; r++) {
            assertThat("run " + r, seen.runBounds.get(r), equalTo(expectedStartEndGroup[r]));
        }
    }

    /** An {@link GroupingAggregatorFunction.AddInput} that records how the group ids were handed over. */
    private static class RecordingAddInput implements GroupingAggregatorFunction.AddInput {
        private String how = "none";
        private int[] groupIds;
        private final List<int[]> runBounds = new ArrayList<>();

        Seen seen() {
            return new Seen(how, groupIds, runBounds);
        }

        @Override
        public void add(int positionOffset, IntArrayBlock groupIds) {
            how = "block";
            this.groupIds = new int[0];
        }

        @Override
        public void add(int positionOffset, IntBigArrayBlock groupIds) {
            how = "big block";
            this.groupIds = new int[0];
        }

        @Override
        public void add(int positionOffset, IntVector groupIds) {
            how = "vector";
            this.groupIds = new int[groupIds.getPositionCount()];
            for (int p = 0; p < this.groupIds.length; p++) {
                this.groupIds[p] = groupIds.getInt(p);
            }
        }

        @Override
        public void addRuns(int positionOffset, GroupRuns runs) {
            how = "runs";
            groupIds = new int[runs.positionCount()];
            for (int run = 0; run < runs.runCount(); run++) {
                runBounds.add(new int[] { runs.start(run), runs.end(run), runs.groupId(run) });
                for (int p = runs.start(run); p < runs.end(run); p++) {
                    groupIds[p] = runs.groupId(run);
                }
            }
        }

        @Override
        public void close() {}
    }
}
