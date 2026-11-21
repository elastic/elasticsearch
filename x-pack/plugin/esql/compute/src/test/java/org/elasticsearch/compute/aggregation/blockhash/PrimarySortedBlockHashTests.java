/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.elasticsearch.compute.aggregation.GroupRuns;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.ElementType;
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
 * The int hash built as the index's primary sort field finds the runs of equal adjacent keys and hands them to the
 * aggregators as {@link GroupRuns}, unless the page has too few repeated keys to gain from it. The runs are only a statement
 * about the shape of the group ids, so whatever the order of the keys, expanding the runs must give exactly the group ids the
 * plain hash gives.
 */
public class PrimarySortedBlockHashTests extends ComputeTestCase {

    /** What a hash handed to its {@link GroupingAggregatorFunction.AddInput}, for one page. */
    private record Seen(String how, int[] groupIds, List<int[]> runBounds) {}

    public void testRunsHaveTheBoundariesOfTheKeys() {
        int[] keys = { 5, 5, 5, 7, 7, 5, 5, 9, 9, 9 };
        try (BlockHash hash = new IntBlockHash(0, blockFactory(), true)) {
            Seen seen = add(hash, keys);
            assertThat(seen.how, equalTo("runs"));
            assertThat(seen.groupIds, equalTo(new int[] { 1, 1, 1, 2, 2, 1, 1, 3, 3, 3 }));
            // a key that comes back later is a new run of the same group
            assertRuns(seen, new int[][] { { 0, 3, 1 }, { 3, 5, 2 }, { 5, 7, 1 }, { 7, 10, 3 } });
        }
    }

    public void testASingleRunPage() {
        int[] keys = new int[between(2, 500)];
        Arrays.fill(keys, randomInt());
        try (BlockHash hash = new IntBlockHash(0, blockFactory(), true)) {
            Seen seen = add(hash, keys);
            assertThat(seen.how, equalTo("runs"));
            assertThat(seen.runBounds.size(), equalTo(1));
            assertThat(seen.runBounds.get(0)[0], equalTo(0));
            assertThat(seen.runBounds.get(0)[1], equalTo(keys.length));
        }
    }

    /** Sorted or not, whichever path a page takes gives the ids of the plain hash, and so does a hash that sees several pages. */
    public void testSameGroupIdsAsThePlainHash() {
        try (BlockHash sorted = new IntBlockHash(0, blockFactory(), true); BlockHash plain = new IntBlockHash(0, blockFactory(), false)) {
            for (int page = 0; page < between(2, 6); page++) {
                int[] keys = new int[between(1, 3000)];
                boolean sortedKeys = randomBoolean();
                int key = randomIntBetween(0, 5);
                for (int i = 0; i < keys.length; i++) {
                    keys[i] = sortedKeys ? (key += randomInt(3) == 0 ? randomIntBetween(1, 5) : 0) : randomIntBetween(0, 40);
                }
                int[] viaSorted = ids(sorted, keys);
                int[] viaPlain = ids(plain, keys);
                assertThat("sorted keys=" + sortedKeys, viaSorted, equalTo(viaPlain));
            }
        }
    }

    /** Nearly unique keys make every run one row long: the page is hashed row by row, with the plain hash's result. */
    public void testNearlyUniqueKeysFallBack() {
        try (BlockHash sorted = new IntBlockHash(0, blockFactory(), true); BlockHash plain = new IntBlockHash(0, blockFactory(), false)) {
            int key = 0;
            for (int page = 0; page < 20; page++) {
                int[] keys = new int[8192];
                for (int i = 0; i < keys.length; i++) {
                    keys[i] = key += randomIntBetween(1, 2);
                }
                assertThat("page " + page, ids(sorted, keys), equalTo(ids(plain, keys)));
            }
            assertThat(sorted.toString(), containsString("sortedPages=0, fallbackPages=20"));
        }
    }

    /** With {@code positions / 2} runs the runs path still pays, with one more run the page is hashed row by row. */
    public void testTheThresholdIsAnAverageRunLengthOfTwo() {
        int positions = 100;
        // 50 runs of 2 rows
        int[] atTheLimit = new int[positions];
        for (int i = 0; i < positions; i++) {
            atTheLimit[i] = i / 2;
        }
        // 49 runs of 2 rows and 2 runs of 1 row: 51 runs
        int[] overTheLimit = new int[positions];
        for (int i = 0; i < 98; i++) {
            overTheLimit[i] = i / 2;
        }
        overTheLimit[98] = 1000;
        overTheLimit[99] = 1001;
        try (BlockHash hash = new IntBlockHash(0, blockFactory(), true)) {
            assertThat(add(hash, atTheLimit).how, equalTo("runs"));
        }
        try (BlockHash hash = new IntBlockHash(0, blockFactory(), true)) {
            assertThat(add(hash, overTheLimit).how, equalTo("vector"));
        }
    }

    /** A page of one row has no repeated key to gain from. */
    public void testAOneRowPageIsHashedRowByRow() {
        try (BlockHash hash = new IntBlockHash(0, blockFactory(), true)) {
            Seen seen = add(hash, new int[] { 42 });
            assertThat(seen.how, equalTo("vector"));
            assertThat(seen.groupIds, equalTo(new int[] { 1 }));
        }
    }

    /** The choice is made for every page on its own: a page of long runs after a unique one takes the runs path again. */
    public void testEveryPageChoosesOnItsOwn() {
        int[] unique = new int[1000];
        int[] runs = new int[1000];
        for (int i = 0; i < unique.length; i++) {
            unique[i] = i;
            runs[i] = i / 100;
        }
        try (BlockHash hash = new IntBlockHash(0, blockFactory(), true)) {
            assertThat(add(hash, unique).how, equalTo("vector"));
            assertThat(add(hash, runs).how, equalTo("runs"));
            assertThat(add(hash, unique).how, equalTo("vector"));
            assertThat(add(hash, runs).how, equalTo("runs"));
            assertThat(hash.toString(), containsString("sortedPages=2, fallbackPages=2"));
        }
    }

    /** A page with a null or a multivalue has ids that are not a vector, so it never takes the runs path. */
    public void testNullsAndMultivaluesFallBack() {
        try (BlockHash hash = new IntBlockHash(0, blockFactory(), true); IntBlock.Builder builder = blockFactory().newIntBlockBuilder(4)) {
            builder.appendInt(1);
            builder.appendNull();
            builder.beginPositionEntry().appendInt(1).appendInt(2).endPositionEntry();
            builder.appendInt(2);
            try (IntBlock block = builder.build()) {
                RecordingAddInput recorder = new RecordingAddInput();
                hash.add(new Page(block), recorder);
                assertThat(recorder.seen().how, not(equalTo("runs")));
            }
        }
    }

    public void testPlainHashNeverUsesRuns() {
        try (BlockHash hash = new IntBlockHash(0, blockFactory(), false)) {
            assertThat(add(hash, new int[] { 1, 1, 2, 2 }).how, equalTo("vector"));
        }
    }

    /** Only the int hash uses the hint: a long key built as sorted is hashed like any other. */
    public void testALongKeyIgnoresTheHint() {
        long[] keys = { 1, 1, 1, 1, 2, 2, 2, 2 };
        try (
            BlockHash hash = BlockHash.build(
                List.of(new BlockHash.GroupSpec(0, ElementType.LONG, null, null, true)),
                blockFactory(),
                8192,
                false
            );
            LongBlock block = blockFactory().newLongArrayVector(keys, keys.length).asBlock()
        ) {
            RecordingAddInput recorder = new RecordingAddInput();
            hash.add(new Page(block), recorder);
            assertThat(recorder.seen().how, equalTo("vector"));
            assertThat(hash.toString(), not(containsString("primarySorted")));
        }
    }

    public void testToStringSaysWhichPathWasTaken() {
        int[] keys = { 1, 1, 2, 2 };
        try (BlockHash hash = new IntBlockHash(0, blockFactory(), true)) {
            assertThat(hash.toString(), containsString("primarySorted=true, sortedPages=0, fallbackPages=0"));
            ids(hash, keys);
            ids(hash, keys);
            assertThat(hash.toString(), containsString("primarySorted=true, sortedPages=2, fallbackPages=0"));
        }
        try (BlockHash hash = new IntBlockHash(0, blockFactory(), false)) {
            ids(hash, keys);
            assertThat(hash.toString(), not(containsString("primarySorted")));
        }
    }

    private Seen add(BlockHash hash, int[] keys) {
        try (IntBlock block = blockFactory().newIntArrayVector(keys.clone(), keys.length).asBlock()) {
            RecordingAddInput recorder = new RecordingAddInput();
            hash.add(new Page(block), recorder);
            return recorder.seen();
        }
    }

    /** The group id of every key of a page, however the hash handed them over. */
    private int[] ids(BlockHash hash, int[] keys) {
        return add(hash, keys).groupIds;
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
