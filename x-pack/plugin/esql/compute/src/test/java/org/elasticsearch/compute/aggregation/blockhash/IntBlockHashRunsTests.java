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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * The int hash finds the runs of equal adjacent keys and hands them to the aggregators as {@link GroupRuns}, unless the page has
 * too few repeated keys to gain from it. The runs are only a statement about the shape of the group ids, so whatever the order
 * of the keys, expanding the runs must give exactly the group ids that hashing row by row gives.
 */
public class IntBlockHashRunsTests extends ComputeTestCase {

    /** What a hash handed to its {@link GroupingAggregatorFunction.AddInput}, for one page. */
    private record Seen(String how, int[] groupIds, List<int[]> runBounds) {}

    public void testRunsHaveTheBoundariesOfTheKeys() {
        int[] keys = new int[19];
        int[] lengths = { 4, 5, 4, 6 };
        int[] values = { 5, 7, 5, 9 };
        for (int run = 0, p = 0; run < lengths.length; run++) {
            for (int i = 0; i < lengths[run]; i++) {
                keys[p++] = values[run];
            }
        }
        try (BlockHash hash = new IntBlockHash(0, blockFactory())) {
            Seen seen = add(hash, keys);
            assertThat(seen.how, equalTo("runs"));
            assertThat(seen.groupIds, equalTo(new int[] { 1, 1, 1, 1, 2, 2, 2, 2, 2, 1, 1, 1, 1, 3, 3, 3, 3, 3, 3 }));
            // a key that comes back later is a new run of the same group
            assertRuns(seen, new int[][] { { 0, 4, 1 }, { 4, 9, 2 }, { 9, 13, 1 }, { 13, 19, 3 } });
        }
    }

    public void testASingleRunPage() {
        int[] keys = new int[between(4, 500)];
        Arrays.fill(keys, randomInt());
        try (BlockHash hash = new IntBlockHash(0, blockFactory())) {
            Seen seen = add(hash, keys);
            assertThat(seen.how, equalTo("runs"));
            assertThat(seen.runBounds.size(), equalTo(1));
            assertThat(seen.runBounds.get(0)[0], equalTo(0));
            assertThat(seen.runBounds.get(0)[1], equalTo(keys.length));
        }
    }

    /**
     * Whichever path a page takes gives the ids of first appearance, and so does a hash that sees several pages, with
     * keys that are in long runs, in short runs or in no order at all.
     */
    public void testGroupIdsAreTheOrderOfFirstAppearance() {
        Map<Integer, Integer> expected = new HashMap<>();
        try (BlockHash hash = new IntBlockHash(0, blockFactory())) {
            for (int page = 0; page < between(2, 6); page++) {
                int[] keys = new int[between(1, 3000)];
                int shape = between(0, 2);
                int key = randomIntBetween(0, 5);
                for (int i = 0; i < keys.length; i++) {
                    keys[i] = switch (shape) {
                        case 0 -> (key += randomInt(30) == 0 ? randomIntBetween(1, 5) : 0);
                        case 1 -> (key += randomInt(3) == 0 ? randomIntBetween(1, 5) : 0);
                        case 2 -> randomIntBetween(0, 40);
                        default -> throw new AssertionError();
                    };
                }
                int[] expectedIds = new int[keys.length];
                for (int i = 0; i < keys.length; i++) {
                    expectedIds[i] = expected.computeIfAbsent(keys[i], k -> expected.size() + 1);
                }
                assertThat("shape " + shape, ids(hash, keys), equalTo(expectedIds));
            }
        }
    }

    /** Unique keys make every run one row long: the page is hashed row by row, and the scan stops after the probe. */
    public void testUniqueKeysAreHashedRowByRow() {
        try (BlockHash hash = new IntBlockHash(0, blockFactory())) {
            int key = 0;
            for (int page = 0; page < 20; page++) {
                int[] keys = new int[8192];
                int[] expected = new int[keys.length];
                for (int i = 0; i < keys.length; i++) {
                    keys[i] = key += randomIntBetween(1, 2);
                    expected[i] = page * keys.length + i + 1;
                }
                assertThat("page " + page, ids(hash, keys), equalTo(expected));
            }
            assertThat(hash.toString(), containsString("runPages=0, rowByRowPages=20"));
        }
    }

    /** With {@code positions / 4} runs the runs path still pays, with one more run the page is hashed row by row. */
    public void testTheThresholdIsAnAverageRunLengthOfFour() {
        int positions = 100;
        // 25 runs of 4 rows
        int[] atTheLimit = new int[positions];
        for (int i = 0; i < positions; i++) {
            atTheLimit[i] = i / 4;
        }
        // 24 runs of 4 rows and 4 runs of 1 row: 28 runs
        int[] overTheLimit = new int[positions];
        for (int i = 0; i < 96; i++) {
            overTheLimit[i] = i / 4;
        }
        for (int i = 96; i < positions; i++) {
            overTheLimit[i] = 1000 + i;
        }
        try (BlockHash hash = new IntBlockHash(0, blockFactory())) {
            assertThat(add(hash, atTheLimit).how, equalTo("runs"));
        }
        try (BlockHash hash = new IntBlockHash(0, blockFactory())) {
            assertThat(add(hash, overTheLimit).how, equalTo("vector"));
        }
    }

    /** A few short runs at the start of a page do not decide for the page: the average is only checked after the probe rows. */
    public void testShortRunsAtTheStartDoNotDecide() {
        int[] keys = new int[10 + 10 * 1000];
        for (int i = 0; i < 10; i++) {
            keys[i] = i;
        }
        for (int i = 10; i < keys.length; i++) {
            keys[i] = 100 + (i - 10) / 1000;
        }
        try (BlockHash hash = new IntBlockHash(0, blockFactory())) {
            assertThat(add(hash, keys).how, equalTo("runs"));
        }
    }

    /** A sparse start shorter than the probe does not decide for the page: unique keys, then one long run, still take the runs path. */
    public void testASparseStartShorterThanTheProbeDoesNotDecide() {
        int[] keys = new int[8192];
        for (int i = 0; i < 200; i++) {
            keys[i] = i;
        }
        Arrays.fill(keys, 200, keys.length, 5000);
        try (BlockHash hash = new IntBlockHash(0, blockFactory())) {
            Seen seen = add(hash, keys);
            assertThat(seen.how, equalTo("runs"));
            assertThat(seen.runBounds.size(), equalTo(201));
        }
    }

    /** A sparse start longer than the probe decides: the page is hashed row by row, and nothing but the speedup is lost. */
    public void testASparseStartLongerThanTheProbeIsHashedRowByRow() {
        int[] keys = new int[8192];
        for (int i = 0; i < 300; i++) {
            keys[i] = i;
        }
        Arrays.fill(keys, 300, keys.length, 5000);
        int[] expected = new int[keys.length];
        for (int i = 0; i < 300; i++) {
            expected[i] = i + 1;
        }
        Arrays.fill(expected, 300, keys.length, 301);
        try (BlockHash hash = new IntBlockHash(0, blockFactory())) {
            Seen seen = add(hash, keys);
            assertThat(seen.how, equalTo("vector"));
            assertThat(seen.groupIds, equalTo(expected));
        }
    }

    /** A page of one row has no repeated key to gain from. */
    public void testAOneRowPageIsHashedRowByRow() {
        try (BlockHash hash = new IntBlockHash(0, blockFactory())) {
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
        try (BlockHash hash = new IntBlockHash(0, blockFactory())) {
            assertThat(add(hash, unique).how, equalTo("vector"));
            assertThat(add(hash, runs).how, equalTo("runs"));
            assertThat(add(hash, unique).how, equalTo("vector"));
            assertThat(add(hash, runs).how, equalTo("runs"));
            assertThat(hash.toString(), containsString("runPages=2, rowByRowPages=2"));
        }
    }

    /** A page with a null or a multivalue has ids that are not a vector, so it never takes the runs path. */
    public void testNullsAndMultivaluesFallBack() {
        try (BlockHash hash = new IntBlockHash(0, blockFactory()); IntBlock.Builder builder = blockFactory().newIntBlockBuilder(4)) {
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

    /** Only the int hash looks for runs: a long key with long runs is hashed like any other. */
    public void testALongKeyIsHashedRowByRow() {
        long[] keys = new long[64];
        Arrays.fill(keys, 0, 32, 1L);
        Arrays.fill(keys, 32, 64, 2L);
        try (
            BlockHash hash = BlockHash.build(List.of(new BlockHash.GroupSpec(0, ElementType.LONG)), blockFactory(), 8192, false);
            LongBlock block = blockFactory().newLongArrayVector(keys, keys.length).asBlock()
        ) {
            RecordingAddInput recorder = new RecordingAddInput();
            hash.add(new Page(block), recorder);
            assertThat(recorder.seen().how, equalTo("vector"));
            assertThat(hash.toString(), not(containsString("runPages")));
        }
    }

    public void testToStringSaysWhichPathWasTaken() {
        int[] keys = new int[16];
        Arrays.fill(keys, 0, 8, 1);
        Arrays.fill(keys, 8, 16, 2);
        try (BlockHash hash = new IntBlockHash(0, blockFactory())) {
            assertThat(hash.toString(), not(containsString("runPages")));
            ids(hash, keys);
            ids(hash, keys);
            assertThat(hash.toString(), containsString("runPages=2, rowByRowPages=0"));
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
