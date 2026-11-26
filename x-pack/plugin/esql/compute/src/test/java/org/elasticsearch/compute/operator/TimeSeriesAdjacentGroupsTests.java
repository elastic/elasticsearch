/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.blockhash.TimeSeriesBlockHash;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests {@link TimeSeriesAdjacentGroups}: for every group of a {@link TimeSeriesBlockHash} the previous/next group must be
 * the group of the same tsid with the closest smaller/larger timestamp, regardless of gaps between buckets, of the number
 * of tsids, or of the order in which the groups were added to the hash.
 */
public class TimeSeriesAdjacentGroupsTests extends ComputeTestCase {

    private static final long BUCKET = 60_000;

    /** A {@code (tsid, timestamp)} pair fed into the hash; the timestamp is already rounded to its bucket. */
    private record Sample(String tsid, long timestamp) {}

    /**
     * With a gap between the populated buckets at {@code 1m} and {@code 3m}, the neighbours must skip the empty {@code 2m}
     * bucket and link {@code 1m <-> 3m} directly.
     */
    public void testSkipsEmptyBuckets() {
        BlockFactory blockFactory = blockFactory();
        List<Sample> samples = List.of(new Sample("a", 3 * BUCKET), new Sample("a", BUCKET), new Sample("a", 0));
        try (var hash = new TimeSeriesBlockHash(0, 1, false, randomBoolean(), blockFactory)) {
            int[] groups = addSamples(hash, blockFactory, samples, samples.size());
            int at3m = groups[0];
            int at1m = groups[1];
            int at0m = groups[2];
            try (var adjacent = TimeSeriesAdjacentGroups.compute(hash, blockFactory.bigArrays())) {
                assertThat(adjacent.previousGroupId(at0m), equalTo(-1));
                assertThat(adjacent.nextGroupId(at0m), equalTo(at1m));

                assertThat(adjacent.previousGroupId(at1m), equalTo(at0m));
                assertThat("the empty 2m bucket must be skipped", adjacent.nextGroupId(at1m), equalTo(at3m));

                assertThat("the empty 2m bucket must be skipped", adjacent.previousGroupId(at3m), equalTo(at1m));
                assertThat(adjacent.nextGroupId(at3m), equalTo(-1));
            }
        }
    }

    /**
     * Groups of different tsids must never be linked, even when their timestamps interleave and one tsid fills exactly the
     * buckets the other one leaves empty.
     */
    public void testLinksStayWithinTsid() {
        BlockFactory blockFactory = blockFactory();
        List<Sample> samples = List.of(
            new Sample("a", 4 * BUCKET),
            new Sample("a", 2 * BUCKET),
            new Sample("a", 0),
            new Sample("b", 3 * BUCKET),
            new Sample("b", BUCKET)
        );
        try (var hash = new TimeSeriesBlockHash(0, 1, false, randomBoolean(), blockFactory)) {
            int[] groups = addSamples(hash, blockFactory, samples, samples.size());
            try (var adjacent = TimeSeriesAdjacentGroups.compute(hash, blockFactory.bigArrays())) {
                // tsid a: 0 <-> 2m <-> 4m
                assertThat(adjacent.previousGroupId(groups[2]), equalTo(-1));
                assertThat(adjacent.nextGroupId(groups[2]), equalTo(groups[1]));
                assertThat(adjacent.previousGroupId(groups[1]), equalTo(groups[2]));
                assertThat(adjacent.nextGroupId(groups[1]), equalTo(groups[0]));
                assertThat(adjacent.previousGroupId(groups[0]), equalTo(groups[1]));
                assertThat(adjacent.nextGroupId(groups[0]), equalTo(-1));
                // tsid b: 1m <-> 3m
                assertThat(adjacent.previousGroupId(groups[4]), equalTo(-1));
                assertThat(adjacent.nextGroupId(groups[4]), equalTo(groups[3]));
                assertThat(adjacent.previousGroupId(groups[3]), equalTo(groups[4]));
                assertThat(adjacent.nextGroupId(groups[3]), equalTo(-1));
            }
        }
    }

    public void testSingleGroupPerTsidHasNoNeighbours() {
        BlockFactory blockFactory = blockFactory();
        List<Sample> samples = new ArrayList<>();
        int numTsids = between(1, 10);
        for (int t = 0; t < numTsids; t++) {
            samples.add(new Sample("tsid-" + t, randomLongBetween(0, 100) * BUCKET));
        }
        try (var hash = new TimeSeriesBlockHash(0, 1, false, randomBoolean(), blockFactory)) {
            int[] groups = addSamples(hash, blockFactory, samples, between(1, samples.size()));
            try (var adjacent = TimeSeriesAdjacentGroups.compute(hash, blockFactory.bigArrays())) {
                for (int group : groups) {
                    assertThat(adjacent.previousGroupId(group), equalTo(-1));
                    assertThat(adjacent.nextGroupId(group), equalTo(-1));
                }
            }
        }
    }

    public void testEmptyHash() {
        BlockFactory blockFactory = blockFactory();
        try (
            var hash = new TimeSeriesBlockHash(0, 1, false, randomBoolean(), blockFactory);
            var adjacent = TimeSeriesAdjacentGroups.compute(hash, blockFactory.bigArrays())
        ) {
            assertThat(hash.numGroups(), equalTo(0L));
        }
    }

    /**
     * Groups fed in the order a time-series source produces them (tsid ascending, timestamp descending), the case the sort
     * is optimized for. Verified against a brute-force reference.
     */
    public void testTimeSeriesOrderedInput() {
        List<Sample> samples = randomSamples();
        samples.sort(Comparator.comparing(Sample::tsid).thenComparing(Comparator.comparingLong(Sample::timestamp).reversed()));
        assertMatchesBruteForce(samples);
    }

    /**
     * Groups fed in arbitrary order, e.g. when the final aggregation on the coordinator receives interleaved chunks from
     * several data nodes. Verified against a brute-force reference.
     */
    public void testShuffledInput() {
        List<Sample> samples = randomSamples();
        Collections.shuffle(samples, random());
        assertMatchesBruteForce(samples);
    }

    /**
     * A single tsid with many buckets so the per-tsid scratch array is large, next to tsids with a single bucket.
     */
    public void testSkewedGroupsPerTsid() {
        List<Sample> samples = new ArrayList<>();
        int numBuckets = between(100, 500);
        for (int i = 0; i < numBuckets; i++) {
            samples.add(new Sample("big", i * BUCKET));
        }
        int numSmall = between(1, 20);
        for (int t = 0; t < numSmall; t++) {
            samples.add(new Sample("small-" + t, randomLongBetween(0, numBuckets) * BUCKET));
        }
        if (randomBoolean()) {
            Collections.shuffle(samples, random());
        }
        assertMatchesBruteForce(samples);
    }

    /**
     * The tracked arrays must be released when allocation trips the circuit breaker half way through the computation.
     */
    public void testReleasesArraysOnCircuitBreak() {
        List<Sample> samples = randomSamples();
        Collections.shuffle(samples, random());
        testWithCrankyBlockFactory(blockFactory -> {
            try (var hash = new TimeSeriesBlockHash(0, 1, false, randomBoolean(), blockFactory)) {
                addSamples(hash, blockFactory, samples, between(1, samples.size()));
                try (var adjacent = TimeSeriesAdjacentGroups.compute(hash, blockFactory.bigArrays())) {
                    assertMatchesBruteForce(hash, adjacent);
                }
            }
        });
    }

    /**
     * Random tsids, each with a random set of distinct buckets (with gaps) and occasionally repeated samples of the same
     * bucket, which the hash collapses into one group.
     */
    private List<Sample> randomSamples() {
        List<Sample> samples = new ArrayList<>();
        int numTsids = between(1, 20);
        for (int t = 0; t < numTsids; t++) {
            String tsid = "tsid-" + t;
            int numBuckets = between(1, 40);
            Set<Long> buckets = new HashSet<>();
            while (buckets.size() < numBuckets) {
                buckets.add(randomLongBetween(0, 200) * BUCKET);
            }
            for (long bucket : buckets) {
                int repeats = randomBoolean() ? 1 : between(2, 3);
                for (int r = 0; r < repeats; r++) {
                    samples.add(new Sample(tsid, bucket));
                }
            }
        }
        return samples;
    }

    private void assertMatchesBruteForce(List<Sample> samples) {
        BlockFactory blockFactory = blockFactory();
        try (var hash = new TimeSeriesBlockHash(0, 1, false, randomBoolean(), blockFactory)) {
            addSamples(hash, blockFactory, samples, between(1, samples.size()));
            try (var adjacent = TimeSeriesAdjacentGroups.compute(hash, blockFactory.bigArrays())) {
                assertMatchesBruteForce(hash, adjacent);
            }
        }
    }

    /**
     * Compares against an {@code O(numGroups^2)} reference: the previous (next) group is the group of the same tsid with the
     * largest timestamp below (smallest timestamp above) the current one.
     */
    private static void assertMatchesBruteForce(TimeSeriesBlockHash hash, TimeSeriesAdjacentGroups adjacent) {
        int numGroups = Math.toIntExact(hash.numGroups());
        for (int group = 0; group < numGroups; group++) {
            int tsid = hash.tsidForGroup(group);
            long timestamp = hash.timestampForGroup(group);
            int expectedPrevious = -1;
            int expectedNext = -1;
            for (int other = 0; other < numGroups; other++) {
                if (other == group || hash.tsidForGroup(other) != tsid) {
                    continue;
                }
                long otherTimestamp = hash.timestampForGroup(other);
                if (otherTimestamp < timestamp && (expectedPrevious == -1 || otherTimestamp > hash.timestampForGroup(expectedPrevious))) {
                    expectedPrevious = other;
                }
                if (otherTimestamp > timestamp && (expectedNext == -1 || otherTimestamp < hash.timestampForGroup(expectedNext))) {
                    expectedNext = other;
                }
            }
            assertThat("previous group of group " + group, adjacent.previousGroupId(group), equalTo(expectedPrevious));
            assertThat("next group of group " + group, adjacent.nextGroupId(group), equalTo(expectedNext));
        }
    }

    /**
     * Adds {@code samples} to {@code hash} in pages of {@code pageSize} rows and returns the group id assigned to each
     * sample, aligned with {@code samples}.
     */
    private static int[] addSamples(TimeSeriesBlockHash hash, BlockFactory blockFactory, List<Sample> samples, int pageSize) {
        int[] groupIds = new int[samples.size()];
        for (int from = 0; from < samples.size(); from += pageSize) {
            int to = Math.min(from + pageSize, samples.size());
            final int pageStart = from;
            BytesRefVector tsids = null;
            LongVector timestamps = null;
            Page page = null;
            try (
                var tsidBuilder = blockFactory.newBytesRefVectorBuilder(to - from);
                var timestampBuilder = blockFactory.newLongVectorBuilder(to - from)
            ) {
                for (int i = from; i < to; i++) {
                    tsidBuilder.appendBytesRef(new BytesRef(samples.get(i).tsid()));
                    timestampBuilder.appendLong(samples.get(i).timestamp());
                }
                tsids = tsidBuilder.build();
                timestamps = timestampBuilder.build();
                page = new Page(tsids.asBlock(), timestamps.asBlock());
                hash.add(page, new GroupingAggregatorFunction.AddInput() {
                    @Override
                    public void add(int positionOffset, IntArrayBlock groups) {
                        throw new AssertionError("time-series block hash should emit a vector");
                    }

                    @Override
                    public void add(int positionOffset, IntBigArrayBlock groups) {
                        throw new AssertionError("time-series block hash should emit a vector");
                    }

                    @Override
                    public void add(int positionOffset, IntVector groups) {
                        for (int p = 0; p < groups.getPositionCount(); p++) {
                            groupIds[pageStart + positionOffset + p] = groups.getInt(p);
                        }
                    }

                    @Override
                    public void close() {}
                });
            } finally {
                if (page != null) {
                    page.releaseBlocks();
                } else {
                    Releasables.close(tsids, timestamps);
                }
            }
        }
        return groupIds;
    }
}
