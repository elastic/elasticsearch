/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.spill.DriverSpillOperation;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.spill.MemorySpillStore;
import org.elasticsearch.core.Releasables;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.nullValue;

/**
 * The value store on its own, with segments that roll every few kilobytes: values appended across several segments
 * read back through gathers of any page size and order, and a compaction rewrites the live ones into one segment and
 * retires the rest.
 */
public class TopNValueStoreTests extends ComputeTestCase {

    public void testSegmentsRollAndTheGatherSpansThem() throws Exception {
        MemorySpillStore store = newStore();
        try {
            DriverContext ctx = spillingDriverContext(ByteSizeValue.ofBytes(1), 0, store, EsExecutors.DIRECT_EXECUTOR_SERVICE);
            TopNValueStore values = new TopNValueStore(ctx, ctx.queryScope(), "test-values", between(1, 8) * 1024L);
            TopNValueStore.Lane lane = values.newLane();
            List<TopNRow> rows = new ArrayList<>();
            List<byte[]> expected = new ArrayList<>();
            try {
                append(ctx, lane, rows, expected, between(100, 1_000));
                lane.sealForGood();
                assertTrue("the store completes inline", lane.settled());
                assertThat("the segments rolled", values.segments(), greaterThan(1));
                assertGathers(values, rows, expected);
            } finally {
                Releasables.close(rows);
                Releasables.close(lane, values);
            }
        } finally {
            closeSpillingScopes();
            shutdown(store);
        }
    }

    /**
     * Half the rows are evicted; the compaction copies the live values, across every segment, into a new one, after
     * which the gathers read the same values and only the new segment is left.
     */
    public void testCompactionRewritesTheLiveValues() throws Exception {
        MemorySpillStore store = newStore();
        try {
            DriverContext ctx = spillingDriverContext(ByteSizeValue.ofBytes(1), 0, store, EsExecutors.DIRECT_EXECUTOR_SERVICE);
            TopNValueStore values = new TopNValueStore(ctx, ctx.queryScope(), "test-values", between(1, 8) * 1024L);
            TopNValueStore.Lane lane = values.newLane();
            List<TopNRow> rows = new ArrayList<>();
            List<byte[]> expected = new ArrayList<>();
            try {
                append(ctx, lane, rows, expected, between(100, 1_000));
                int segmentsBefore = values.segments();
                assertThat(segmentsBefore, greaterThan(1));
                List<TopNRow> live = new ArrayList<>();
                List<byte[]> liveExpected = new ArrayList<>();
                List<TopNRow> evicted = new ArrayList<>();
                for (int i = 0; i < rows.size(); i++) {
                    if (randomBoolean()) {
                        live.add(rows.get(i));
                        liveExpected.add(expected.get(i));
                    } else {
                        evicted.add(rows.get(i));
                    }
                }
                assertTrue(lane.quiescent());
                lane.compact(live);
                assertTrue("the compaction completed inline", lane.compacting() == false || lane.finishCompaction());
                assertThat(values.compactions(), equalTo(1));
                assertThat(values.compactionsFailed(), equalTo(0));
                int target = values.segments() - 1;
                for (TopNRow row : live) {
                    assertThat("the row points at the new segment", row.valueSegment, equalTo(target));
                }
                lane.sealForGood();
                assertTrue(lane.settled());
                assertGathers(values, live, liveExpected);
                Releasables.close(evicted);
                rows.clear();
                rows.addAll(live);
            } finally {
                Releasables.close(rows);
                Releasables.close(lane, values);
            }
        } finally {
            closeSpillingScopes();
            shutdown(store);
        }
    }

    /**
     * A row evicted while its append is still on the codec's thread is reused and appended to the next segment; the
     * earlier append's completion lands after the later one's and must leave the row's offset alone, since the row
     * points into the later segment. The codec's drains are queued and run by hand, in that order.
     */
    public void testACompletionLandingAfterTheRowWasReusedAcrossARollIsIgnored() throws Exception {
        MemorySpillStore store = newStore();
        try {
            ArrayDeque<Runnable> codec = new ArrayDeque<>();
            DriverContext ctx = spillingDriverContext(ByteSizeValue.ofBytes(1), 0, store, codec::add);
            TopNValueStore values = new TopNValueStore(ctx, ctx.queryScope(), "test-values", 1024L);
            TopNValueStore.Lane lane = values.newLane();
            TopNRow row = new TopNRow(ctx.breaker(), 0, 600);
            TopNRow firstFiller = new TopNRow(ctx.breaker(), 0, 600);
            TopNRow secondFiller = new TopNRow(ctx.breaker(), 0, 600);
            try {
                lane.acquire();
                assertTrue("the writer lands inline", lane.ready());
                append(lane, row, randomByteArrayOfLength(600));
                append(lane, firstFiller, randomByteArrayOfLength(600));
                assertThat("one drain is queued for the first segment", codec.size(), equalTo(1));
                Runnable firstDrain = codec.removeFirst();
                assertTrue("the first segment is full", lane.needsRoll());
                lane.roll();
                assertTrue("the next writer lands inline", lane.ready());
                // the row is evicted while its append is in flight and reused for a row of the next segment
                row.clear();
                row.ensureValues(600);
                byte[] expected = randomByteArrayOfLength(600);
                append(lane, secondFiller, randomByteArrayOfLength(600));
                append(lane, row, expected);
                assertThat("one drain is queued for the second segment", codec.size(), equalTo(1));
                int segment = row.valueSegment;
                // the second segment's drain lands first, then the first segment's, with the row's stale completion
                codec.removeFirst().run();
                long offset = row.valueOffset;
                firstDrain.run();
                assertThat(row.valueSegment, equalTo(segment));
                assertThat("the stale completion left the offset alone", row.valueOffset, equalTo(offset));
                lane.sealForGood();
                while (codec.isEmpty() == false) {
                    codec.removeFirst().run();
                }
                assertTrue(lane.settled());
                DriverSpillOperation<ReleasableBytesReference[]> gather = values.gather(List.of(row), 0, 1);
                assertTrue("the gather completes inline", gather.isDone());
                assertThat(gather.failure(), nullValue());
                ReleasableBytesReference[] got = gather.result();
                try {
                    assertArrayEquals(expected, BytesReference.toBytes(got[0]));
                } finally {
                    Releasables.close(got);
                }
            } finally {
                Releasables.close(row, firstFiller, secondFiller);
                Releasables.close(lane, values);
            }
        } finally {
            closeSpillingScopes();
            shutdown(store);
        }
    }

    private static void append(TopNValueStore.Lane lane, TopNRow row, byte[] bytes) {
        row.values.append(bytes, 0, bytes.length);
        lane.append(row, row.values);
        assertThat(row.values, nullValue());
        assertThat(row.valueLength, equalTo(bytes.length));
    }

    private static void append(DriverContext ctx, TopNValueStore.Lane lane, List<TopNRow> rows, List<byte[]> expected, int count) {
        lane.acquire();
        assertTrue("the writer lands inline", lane.ready());
        for (int i = 0; i < count; i++) {
            if (lane.needsRoll()) {
                lane.roll();
                assertTrue("the next writer lands inline", lane.ready());
            }
            byte[] bytes = randomByteArrayOfLength(between(1, 600));
            TopNRow row = new TopNRow(ctx.breaker(), 0, bytes.length);
            row.values.append(bytes, 0, bytes.length);
            lane.append(row, row.values);
            assertThat(row.values, nullValue());
            assertThat(row.valueLength, equalTo(bytes.length));
            rows.add(row);
            expected.add(bytes);
        }
        assertTrue("every append landed inline", lane.quiescent());
    }

    /**
     * Gathers the rows in a shuffled order, as a heap pops them, a page at a time, and checks each value.
     */
    private static void assertGathers(TopNValueStore values, List<TopNRow> rows, List<byte[]> expected) {
        List<Integer> order = new ArrayList<>();
        for (int i = 0; i < rows.size(); i++) {
            order.add(i);
        }
        Collections.shuffle(order, random());
        List<TopNRow> shuffledRows = new ArrayList<>();
        List<byte[]> shuffledExpected = new ArrayList<>();
        for (int i : order) {
            shuffledRows.add(rows.get(i));
            shuffledExpected.add(expected.get(i));
        }
        int pageSize = between(1, 50);
        for (int from = 0; from < shuffledRows.size(); from += pageSize) {
            int to = Math.min(shuffledRows.size(), from + pageSize);
            DriverSpillOperation<ReleasableBytesReference[]> gather = values.gather(shuffledRows, from, to);
            assertTrue("the gather completes inline", gather.isDone());
            assertThat(gather.failure(), nullValue());
            ReleasableBytesReference[] got = gather.result();
            try {
                assertThat(got.length, equalTo(to - from));
                for (int i = 0; i < got.length; i++) {
                    assertArrayEquals("value " + (from + i), shuffledExpected.get(from + i), BytesReference.toBytes(got[i]));
                }
            } finally {
                Releasables.close(got);
            }
        }
        assertThat(values.status().readBackBytes(), greaterThan(0L));
    }

    private static MemorySpillStore newStore() {
        return new MemorySpillStore(MemorySpillStore.Completion.INLINE, null, Long.MAX_VALUE, "test.spill", new Random(randomLong()));
    }

    private static void shutdown(MemorySpillStore store) throws Exception {
        PlainActionFuture<Void> shutdown = new PlainActionFuture<>();
        store.shutdown(shutdown);
        shutdown.get(30, TimeUnit.SECONDS);
    }
}
