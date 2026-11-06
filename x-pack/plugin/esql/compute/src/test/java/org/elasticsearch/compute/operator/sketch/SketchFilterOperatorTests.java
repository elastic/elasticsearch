/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.sketch;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

/**
 * Drives several {@link SketchFilterOperator}s from one factory by hand, the way the drivers of one query would run them, and
 * checks that the filtered rows give the same top N groups, with the same counts, as counting all the rows.
 */
public class SketchFilterOperatorTests extends ESTestCase {
    private static final int TOP_N = 10;
    private final BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();

    private DriverContext driverContext() {
        return new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, blockFactory, null);
    }

    private Operator[] build(SketchFilterOperator.Factory factory, int drivers) {
        // All operators are built before any runs, as LocalExecutionPlan.createDrivers does.
        Operator[] ops = new Operator[drivers];
        for (int d = 0; d < drivers; d++) {
            ops[d] = factory.get(driverContext());
        }
        return ops;
    }

    private Page page(long[] keys) {
        return new Page(blockFactory.newLongArrayVector(keys, keys.length).asBlock());
    }

    /** Skewed data: 10 heavy keys with distinct large counts, and many keys that occur once or twice. */
    private long[] skewedRows(int noiseRows) {
        List<Long> rows = new ArrayList<>();
        for (int k = 0; k < TOP_N; k++) {
            int count = 20_000 + k * 1_000;
            for (int i = 0; i < count; i++) {
                rows.add(1_000_000L + k);
            }
        }
        for (int i = 0; i < noiseRows; i++) {
            rows.add((long) randomIntBetween(0, 400_000));
        }
        java.util.Collections.shuffle(rows, random());
        return rows.stream().mapToLong(Long::longValue).toArray();
    }

    /** Deal the rows to the drivers in pages, finish every operator, and return all rows that come out. */
    private List<Long> run(Operator[] ops, long[] rows, boolean expectFiltering, boolean expectPassthrough) {
        int pageSize = 512;
        List<Long> out = new ArrayList<>();
        for (int start = 0, d = 0; start < rows.length; start += pageSize, d = (d + 1) % ops.length) {
            long[] chunk = java.util.Arrays.copyOfRange(rows, start, Math.min(rows.length, start + pageSize));
            // As the Driver does: an operator that is letting pages through must be drained before it takes more input.
            while (ops[d].needsInput() == false) {
                collect(ops[d].getOutput(), out);
            }
            ops[d].addInput(page(chunk));
        }
        for (int d = 0; d < ops.length; d++) {
            ops[d].finish();
            if (d < ops.length - 1 && expectPassthrough == false) {
                // Not every driver is done: nothing may come out, and the driver must be blocked.
                assertFalse(ops[d].isBlocked().listener().isDone());
                assertNull(ops[d].getOutput());
                assertFalse(ops[d].isFinished());
            }
        }
        for (Operator op : ops) {
            assertTrue(op.isBlocked().listener().isDone());
            while (op.isFinished() == false) {
                Page p = op.getOutput();
                if (p == null) {
                    break;
                }
                collect(p, out);
            }
            assertTrue(op.isFinished());
        }
        if (expectFiltering) {
            assertThat("rows should have been dropped", out.size(), lessThan(rows.length));
        } else {
            assertThat(out.size(), equalTo(rows.length));
        }
        return out;
    }

    private static void collect(Page p, List<Long> out) {
        assertNotNull(p);
        LongBlock b = p.getBlock(0);
        for (int i = 0; i < p.getPositionCount(); i++) {
            out.add(b.getLong(b.getFirstValueIndex(i)));
        }
        p.releaseBlocks();
    }

    private static Map<Long, Long> counts(long[] rows) {
        Map<Long, Long> m = new HashMap<>();
        for (long r : rows) {
            m.merge(r, 1L, Long::sum);
        }
        return m;
    }

    private static Map<Long, Long> counts(List<Long> rows) {
        Map<Long, Long> m = new HashMap<>();
        for (long r : rows) {
            m.merge(r, 1L, Long::sum);
        }
        return m;
    }

    private static List<Long> topCounts(Map<Long, Long> counts) {
        return counts.values().stream().sorted(java.util.Comparator.reverseOrder()).limit(TOP_N).toList();
    }

    public void testSkewedDataIsFilteredAndTopNIsExact() {
        long[] rows = skewedRows(200_000);
        int drivers = randomIntBetween(2, 8);
        var factory = new SketchFilterOperator.Factory(0, 4096, TOP_N, 4096, Long.MAX_VALUE);
        Operator[] ops = build(factory, drivers);
        try {
            List<Long> out = run(ops, rows, true, false);
            Map<Long, Long> truth = counts(rows);
            Map<Long, Long> filtered = counts(out);
            assertThat(topCounts(filtered), equalTo(topCounts(truth)));
            // Every key that is left must have its exact count, because a key is either fully kept or fully dropped.
            for (var e : filtered.entrySet()) {
                assertThat(e.getValue(), equalTo(truth.get(e.getKey())));
            }
        } finally {
            org.elasticsearch.core.Releasables.close(ops);
        }
    }

    private static double field(String status, String name) {
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("[ \\[]" + name + "=(-?[0-9.]+)").matcher(status);
        assertTrue(name + " in " + status, m.find());
        return Double.parseDouble(m.group(1));
    }

    /** The timing in {@code toString} is what the profile shows, so it must be in the order the events happen in. */
    public void testTimingIsRecordedInOrder() {
        long[] rows = skewedRows(100_000);
        int drivers = 4;
        var factory = new SketchFilterOperator.Factory(0, 4096, TOP_N, 4096, Long.MAX_VALUE);
        Operator[] ops = build(factory, drivers);
        try {
            run(ops, rows, true, false);
            double maxArrived = 0;
            for (Operator op : ops) {
                String status = op.toString();
                double firstInput = field(status, "firstInputMs");
                double arrived = field(status, "arrivedMs");
                assertThat(status, firstInput >= 0, equalTo(true));
                assertThat(status, arrived >= firstInput, equalTo(true));
                assertThat(status, field(status, "sketchMs") > 0, equalTo(true));
                assertThat(status, field(status, "filterMs") >= 0, equalTo(true));
                maxArrived = Math.max(maxArrived, arrived);
            }
            for (Operator op : ops) {
                String status = op.toString();
                double decideStart = field(status, "decideStartMs");
                double decideEnd = field(status, "decideEndMs");
                // The proof starts when the last driver has arrived, and the same proof is shown on every operator.
                assertThat(status, decideStart >= maxArrived, equalTo(true));
                assertThat(status, decideEnd >= decideStart, equalTo(true));
                assertThat(status, decideStart, equalTo(field(ops[0].toString(), "decideStartMs")));
                if (field(status, "rowsEmitted") > 0) {
                    assertThat(status, field(status, "lastOutputMs") >= decideEnd, equalTo(true));
                }
                assertThat(status, field(status, "startEpochMs") > 0, equalTo(true));
            }
        } finally {
            org.elasticsearch.core.Releasables.close(ops);
        }
    }

    /** A Bloom filter has no false negatives, and it must not reject everything's neighbours either. */
    public void testPageBloomHasNoFalseNegatives() {
        for (int trial = 0; trial < 50; trial++) {
            int positions = randomFrom(1, 7, 100, 512, 20_480, 100_000);
            int shift = PageBloom.shiftFor(positions);
            long[] bloom = PageBloom.create(shift);
            long[] added = new long[between(1, positions)];
            for (int i = 0; i < added.length; i++) {
                added[i] = randomBoolean() ? randomLong() : randomIntBetween(-50, 50);
                PageBloom.add(bloom, shift, added[i]);
            }
            for (long key : added) {
                assertTrue(PageBloom.mayContain(bloom, shift, key));
            }
            // About 6 bits per row and two bits per key: other keys are rejected most of the time when the page is full.
            if (added.length >= positions / 2 && positions >= 512) {
                int falsePositives = 0;
                for (int i = 0; i < 2000; i++) {
                    long other = randomLong();
                    if (PageBloom.mayContain(bloom, shift, other)) {
                        falsePositives++;
                    }
                }
                assertThat("false positives of 2000", falsePositives, lessThan(300));
            }
        }
    }

    /**
     * Long runs of a heavy key span several pages, so the first row of a page often repeats the last key of the previous page of the
     * same driver. The count of the sketch skips the slow path for that row, but its key must still be in the Bloom filter of the
     * page, or a page with candidate rows could be skipped. The counts must stay exact, and pages without candidate rows must be skipped.
     */
    public void testPagesWithoutCandidatesAreSkippedAndCountsStayExact() {
        List<Long> rows = new ArrayList<>();
        for (int i = 0; i < 80_000; i++) {
            long key = randomLongBetween(0, 1_000_000);
            for (int r = randomIntBetween(1, 3); r > 0; r--) {
                rows.add(key);
            }
        }
        for (int heavy = 0; heavy < TOP_N; heavy++) {
            int position = randomIntBetween(0, rows.size());
            rows.addAll(position, java.util.Collections.nCopies(4_000 + heavy * 500, 2_000_000L + heavy));
        }
        long[] all = rows.stream().mapToLong(Long::longValue).toArray();
        int drivers = 4;
        var factory = new SketchFilterOperator.Factory(0, 4096, TOP_N, 4096, Long.MAX_VALUE);
        Operator[] ops = build(factory, drivers);
        try {
            List<Long> out = run(ops, all, true, false);
            Map<Long, Long> truth = counts(all);
            Map<Long, Long> filtered = counts(out);
            assertThat(topCounts(filtered), equalTo(topCounts(truth)));
            for (var e : filtered.entrySet()) {
                assertThat(e.getValue(), equalTo(truth.get(e.getKey())));
            }
            double skipped = 0;
            double buffered = 0;
            for (Operator op : ops) {
                skipped += field(op.toString(), "pagesSkipped");
                buffered += field(op.toString(), "pages");
            }
            assertThat("pages buffered", buffered, org.hamcrest.Matchers.greaterThan(100.0));
            assertThat("pages skipped", skipped, org.hamcrest.Matchers.greaterThan(buffered / 2));
        } finally {
            org.elasticsearch.core.Releasables.close(ops);
        }
    }

    /** Two different pairs of int values must never get the same packed key, also for negative and extreme values. */
    public void testPackedKeysOfIntPairsAreDistinct() {
        Map<Long, int[]> seen = new HashMap<>();
        int[] extremes = { Integer.MIN_VALUE, Integer.MIN_VALUE + 1, -1, 0, 1, Integer.MAX_VALUE - 1, Integer.MAX_VALUE };
        for (int a : extremes) {
            for (int b : extremes) {
                assertNull(seen.put(KeyColumns.pack(a, b), new int[] { a, b }));
            }
        }
        for (int i = 0; i < 50_000; i++) {
            int a = randomBoolean() ? randomInt() : randomIntBetween(-3, 3);
            int b = randomBoolean() ? randomInt() : randomIntBetween(-3, 3);
            int[] previous = seen.put(KeyColumns.pack(a, b), new int[] { a, b });
            if (previous != null) {
                assertThat(previous, equalTo(new int[] { a, b }));
            }
        }
    }

    /**
     * A key of two int columns (packed into a long) next to a value column, like {@code STATS COUNT(*), SUM(v) BY a, b}: the rows of
     * every key that is kept must all be kept, so that its count and its sum are exact, and most rows must be dropped.
     */
    public void testIntPairKeysWithAnotherColumnAreFilteredAndExact() {
        int drivers = 3;
        List<int[]> rows = new ArrayList<>(); // a, b, v
        for (int heavy = 0; heavy < TOP_N; heavy++) {
            for (int i = 0; i < 3_000 + heavy * 200; i++) {
                rows.add(new int[] { -5 - heavy, 100_000 + heavy, randomIntBetween(0, 50) });
            }
        }
        for (int i = 0; i < 150_000; i++) {
            rows.add(new int[] { randomIntBetween(-1000, 1000), randomInt(), randomIntBetween(0, 50) });
        }
        java.util.Collections.shuffle(rows, random());
        Map<Long, long[]> truth = new HashMap<>(); // count, sum
        for (int[] r : rows) {
            long[] t = truth.computeIfAbsent(KeyColumns.pack(r[0], r[1]), k -> new long[2]);
            t[0]++;
            t[1] += r[2];
        }
        var factory = new SketchFilterOperator.Factory(KeyColumns.intPairKey(0, 1), 4096, TOP_N, 4096, Long.MAX_VALUE);
        Operator[] ops = build(factory, drivers);
        try {
            int pageSize = 512;
            for (int start = 0, d = 0; start < rows.size(); start += pageSize, d = (d + 1) % drivers) {
                int n = Math.min(pageSize, rows.size() - start);
                int[] a = new int[n];
                int[] b = new int[n];
                long[] v = new long[n];
                for (int i = 0; i < n; i++) {
                    a[i] = rows.get(start + i)[0];
                    b[i] = rows.get(start + i)[1];
                    v[i] = rows.get(start + i)[2];
                }
                ops[d].addInput(
                    new Page(
                        blockFactory.newIntArrayVector(a, n).asBlock(),
                        blockFactory.newIntArrayVector(b, n).asBlock(),
                        blockFactory.newLongArrayVector(v, n).asBlock()
                    )
                );
            }
            for (Operator op : ops) {
                op.finish();
            }
            Map<Long, long[]> got = new HashMap<>();
            int rowsOut = 0;
            for (Operator op : ops) {
                assertTrue(op.isBlocked().listener().isDone());
                Page p;
                while ((p = op.getOutput()) != null) {
                    org.elasticsearch.compute.data.IntBlock a = p.getBlock(0);
                    org.elasticsearch.compute.data.IntBlock b = p.getBlock(1);
                    LongBlock v = p.getBlock(2);
                    for (int i = 0; i < p.getPositionCount(); i++) {
                        long[] t = got.computeIfAbsent(KeyColumns.pack(a.getInt(i), b.getInt(i)), k -> new long[2]);
                        t[0]++;
                        t[1] += v.getLong(i);
                        rowsOut++;
                    }
                    p.releaseBlocks();
                }
            }
            List<Long> truthTop = truth.values().stream().map(t -> t[0]).sorted(java.util.Comparator.reverseOrder()).limit(TOP_N).toList();
            List<Long> gotTop = got.values().stream().map(t -> t[0]).sorted(java.util.Comparator.reverseOrder()).limit(TOP_N).toList();
            assertThat(gotTop, equalTo(truthTop));
            for (var e : got.entrySet()) {
                assertThat("count of " + e.getKey(), e.getValue()[0], equalTo(truth.get(e.getKey())[0]));
                assertThat("sum of " + e.getKey(), e.getValue()[1], equalTo(truth.get(e.getKey())[1]));
            }
            assertThat("rows should have been dropped", rowsOut, lessThan(rows.size() / 2));
        } finally {
            org.elasticsearch.core.Releasables.close(ops);
        }
    }

    /** One int key column with a null in it: that driver gives up, the proof fails, and every row passes. */
    public void testNullInAnIntKeyMakesEveryDriverPassThrough() {
        var factory = new SketchFilterOperator.Factory(KeyColumns.intKey(0), 4096, TOP_N, 4096, Long.MAX_VALUE);
        Operator[] ops = build(factory, 2);
        try {
            int[] a = new int[20_000];
            for (int i = 0; i < a.length; i++) {
                a[i] = i % 4 == 0 ? randomIntBetween(0, 9) : randomInt();
            }
            ops[0].addInput(new Page(blockFactory.newIntArrayVector(a, a.length).asBlock()));
            try (var builder = blockFactory.newIntBlockBuilder(3)) {
                builder.appendInt(1).appendNull().appendInt(1);
                ops[1].addInput(new Page(builder.build()));
            }
            ops[0].finish();
            ops[1].finish();
            long rowsOut = 0;
            for (Operator op : ops) {
                assertTrue(op.isBlocked().listener().isDone());
                Page p;
                while ((p = op.getOutput()) != null) {
                    rowsOut += p.getPositionCount();
                    p.releaseBlocks();
                }
            }
            // The operator with the null key gave up, so the proof failed and every row passes.
            assertThat(rowsOut, equalTo(20_003L));
        } finally {
            org.elasticsearch.core.Releasables.close(ops);
        }
    }

    public void testUniformDataIsNotFiltered() {
        long[] rows = new long[50_000];
        for (int i = 0; i < rows.length; i++) {
            rows[i] = randomIntBetween(0, 20_000);
        }
        var factory = new SketchFilterOperator.Factory(0, 1024, TOP_N, 4096, Long.MAX_VALUE);
        Operator[] ops = build(factory, 4);
        try {
            List<Long> out = run(ops, rows, false, false);
            assertThat(counts(out), equalTo(counts(rows)));
        } finally {
            org.elasticsearch.core.Releasables.close(ops);
        }
    }

    public void testNullKeysPassThrough() {
        var factory = new SketchFilterOperator.Factory(0, 1024, TOP_N, 4096, Long.MAX_VALUE);
        Operator[] ops = build(factory, 2);
        try {
            try (var builder = blockFactory.newLongBlockBuilder(3)) {
                builder.appendLong(1).appendNull().appendLong(1);
                ops[0].addInput(new Page(builder.build()));
            }
            ops[1].addInput(page(new long[] { 2, 2, 3 }));
            ops[0].finish();
            ops[1].finish();
            List<Long> out = new ArrayList<>();
            for (Operator op : ops) {
                assertTrue(op.isBlocked().listener().isDone());
                Page p;
                while ((p = op.getOutput()) != null) {
                    out.add((long) p.getPositionCount());
                    p.releaseBlocks();
                }
            }
            assertThat(out.stream().mapToLong(Long::longValue).sum(), equalTo(6L));
        } finally {
            org.elasticsearch.core.Releasables.close(ops);
        }
    }

    /** One driver is closed before it finished: the others must not wait, and must not filter. */
    public void testClosedDriverReleasesTheOthersWithoutFiltering() {
        long[] rows = skewedRows(100_000);
        var factory = new SketchFilterOperator.Factory(0, 4096, TOP_N, 4096, Long.MAX_VALUE);
        Operator[] ops = build(factory, 3);
        try {
            ops[0].addInput(page(java.util.Arrays.copyOfRange(rows, 0, 1000)));
            ops[1].addInput(page(java.util.Arrays.copyOfRange(rows, 1000, 3000)));
            ops[1].finish();
            assertFalse(ops[1].isBlocked().listener().isDone());
            ops[2].finish();
            assertFalse(ops[1].isBlocked().listener().isDone());
            ops[0].close(); // the failed driver
            assertTrue(ops[1].isBlocked().listener().isDone());
            long out = 0;
            Page p;
            while ((p = ops[1].getOutput()) != null) {
                out += p.getPositionCount();
                p.releaseBlocks();
            }
            assertThat(out, equalTo(2000L));
        } finally {
            org.elasticsearch.core.Releasables.close(ops);
        }
    }

    public void testBufferLimitMakesEveryDriverPassThrough() {
        long[] rows = skewedRows(50_000);
        var factory = new SketchFilterOperator.Factory(0, 4096, TOP_N, 4096, 10_000);
        Operator[] ops = build(factory, 2);
        try {
            List<Long> out = run(ops, rows, false, true);
            assertThat(counts(out), equalTo(counts(rows)));
        } finally {
            org.elasticsearch.core.Releasables.close(ops);
        }
    }

    /** {@code addAll} must leave the sketch exactly as {@code add} would, also when a run of equal keys spans two calls. */
    public void testAddAllGivesTheSameSketchAsAdd() {
        List<Long> keys = new ArrayList<>();
        while (keys.size() < 50_000) {
            long key = randomBoolean() ? randomIntBetween(0, 50) : randomIntBetween(0, 20_000);
            for (int i = randomIntBetween(1, 6); i > 0; i--) {
                keys.add(key);
            }
        }
        CountSketch one = new CountSketch(256);
        CountSketch bulk = new CountSketch(256);
        long[] all = keys.stream().mapToLong(Long::longValue).toArray();
        for (long k : all) {
            one.add(k);
        }
        for (int start = 0; start < all.length;) {
            int length = Math.min(all.length - start, randomIntBetween(0, 700));
            bulk.addAll(java.util.Arrays.copyOfRange(all, start, start + length), length);
            start += length;
        }
        assertThat(tracked(bulk), equalTo(tracked(one)));
        for (int b = 0; b < one.buckets(); b++) {
            assertThat(bulk.cmin(b), equalTo(one.cmin(b)));
        }
    }

    /**
     * {@code insert} is written out for four slots. It must do what the rules say: a tracked key adds one; a new key takes the first
     * free slot; if the bucket is full, the new key takes the first slot with the smallest count, with {@code c} one above that
     * count and {@code e} equal to it. This test runs the rules in the simplest way, on slots in order, and compares the state.
     */
    public void testInsertFollowsTheRules() {
        int slots = randomFrom(8, 64, 256, 1024);
        CountSketch sketch = new CountSketch(slots);
        int buckets = sketch.buckets();
        long[][] key = new long[buckets][4];
        long[][] count = new long[buckets][4];
        long[][] error = new long[buckets][4];
        int keyRange = randomFrom(10, 100, 5_000, 100_000);
        for (int row = 0; row < 40_000; row++) {
            long k = randomBoolean() ? randomIntBetween(0, Math.min(keyRange, 20)) : randomIntBetween(0, keyRange);
            if (randomInt(4) == 0) {
                k = -k - 1; // negative and large keys too
            }
            sketch.add(k);
            int b = sketch.bucketOf(k);
            int slot = -1;
            for (int j = 0; j < 4 && slot < 0; j++) {
                if (count[b][j] != 0 && key[b][j] == k) {
                    slot = j;
                }
            }
            if (slot >= 0) {
                count[b][slot]++;
                continue;
            }
            for (int j = 0; j < 4 && slot < 0; j++) {
                if (count[b][j] == 0) {
                    slot = j;
                }
            }
            if (slot >= 0) {
                key[b][slot] = k;
                count[b][slot] = 1;
                error[b][slot] = 0;
                continue;
            }
            slot = 0;
            for (int j = 1; j < 4; j++) {
                if (count[b][j] < count[b][slot]) {
                    slot = j;
                }
            }
            error[b][slot] = count[b][slot];
            key[b][slot] = k;
            count[b][slot]++;
        }
        Map<Long, List<Long>> expected = new HashMap<>();
        for (int b = 0; b < buckets; b++) {
            for (int j = 0; j < 4; j++) {
                if (count[b][j] != 0) {
                    expected.put(key[b][j], List.of(count[b][j], error[b][j]));
                }
            }
        }
        assertThat(tracked(sketch), equalTo(expected));
    }

    private static Map<Long, List<Long>> tracked(CountSketch sketch) {
        Map<Long, List<Long>> m = new HashMap<>();
        sketch.forEachTracked((bucket, key, count, error) -> m.put(key, List.of(count, error)));
        return m;
    }

    /**
     * The proof must give the same decision and the same candidates as its definition: {@code lo(k)} is the sum of {@code c - e},
     * {@code hi(k)} adds the bucket bound for the drivers that do not track {@code k}, {@code L} is the N-th largest {@code lo},
     * and the bound for untracked keys is the largest bucket sum of {@code c_min}.
     */
    public void testProofMatchesItsDefinition() {
        int passes = 0;
        for (int trial = 0; trial < 120; trial++) {
            int drivers = randomBoolean() ? between(1, 6) : between(7, 19);
            int slots = randomFrom(64, 256, 1024, 4096);
            int topN = between(1, 10);
            CountSketch[] sketches = new CountSketch[drivers];
            for (int d = 0; d < drivers; d++) {
                sketches[d] = new CountSketch(slots);
            }
            int heavy = between(0, 15);
            long[] heavyCounts = new long[heavy];
            for (int h = 0; h < heavy; h++) {
                heavyCounts[h] = between(50, 3000);
            }
            int rows = between(1_000, 60_000);
            for (int i = 0; i < rows; i++) {
                long key;
                if (heavy > 0 && randomInt(3) == 0) {
                    key = 1_000_000L + randomIntBetween(0, heavy - 1);
                } else {
                    key = randomIntBetween(0, between(100, 30_000));
                }
                sketches[randomInt(drivers - 1)].add(key);
            }
            // reference
            int buckets = sketches[0].buckets();
            long bound = 0;
            for (int b = 0; b < buckets; b++) {
                long sum = 0;
                for (CountSketch s : sketches) {
                    sum += s.cmin(b);
                }
                bound = Math.max(bound, sum);
            }
            Map<Long, Long> lo = new HashMap<>();
            for (CountSketch s : sketches) {
                s.forEachTracked((bucket, key, count, error) -> lo.merge(key, count - error, Long::sum));
            }
            boolean expectedPass = false;
            java.util.Set<Long> expected = new java.util.HashSet<>();
            if (lo.size() >= topN) {
                long lowest = lo.values().stream().sorted(java.util.Comparator.reverseOrder()).skip(topN - 1).findFirst().get();
                expectedPass = lowest > bound;
                if (expectedPass) {
                    for (long key : lo.keySet()) {
                        long hi = 0;
                        for (CountSketch s : sketches) {
                            long meta = s.find(key);
                            hi += meta != 0 ? meta >>> 32 : s.cmin(s.bucketOf(key));
                        }
                        if (hi >= lowest) {
                            expected.add(key);
                        }
                    }
                }
            }
            SketchFilterBarrier barrier = new SketchFilterBarrier(topN, 1_000_000, Long.MAX_VALUE);
            for (int d = 0; d < drivers; d++) {
                barrier.register();
            }
            for (CountSketch s : sketches) {
                assertFalse(barrier.isDecided());
                barrier.arrive(s);
            }
            assertTrue(barrier.isDecided());
            assertThat("trial " + trial, barrier.filtering(), equalTo(expectedPass));
            if (expectedPass) {
                passes++;
                LongSet candidates = barrier.candidates();
                assertThat("trial " + trial, candidates.size(), equalTo(expected.size()));
                for (long key : expected) {
                    assertTrue("trial " + trial + " key " + key, candidates.contains(key));
                }
            }
        }
        assertThat("the test should cover passing proofs", passes, org.hamcrest.Matchers.greaterThan(3));
    }

    public void testSketchBoundsHoldForRandomInput() {
        int slots = 256;
        CountSketch sketch = new CountSketch(slots);
        Map<Long, Long> truth = new HashMap<>();
        for (int i = 0; i < 100_000; i++) {
            long key = randomBoolean() ? randomIntBetween(0, 20) : randomIntBetween(0, 50_000);
            sketch.add(key);
            truth.merge(key, 1L, Long::sum);
        }
        Map<Long, long[]> tracked = new HashMap<>();
        sketch.forEachTracked((bucket, key, count, error) -> tracked.put(key, new long[] { count, error }));
        for (var e : truth.entrySet()) {
            long[] t = tracked.get(e.getKey());
            if (t != null) {
                assertTrue(e.getValue() >= t[0] - t[1]);
                assertTrue(e.getValue() <= t[0]);
            } else {
                assertTrue(e.getValue() <= sketch.cmin(sketch.bucketOf(e.getKey())));
            }
        }
    }
}
