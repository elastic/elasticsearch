/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.timeseries;

import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.compute.lucene.query.TsidPrefixRange;
import org.elasticsearch.index.codec.tsdb.PartitionedDocValues.PrefixPartitions;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class TsidPrefixHistogramTests extends ESTestCase {

    public void testLeafWithoutFilter() throws IOException {
        var partitions = new PrefixPartitions(3, new int[] { 0, 2, 3 }, new int[] { 0, 100, 300 });
        var histogram = allDocs(partitions, 450, 1000, 1999, 0);
        assertThat(
            histogram,
            equalTo(new TsidPrefixHistogram(10, new int[] { 0, 2, 3 }, new double[] { 10_000, 20_000, 15_000 }, 3, 1000, 1999, 450))
        );
        assertThat(histogram.estimatedSeries(), equalTo(45L));
    }

    public void testLeafWithFilter() throws IOException {
        var partitions = new PrefixPartitions(4, new int[] { 0, 2, 3, 7 }, new int[] { 0, 100, 300, 400 });
        FixedBitSet matching = new FixedBitSet(450);
        matching.set(120);
        matching.set(130);
        matching.set(299);
        matching.set(449);
        var histogram = leaf(partitions, 450, new BitSetIterator(matching, 4), 1000, 1999, 0);
        assertThat(histogram, equalTo(new TsidPrefixHistogram(10, new int[] { 2, 7 }, new double[] { 20_000, 5_000 }, 2, 1000, 1999, 4)));

        var none = leaf(partitions, 450, new BitSetIterator(new FixedBitSet(450), 0), 1000, 1999, 0);
        assertThat(none.totalPrefixes(), equalTo(0));
    }

    public void testGroupsCappedByDocs() throws IOException {
        var partitions = new PrefixPartitions(3, new int[] { 0, 2, 3 }, new int[] { 0, 100, 300 });
        // 45 series over 1000ms in 25ms buckets would be 1800 groups, but 450 docs hold at most 450
        var dense = allDocs(partitions, 450, 1000, 1999, 25);
        assertThat(
            dense,
            equalTo(new TsidPrefixHistogram(10, new int[] { 0, 2, 3 }, new double[] { 2_500, 5_000, 3_750 }, 3, 1000, 1999, 450))
        );
        assertThat(dense.estimatedGroups(25), equalTo(450L));
        var merged = TsidPrefixHistogram.merge(dense, dense);
        var copy = copyWriteable(merged, new NamedWriteableRegistry(List.of()), TsidPrefixHistogram::new);
        assertThat(copy, equalTo(merged));
        assertThat(copy.hashCode(), equalTo(merged.hashCode()));
        assertThat(copy.capped(1).estimatedGroups(25), equalTo(900L));
    }

    public void testGroupsCoverMisalignedBuckets() throws IOException {
        // 100 series over 19001ms starting 1ms before a 5s boundary touch five 5s buckets each, one more than an aligned span
        var partitions = new PrefixPartitions(1, new int[] { 0 }, new int[] { 0 });
        var histogram = allDocs(partitions, 1000, 4999, 23_999, 5000);
        assertThat(histogram.estimatedGroups(5000), greaterThanOrEqualTo(500L));
    }

    public void testSingleDocPartition() throws IOException {
        // partitions start at prefix changes, so they never split a series: the second one is the single doc of series 1
        var partitions = new PrefixPartitions(2, new int[] { 0, 5 }, new int[] { 0, 10 });
        var histogram = allDocs(partitions, 11, 0, 9, 0);
        assertThat(histogram.seriesMillis(0), equalTo(10.0));
        assertThat(histogram.seriesMillis(1), equalTo(10.0));
    }

    public void testLargeTimeSpan() throws IOException {
        var partitions = new PrefixPartitions(1, new int[] { 0 }, new int[] { 0 });
        var histogram = allDocs(partitions, 20, Long.MIN_VALUE, Long.MAX_VALUE, 0);
        assertThat(histogram.totalSeriesMillis(), equalTo(Math.scalb(1.0, 65)));
        assertThat(histogram.estimatedSeries(), equalTo(2L));
    }

    public void testSeriesAcrossSegments() throws IOException {
        var partitions = new PrefixPartitions(1, new int[] { 0 }, new int[] { 0 });
        var older = allDocs(partitions, 100, 0, 499, 60_000);
        var newer = allDocs(partitions, 100, 500, 999, 60_000);
        var merged = TsidPrefixHistogram.merge(older, newer);
        assertThat(merged.minTimestamp(), equalTo(0L));
        assertThat(merged.maxTimestamp(), equalTo(999L));
        assertThat(merged.estimatedSeries(), equalTo(10L));
        // one 60s bucket plus two ends per series; estimating each segment on its own would double it
        assertThat(merged.estimatedGroups(60_000), equalTo(21L));
    }

    public void testMerge() {
        var a = new TsidPrefixHistogram(4, new int[] { 1, 4, 5 }, new double[] { 10, 20, 30 }, 3, 0, 99, Long.MAX_VALUE);
        var b = new TsidPrefixHistogram(4, new int[] { 0, 4, 9 }, new double[] { 1, 2, 3 }, 3, 50, 199, Long.MAX_VALUE);
        assertThat(
            TsidPrefixHistogram.merge(a, b),
            equalTo(new TsidPrefixHistogram(4, new int[] { 0, 1, 4, 5, 9 }, new double[] { 1, 10, 22, 30, 3 }, 5, 0, 199, Long.MAX_VALUE))
        );
        assertThat(TsidPrefixHistogram.merge(a, TsidPrefixHistogram.EMPTY), equalTo(a));
        assertThat(TsidPrefixHistogram.merge(TsidPrefixHistogram.EMPTY, a), equalTo(a));

        var coarse = new TsidPrefixHistogram(3, new int[] { 2 }, new double[] { 7.25 }, 1, 0, 99, Long.MAX_VALUE);
        var fine = new TsidPrefixHistogram(4, new int[] { 1, 4, 5, 8, 9 }, new double[] { 10, 20, 30, 40, 50 }, 5, 0, 99, Long.MAX_VALUE);
        var merged = TsidPrefixHistogram.merge(fine, coarse);
        assertThat(
            merged,
            equalTo(new TsidPrefixHistogram(3, new int[] { 0, 2, 4 }, new double[] { 10, 57.25, 90 }, 3, 0, 99, Long.MAX_VALUE))
        );
        assertThat(TsidPrefixHistogram.merge(coarse, fine), equalTo(merged));
    }

    public void testCappedDropsFewestBits() {
        int prefixBits = between(1, 20);
        int[] prefixes = IntStream.generate(() -> randomIntBetween(0, (1 << prefixBits) - 1))
            .limit(between(1, 2000))
            .sorted()
            .distinct()
            .toArray();
        double[] weights = DoubleStream.generate(() -> randomLongBetween(1, 100)).limit(prefixes.length).toArray();
        TsidPrefixHistogram histogram = new TsidPrefixHistogram(prefixBits, prefixes, weights, prefixes.length, 0, 99, Long.MAX_VALUE);
        int maxCells = between(1, prefixes.length + 1);
        var expectedCells = new TreeMap<Integer, Double>();
        int shift = 0;
        while (true) {
            expectedCells.clear();
            for (int i = 0; i < prefixes.length; i++) {
                expectedCells.merge(prefixes[i] >>> shift, weights[i], Double::sum);
            }
            if (expectedCells.size() <= maxCells) {
                break;
            }
            shift++;
        }
        var expected = new TsidPrefixHistogram(
            prefixBits - shift,
            expectedCells.keySet().stream().mapToInt(Integer::intValue).toArray(),
            expectedCells.values().stream().mapToDouble(Double::doubleValue).toArray(),
            expectedCells.size(),
            0,
            99,
            Long.MAX_VALUE
        );
        assertThat(histogram.capped(maxCells), equalTo(expected));
    }

    public void testSplits() {
        var uniform = new TsidPrefixHistogram(
            4,
            new int[] { 0, 1, 2, 3, 4, 5, 6, 7 },
            new double[] { 10, 10, 10, 10, 10, 10, 10, 10 },
            8,
            0,
            9,
            Long.MAX_VALUE
        );
        assertThat(uniform.splits(1), empty());
        assertThat(
            uniform.splits(4),
            equalTo(
                List.of(
                    new TsidPrefixRange(4, 0, 2),
                    new TsidPrefixRange(4, 2, 4),
                    new TsidPrefixRange(4, 4, 6),
                    new TsidPrefixRange(4, 6, 16)
                )
            )
        );

        var skewed = new TsidPrefixHistogram(4, new int[] { 3, 5, 9 }, new double[] { 1000, 10, 10 }, 3, 0, 9, Long.MAX_VALUE);
        assertThat(skewed.splits(11), equalTo(List.of(new TsidPrefixRange(4, 0, 5), new TsidPrefixRange(4, 5, 16))));

        assertThat(TsidPrefixHistogram.EMPTY.splits(4), empty());
    }

    public void testSplitsTileThePrefixSpace() {
        int prefixBits = between(4, 12);
        int cells = between(1, 1 << prefixBits);
        int[] prefixes = new int[cells];
        double[] weights = new double[cells];
        int prefix = 0;
        int size = 0;
        while (size < cells && prefix < 1 << prefixBits) {
            prefixes[size] = prefix;
            weights[size++] = between(1, 1000);
            prefix += between(1, 3);
        }
        var histogram = new TsidPrefixHistogram(
            prefixBits,
            Arrays.copyOf(prefixes, size),
            Arrays.copyOf(weights, size),
            size,
            0,
            9,
            Long.MAX_VALUE
        );
        int numSplits = between(2, 64);
        List<TsidPrefixRange> splits = histogram.splits(numSplits);
        if (splits.isEmpty()) {
            return;
        }
        assertThat(splits.size(), lessThanOrEqualTo(numSplits));
        assertThat(splits.getFirst().lower(), equalTo(0));
        assertThat(splits.getLast().upper(), equalTo(1 << prefixBits));
        double total = 0;
        for (int s = 0; s < splits.size(); s++) {
            TsidPrefixRange split = splits.get(s);
            assertThat(split.prefixBits(), equalTo(prefixBits));
            if (s > 0) {
                assertThat(split.lower(), equalTo(splits.get(s - 1).upper()));
            }
            double splitWeight = 0;
            for (int i = 0; i < histogram.totalPrefixes(); i++) {
                if (histogram.prefix(i) >= split.lower() && histogram.prefix(i) < split.upper()) {
                    splitWeight += histogram.seriesMillis(i);
                }
            }
            assertThat(splitWeight, greaterThan(0.0));
            total += splitWeight;
        }
        assertThat(total, equalTo(histogram.totalSeriesMillis()));
    }

    private static TsidPrefixHistogram allDocs(PrefixPartitions partitions, int maxDoc, long min, long max, long bucketMillis)
        throws IOException {
        return leaf(partitions, maxDoc, DocIdSetIterator.all(maxDoc), min, max, bucketMillis);
    }

    private static TsidPrefixHistogram leaf(
        PrefixPartitions partitions,
        int maxDoc,
        DocIdSetIterator matching,
        long min,
        long max,
        long bucketMillis
    ) throws IOException {
        var task = new CancellableTask(1, "test", "test", "", TaskId.EMPTY_TASK_ID, Map.of());
        return TsidPrefixHistogramCollector.collectOneLeaf(
            task,
            partitions,
            10,
            maxDoc,
            matching,
            tenDocsPerSeries(maxDoc),
            min,
            max,
            bucketMillis
        );
    }

    /**
     * {@code _tsid} doc values for a segment sorted by {@code _tsid} where every series has ten docs. Building real
     * time-series segments with prefix partitions needs the TSDB codec; this only has to answer ordinal lookups.
     */
    private static SortedDocValues tenDocsPerSeries(int maxDoc) {
        return new SortedDocValues() {
            private int doc = -1;

            @Override
            public int ordValue() {
                return doc / 10;
            }

            @Override
            public BytesRef lookupOrd(int ord) {
                throw new UnsupportedOperationException();
            }

            @Override
            public int getValueCount() {
                return Math.ceilDiv(maxDoc, 10);
            }

            @Override
            public boolean advanceExact(int target) {
                assert target >= doc : target + " < " + doc;
                doc = target;
                return true;
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() {
                return advance(doc + 1);
            }

            @Override
            public int advance(int target) {
                doc = target < maxDoc ? target : NO_MORE_DOCS;
                return doc;
            }

            @Override
            public long cost() {
                return maxDoc;
            }
        };
    }
}
