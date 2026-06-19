/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.aggregation.PromqlHistogramQuantileStates.Bucket;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.test.operator.blocksource.ListRowsBlockSourceOperator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

import static org.elasticsearch.test.ESTestCase.between;
import static org.elasticsearch.test.ESTestCase.randomDoubleBetween;

final class PromqlHistogramQuantileTestHelpers {
    private PromqlHistogramQuantileTestHelpers() {}

    static List<Bucket> randomCumulativeHistogram() {
        int numFiniteBuckets = between(2, 5);
        TreeSet<Double> bounds = new TreeSet<>();
        while (bounds.size() < numFiniteBuckets) {
            bounds.add(randomDoubleBetween(0.001, 1_000, true));
        }
        double cumulative = 0;
        List<Bucket> buckets = new ArrayList<>(numFiniteBuckets + 1);
        for (double bound : bounds) {
            cumulative += between(1, 100);
            buckets.add(new Bucket(bound, cumulative));
        }
        buckets.add(new Bucket(Double.POSITIVE_INFINITY, cumulative));
        return buckets;
    }

    static List<List<Object>> randomBucketRows(int size) {
        List<List<Object>> rows = new ArrayList<>(size);
        while (rows.size() < size) {
            for (Bucket bucket : randomCumulativeHistogram()) {
                rows.add(List.of(bucket.count(), leLabel(bucket.upperBound())));
                if (rows.size() == size) {
                    break;
                }
            }
        }
        return rows;
    }

    static List<List<Object>> randomGroupedBucketRows(int size) {
        List<List<Object>> rows = new ArrayList<>(size);
        while (rows.size() < size) {
            long group = between(0, 4);
            for (Bucket bucket : randomCumulativeHistogram()) {
                rows.add(List.of(group, bucket.count(), leLabel(bucket.upperBound())));
                if (rows.size() == size) {
                    break;
                }
            }
        }
        return rows;
    }

    /**
     * Renders a bucket upper bound as the {@code le} keyword label the aggregator parses: PromQL writes the terminating
     * bucket as {@code "+Inf"}, and every other bound as a finite number that round-trips through {@link Double#toString}.
     */
    static String leLabel(double upperBound) {
        return upperBound == Double.POSITIVE_INFINITY ? "+Inf" : Double.toString(upperBound);
    }

    static SourceOperator bucketRowsSource(BlockFactory blockFactory, int size) {
        return new ListRowsBlockSourceOperator(blockFactory, List.of(ElementType.DOUBLE, ElementType.BYTES_REF), randomBucketRows(size));
    }

    static SourceOperator groupedBucketRowsSource(BlockFactory blockFactory, int size) {
        return new ListRowsBlockSourceOperator(
            blockFactory,
            List.of(ElementType.LONG, ElementType.DOUBLE, ElementType.BYTES_REF),
            randomGroupedBucketRows(size)
        );
    }

    static List<Bucket> bucketsFromPages(List<Page> pages, int countChannel, int upperBoundChannel) {
        List<Bucket> buckets = new ArrayList<>();
        for (Page page : pages) {
            appendBuckets(page, countChannel, upperBoundChannel, buckets);
        }
        return buckets;
    }

    private static void appendBuckets(Page page, int countChannel, int upperBoundChannel, List<Bucket> buckets) {
        for (int p = 0; p < page.getPositionCount(); p++) {
            appendBuckets(page, countChannel, upperBoundChannel, p, buckets);
        }
    }

    static void appendBuckets(Page page, int countChannel, int upperBoundChannel, int position, List<Bucket> buckets) {
        DoubleBlock counts = page.getBlock(countChannel);
        BytesRefBlock upperBounds = page.getBlock(upperBoundChannel);
        BytesRef scratch = new BytesRef();
        int countStart = counts.getFirstValueIndex(position);
        int countEnd = countStart + counts.getValueCount(position);
        int upperBoundStart = upperBounds.getFirstValueIndex(position);
        int upperBoundEnd = upperBoundStart + upperBounds.getValueCount(position);
        for (int countOffset = countStart; countOffset < countEnd; countOffset++) {
            double count = counts.getDouble(countOffset);
            for (int upperBoundOffset = upperBoundStart; upperBoundOffset < upperBoundEnd; upperBoundOffset++) {
                double upperBound;
                try {
                    upperBound = PromqlHistogramQuantileStates.parseUpperBound(upperBounds.getBytesRef(upperBoundOffset, scratch));
                } catch (NumberFormatException e) {
                    // Mirror the aggregator: buckets with an unparseable `le` label are skipped, not counted.
                    continue;
                }
                buckets.add(new Bucket(upperBound, count));
            }
        }
    }

    static double expectedQuantile(double quantile, List<Bucket> buckets) {
        List<Bucket> sortedBuckets = new ArrayList<>(buckets);
        sortedBuckets.sort(Comparator.comparingDouble(Bucket::upperBound));
        return PromqlHistogramQuantileStates.bucketQuantile(quantile, coalesceBuckets(sortedBuckets));
    }

    /**
     * Merges buckets that share the same upper bound by summing their cumulative counts before calling
     * {@link PromqlHistogramQuantileStates#bucketQuantile}. The production state does the same merge as raw and
     * intermediate buckets are added, so the quantile helper can assert that its input is already pre-aggregated.
     */
    private static List<Bucket> coalesceBuckets(List<Bucket> buckets) {
        if (buckets.isEmpty()) {
            return buckets;
        }
        List<Bucket> result = new ArrayList<>(buckets.size());
        Bucket previous = buckets.getFirst();
        for (int i = 1; i < buckets.size(); i++) {
            Bucket bucket = buckets.get(i);
            if (bucket.upperBound() == previous.upperBound()) {
                previous = new Bucket(previous.upperBound(), previous.count() + bucket.count());
            } else {
                result.add(previous);
                previous = bucket;
            }
        }
        result.add(previous);
        return result;
    }

    static List<Bucket> canonicalHistogram() {
        return List.of(new Bucket(1.0, 1.0), new Bucket(2.0, 3.0), new Bucket(Double.POSITIVE_INFINITY, 4.0));
    }

    static List<Bucket> unsortedCanonicalHistogram() {
        return Arrays.asList(new Bucket(2.0, 3.0), new Bucket(Double.POSITIVE_INFINITY, 4.0), new Bucket(1.0, 1.0));
    }
}
