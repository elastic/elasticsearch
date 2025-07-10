/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.exponentialhistogram;

import org.elasticsearch.test.ESTestCase;

import java.util.stream.IntStream;

public class FixedSizeExponentialHistogramTest extends ESTestCase {


    public void testPrintBuckets() {
        ExponentialHistogram first = ExponentialHistogramGenerator.createFor(0.01234, 42, 56789);
        ExponentialHistogram second = ExponentialHistogramGenerator.createFor(38, 50, 250, 257, 10001.1234);

        ExponentialHistogram result = ExponentialHistogramMerger.merge(7, first, second);
        printMidpoints(result);
    }


    public void testPrintBucketsLinearScale() {

        ExponentialHistogram result = ExponentialHistogramGenerator.createFor(
            1000,
            IntStream.range(-1_000_000, 2_000_000).mapToDouble(Double::valueOf)
        );

        double smallPerc = ExpHistoPercentiles.getPercentile(result, 0.00001);
        double highPerc = ExpHistoPercentiles.getPercentile(result, 0.9999);
        double median = ExpHistoPercentiles.getPercentile(result, 0.5);

        printMidpoints(result);
    }

    public static void printMidpoints(ExponentialHistogram histo) {
        StringBuilder sb = new StringBuilder("{ base : ");
        sb.append(ExponentialHistogramUtils.getLowerBucketBoundary(1, histo.scale())).append(", ");
        ExponentialHistogram.BucketIterator neg = histo.negativeBuckets();
        while (neg.hasNext()) {
            long idx = neg.peekIndex();
            long count = neg.peekCount();
            double center = -ExponentialHistogramUtils.getPointOfLeastRelativeError(idx, neg.scale());
            sb.append(center).append(":").append(count).append(", ");
            neg.advance();
        }
        sb.append("0.0 : ").append(histo.zeroBucket().count());
        ExponentialHistogram.BucketIterator pos = histo.positiveBuckets();
        while (pos.hasNext()) {
            long idx = pos.peekIndex();
            long count = pos.peekCount();
            double center = ExponentialHistogramUtils.getPointOfLeastRelativeError(idx, pos.scale());
            sb.append(", ").append(center).append(":").append(count);
            pos.advance();
        }
        sb.append('}');
        System.out.println(sb);

    }
}
