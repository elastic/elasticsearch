/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.grouping;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.DoubleRangeBlockBuilder;
import org.elasticsearch.compute.data.TDigestHolder;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.is;

/**
 * Tests {@link BucketTests.HistogramResultsMatcher} itself. {@code testEvaluateInManyThreads} matches the same t-digest
 * output thousands of times per thread, so a full check on every match can exceed the suite timeout (#163273).
 */
public class BucketHistogramResultsMatcherTests extends ESTestCase {

    public void testRepeatedOutputIsFullyCheckedOnce() {
        TDigestHolder digest = EsqlTestUtils.randomTDigest();
        Object output = bucket(digest, randomDoubleBetween(0.01, 1, true));
        BucketTests.HistogramResultsMatcher matcher = new BucketTests.HistogramResultsMatcher(digest);
        int repeats = between(2, 100);
        for (int i = 0; i < repeats; i++) {
            assertThat(output, matcher);
        }
        assertThat(matcher.fullChecks(), is(1));
    }

    public void testRejectsWrongOutputAfterAcceptingCorrectOne() {
        TDigestHolder digest = EsqlTestUtils.randomTDigest();
        double roundTo = randomDoubleBetween(0.01, 1, true);
        Object output = bucket(digest, roundTo);
        BucketTests.HistogramResultsMatcher matcher = new BucketTests.HistogramResultsMatcher(digest);
        assertThat(output, matcher);

        List<Object> wrong = output instanceof List<?> list ? new ArrayList<>(list) : new ArrayList<>(List.of(output));
        double end = digest.getMax() + 10 * roundTo;
        wrong.add(new DoubleRangeBlockBuilder.DoubleRange(end, end + roundTo));
        assertFalse(matcher.matches(wrong));
        assertFalse(matcher.matches(wrong));
        assertThat(matcher.fullChecks(), is(3));
    }

    public void testRejectsNullForNonEmptyHistogram() {
        BucketTests.HistogramResultsMatcher matcher = new BucketTests.HistogramResultsMatcher(EsqlTestUtils.randomTDigest());
        assertFalse(matcher.matches(null));
    }

    private static Object bucket(TDigestHolder digest, double roundTo) {
        try (DoubleRangeBlockBuilder builder = TestBlockFactory.getNonBreakingInstance().newDoubleRangeBlockBuilder(1)) {
            Bucket.process(builder, digest, roundTo);
            try (Block block = builder.build()) {
                return BlockUtils.toJavaObject(block, 0);
            }
        }
    }
}
