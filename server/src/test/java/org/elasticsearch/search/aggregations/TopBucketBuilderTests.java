/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation.InternalBucket;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.search.aggregations.DelayedBucketTests.mockReduce;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;

public class TopBucketBuilderTests extends ESTestCase {
    public void testSizeOne() {
        int count = between(1, 1000);
        ReduceContext context = mock(ReduceContext.class);
        List<String> nonCompetitive = new ArrayList<>();
        TopBucketBuilder<InternalBucket> builder = TopBucketBuilder.build(1, BucketOrder.key(true), b -> nonCompetitive.add(b.toString()));

        for (int i = 0; i < count; i++) {
            builder.add(new DelayedBucket<>(mockReduce(context), context, List.of(bucket(i))));
        }

        List<InternalBucket> top = builder.build();
        assertThat(top, hasSize(1));
        assertThat(top.get(0).getKeyAsString(), equalTo("000000"));
        assertThat(top.get(0).getDocCount(), equalTo(1L));
        for (int i = 1; i < count; i++) {
            assertThat(nonCompetitive.get(i - 1), equalTo("Delayed[" + bucketKey(i) + "]"));
        }
    }

    public void testAllCompetitive() {
        int size = between(3, 1000);
        int count = between(1, size);
        ReduceContext context = mock(ReduceContext.class);
        TopBucketBuilder<InternalBucket> builder = TopBucketBuilder.build(
            size,
            BucketOrder.key(true),
            b -> fail("unexpected uncompetitive bucket " + b)
        );

        for (int i = 0; i < count; i++) {
            builder.add(new DelayedBucket<>(mockReduce(context), context, List.of(bucket(i))));
        }

        List<InternalBucket> top = builder.build();
        assertThat(top, hasSize(count));
        for (int i = 0; i < count; i++) {
            assertThat(top.get(i).getKeyAsString(), equalTo(bucketKey(i)));
            assertThat(top.get(i).getDocCount(), equalTo(1L));
        }
    }

    public void someNonCompetitiveTestCase(int size) {
        int count = between(size + 1, size * 30);
        ReduceContext context = mock(ReduceContext.class);
        List<String> nonCompetitive = new ArrayList<>();
        TopBucketBuilder<InternalBucket> builder = TopBucketBuilder.build(
            size,
            BucketOrder.key(true),
            b -> nonCompetitive.add(b.toString())
        );

        for (int i = 0; i < count; i++) {
            builder.add(new DelayedBucket<>(mockReduce(context), context, List.of(bucket(i))));
        }

        List<InternalBucket> top = builder.build();
        assertThat(top, hasSize(size));
        for (int i = 0; i < count; i++) {
            if (i < size) {
                assertThat(top.get(i).getKeyAsString(), equalTo(bucketKey(i)));
                assertThat(top.get(i).getDocCount(), equalTo(1L));
            } else {
                assertThat(nonCompetitive.get(i - size), equalTo("Delayed[" + bucketKey(i) + "]"));
            }
        }
    }

    public void testSomeNonCompetitiveSmall() {
        someNonCompetitiveTestCase(between(2, TopBucketBuilder.USE_BUFFERING_BUILDER - 1));
    }

    public void testSomeNonCompetitiveLarge() {
        someNonCompetitiveTestCase(between(TopBucketBuilder.USE_BUFFERING_BUILDER, TopBucketBuilder.USE_BUFFERING_BUILDER * 5));
    }

    public void testHuge() {
        int count = between(1, 1000);
        ReduceContext context = mock(ReduceContext.class);
        TopBucketBuilder<InternalBucket> builder = TopBucketBuilder.build(
            Integer.MAX_VALUE,
            BucketOrder.key(true),
            b -> fail("unexpected uncompetitive bucket " + b)
        );

        for (int i = 0; i < count; i++) {
            builder.add(new DelayedBucket<>(mockReduce(context), context, List.of(bucket(i))));
        }

        List<InternalBucket> top = builder.build();
        assertThat(top, hasSize(count));
        assertThat(top.get(0).getKeyAsString(), equalTo("000000"));
        assertThat(top.get(0).getDocCount(), equalTo(1L));
        for (int i = 0; i < count; i++) {
            assertThat(top.get(i).getKeyAsString(), equalTo(bucketKey(i)));
            assertThat(top.get(i).getDocCount(), equalTo(1L));
        }
    }

    public void testHugeQueueError() {
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> new TopBucketBuilder.PriorityQueueTopBucketBuilder<>(
                ArrayUtil.MAX_ARRAY_LENGTH,
                BucketOrder.key(true),
                b -> fail("unexpected uncompetitive bucket " + b)
            )
        );
        assertThat(e.getMessage(), equalTo("can't reduce more than [" + ArrayUtil.MAX_ARRAY_LENGTH + "] buckets"));
    }

    private String bucketKey(int index) {
        return String.format(Locale.ROOT, "%06d", index);
    }

    private InternalBucket bucket(int index) {
        return new StringTerms.Bucket(new BytesRef(bucketKey(index)), 1, InternalAggregations.EMPTY, false, 0, DocValueFormat.RAW);
    }
}
