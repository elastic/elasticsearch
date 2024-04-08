/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation.InternalBucket;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalAggregationTestCase;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.sameInstance;

public class DelayedBucketTests extends ESTestCase {
    public void testToString() {
        assertThat(new DelayedBucket<>(null, null, List.of(bucket("test", 1))).toString(), equalTo("Delayed[test]"));
    }

    public void testReduced() {
        AtomicInteger buckets = new AtomicInteger();
        AggregationReduceContext context = new AggregationReduceContext.ForFinal(null, null, () -> false, null, buckets::addAndGet);
        DelayedBucket<?> b = new DelayedBucket<>(mockReduce(context), context, List.of(bucket("test", 1), bucket("test", 2)));
        assertThat(b.getDocCount(), equalTo(3L));
        assertThat(b.reduced(), sameInstance(b.reduced()));
        assertThat(b.reduced().getKeyAsString(), equalTo("test"));
        assertThat(b.reduced().getDocCount(), equalTo(3L));
        assertEquals(1, buckets.get());
    }

    public void testCompareKey() {
        AggregationReduceContext context = InternalAggregationTestCase.emptyReduceContextBuilder().forFinalReduction();
        DelayedBucket<?> a = new DelayedBucket<>(mockReduce(context), context, List.of(bucket("a", 1)));
        DelayedBucket<?> b = new DelayedBucket<>(mockReduce(context), context, List.of(bucket("b", 1)));
        if (randomBoolean()) {
            a.reduced();
        }
        if (randomBoolean()) {
            b.reduced();
        }
        assertThat(a.compareKey(b), lessThan(0));
        assertThat(b.compareKey(a), greaterThan(0));
        assertThat(a.compareKey(a), equalTo(0));
    }

    public void testNonCompetitiveNotReduced() {
        AggregationReduceContext context = new AggregationReduceContext.ForFinal(
            null,
            null,
            () -> false,
            null,
            b -> fail("shouldn't be called")
        );
        new DelayedBucket<>(mockReduce(context), context, List.of(bucket("test", 1))).nonCompetitive();
    }

    public void testNonCompetitiveReduced() {
        AtomicInteger buckets = new AtomicInteger();
        AggregationReduceContext context = new AggregationReduceContext.ForFinal(null, null, () -> false, null, buckets::addAndGet);
        DelayedBucket<?> b = new DelayedBucket<>(mockReduce(context), context, List.of(bucket("test", 1)));
        b.reduced();
        assertEquals(1, buckets.get());
        b.nonCompetitive();
        assertEquals(0, buckets.get());
    }

    private static InternalBucket bucket(String key, long docCount) {
        return new StringTerms.Bucket(new BytesRef(key), docCount, InternalAggregations.EMPTY, false, 0, DocValueFormat.RAW);
    }

    static BiFunction<List<InternalBucket>, AggregationReduceContext, InternalBucket> mockReduce(AggregationReduceContext context) {
        return (l, c) -> {
            assertThat(c, sameInstance(context));
            return bucket(l.get(0).getKeyAsString(), l.stream().mapToLong(Bucket::getDocCount).sum());
        };
    }
}
