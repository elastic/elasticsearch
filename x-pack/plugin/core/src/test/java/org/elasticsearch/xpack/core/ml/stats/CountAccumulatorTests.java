/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.stats;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms.Bucket;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CountAccumulatorTests extends AbstractWireSerializingTestCase<CountAccumulator> {

    public void testEmpty() {
        CountAccumulator accumulator = new CountAccumulator();
        assertEquals(Collections.emptyMap(), accumulator.asMap());
    }

    public void testAdd() {
        CountAccumulator accumulator = new CountAccumulator();
        accumulator.add("a", 22L);
        accumulator.add("a", 10L);
        accumulator.add("a", 15L);
        accumulator.add("a", -12L);
        accumulator.add("a", 0L);

        accumulator.add("b", 13L);
        accumulator.add("b", 1L);
        accumulator.add("b", 40000L);
        accumulator.add("b", -2L);
        accumulator.add("b", 333L);

        assertEquals(35L, accumulator.asMap().get("a").longValue());
        assertEquals(40345L, accumulator.asMap().get("b").longValue());
        assertEquals(2, accumulator.asMap().size());
    }

    public void testMerge() {
        CountAccumulator accumulator = new CountAccumulator();
        accumulator.add("a", 13L);
        accumulator.add("b", 42L);

        CountAccumulator accumulator2 = new CountAccumulator();
        accumulator2.add("a", 12L);
        accumulator2.add("c", -1L);

        accumulator.merge(accumulator2);

        assertEquals(25L, accumulator.asMap().get("a").longValue());
        assertEquals(42L, accumulator.asMap().get("b").longValue());
        assertEquals(-1L, accumulator.asMap().get("c").longValue());
        assertEquals(3, accumulator.asMap().size());
    }

    public void testFromTermsAggregation() {
        StringTerms termsAggregation = mock(StringTerms.class);

        Bucket bucket1 = mock(Bucket.class);
        when(bucket1.getKeyAsString()).thenReturn("a");
        when(bucket1.getDocCount()).thenReturn(10L);

        Bucket bucket2 = mock(Bucket.class);
        when(bucket2.getKeyAsString()).thenReturn("b");
        when(bucket2.getDocCount()).thenReturn(33L);

        List<Bucket> buckets = Arrays.asList(bucket1, bucket2);
        when(termsAggregation.getBuckets()).thenReturn(buckets);

        CountAccumulator accumulator = CountAccumulator.fromTermsAggregation(termsAggregation);

        assertEquals(10L, accumulator.asMap().get("a").longValue());
        assertEquals(33L, accumulator.asMap().get("b").longValue());
        assertEquals(2, accumulator.asMap().size());
    }

    @Override
    public CountAccumulator createTestInstance() {
        CountAccumulator accumulator = new CountAccumulator();
        for (int i = 0; i < randomInt(10); ++i) {
            accumulator.add(randomAlphaOfLengthBetween(1, 20), randomLongBetween(1L, 100L));
        }

        return accumulator;
    }

    @Override
    protected Reader<CountAccumulator> instanceReader() {
        return CountAccumulator::new;
    }

}
