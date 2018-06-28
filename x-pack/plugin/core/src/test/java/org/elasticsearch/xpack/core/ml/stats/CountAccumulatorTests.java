/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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
        accumulator.add("a", 22l);
        accumulator.add("a", 10l);
        accumulator.add("a", 15l);
        accumulator.add("a", -12l);
        accumulator.add("a", 0l);

        accumulator.add("b", 13l);
        accumulator.add("b", 1l);
        accumulator.add("b", 40000l);
        accumulator.add("b", -2l);
        accumulator.add("b", 333l);

        assertEquals(35l, accumulator.asMap().get("a").longValue());
        assertEquals(40345l, accumulator.asMap().get("b").longValue());
        assertEquals(2, accumulator.asMap().size());
    }

    public void testMerge() {
        CountAccumulator accumulator = new CountAccumulator();
        accumulator.add("a", 13l);
        accumulator.add("b", 42l);

        CountAccumulator accumulator2 = new CountAccumulator();
        accumulator2.add("a", 12l);
        accumulator2.add("c", -1l);
        
        accumulator.merge(accumulator2);
        
        assertEquals(25l, accumulator.asMap().get("a").longValue());
        assertEquals(42l, accumulator.asMap().get("b").longValue());
        assertEquals(-1l, accumulator.asMap().get("c").longValue());
        assertEquals(3, accumulator.asMap().size());
    }

    public void testFromTermsAggregation() {
        StringTerms termsAggregation = mock(StringTerms.class);
        
        Bucket bucket1 = mock(Bucket.class);
        when(bucket1.getKeyAsString()).thenReturn("a");
        when(bucket1.getDocCount()).thenReturn(10l);
        
        Bucket bucket2 = mock(Bucket.class);
        when(bucket2.getKeyAsString()).thenReturn("b");
        when(bucket2.getDocCount()).thenReturn(33l);
        
        List<Bucket> buckets =  Arrays.asList(bucket1, bucket2);
        when(termsAggregation.getBuckets()).thenReturn(buckets);
        
        CountAccumulator accumulator = CountAccumulator.fromTermsAggregation(termsAggregation);
        
        assertEquals(10l, accumulator.asMap().get("a").longValue());
        assertEquals(33l, accumulator.asMap().get("b").longValue());
        assertEquals(2, accumulator.asMap().size());
    }

    @Override
    public CountAccumulator createTestInstance() {
        CountAccumulator accumulator = new CountAccumulator();
        for (int i = 0; i < randomInt(10); ++i) {
            accumulator.add(randomAlphaOfLengthBetween(1, 20), randomLongBetween(1l, 100l));
        }

        return accumulator;
    }

    @Override
    protected Reader<CountAccumulator> instanceReader() {
        return CountAccumulator::new;
    }

}
