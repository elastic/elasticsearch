/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.sampler.random;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.metrics.Avg;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class RandomSamplerAggregatorTests extends AggregatorTestCase {

    private static final String NUMERIC_FIELD_NAME = "value";

    public void testAggregationSampling() throws IOException {
        double[] avgs = new double[5];
        long[] counts = new long[5];
        AtomicInteger integer = new AtomicInteger();
        do {
            testCase(
                new RandomSamplerAggregationBuilder("my_agg").subAggregation(AggregationBuilders.avg("avg").field(NUMERIC_FIELD_NAME))
                    .setProbability(0.25),
                new MatchAllDocsQuery(),
                RandomSamplerAggregatorTests::writeTestDocs,
                (InternalRandomSampler result) -> {
                    counts[integer.get()] = result.getDocCount();
                    Avg agg = result.getAggregations().get("avg");
                    assertTrue(Double.isNaN(agg.getValue()) == false && Double.isFinite(agg.getValue()));
                    avgs[integer.get()] = agg.getValue();
                },
                longField(NUMERIC_FIELD_NAME)
            );
        } while (integer.incrementAndGet() < 5);
        long avgCount = LongStream.of(counts).sum() / integer.get();
        double avgAvg = DoubleStream.of(avgs).sum() / integer.get();
        assertThat(avgCount, allOf(greaterThanOrEqualTo(20L), lessThanOrEqualTo(70L)));
        assertThat(avgAvg, closeTo(1.5, 0.5));
    }

    private static void writeTestDocs(RandomIndexWriter w) throws IOException {
        for (int i = 0; i < 75; i++) {
            w.addDocument(List.of(new SortedNumericDocValuesField(NUMERIC_FIELD_NAME, 1)));
        }
        for (int i = 0; i < 75; i++) {
            w.addDocument(List.of(new SortedNumericDocValuesField(NUMERIC_FIELD_NAME, 2)));
        }
    }

}
