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

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class RandomSamplerAggregatorTests extends AggregatorTestCase {

    private static final String NUMERIC_FIELD_NAME = "value";

    public void testAggregationSampling() throws IOException {
        testCase(
            new RandomSamplerAggregationBuilder("my_agg").subAggregation(AggregationBuilders.avg("avg").field(NUMERIC_FIELD_NAME))
                .setProbability(0.25),
            new MatchAllDocsQuery(),
            RandomSamplerAggregatorTests::writeTestDocs,
            (InternalRandomSampler result) -> {
                assertThat(result.getDocCount(), allOf(greaterThan(20L), lessThan(60L)));
                Avg agg = result.getAggregations().get("avg");
                assertThat(agg.getValue(), closeTo(2, 1));
            },
            longField(NUMERIC_FIELD_NAME)
        );
    }

    private static void writeTestDocs(RandomIndexWriter w) throws IOException {
        for (int i = 0; i < 50; i++) {
            w.addDocument(List.of(new SortedNumericDocValuesField(NUMERIC_FIELD_NAME, 1)));
        }
        for (int i = 0; i < 50; i++) {
            w.addDocument(List.of(new SortedNumericDocValuesField(NUMERIC_FIELD_NAME, 2)));
        }
        for (int i = 0; i < 25; i++) {
            w.addDocument(List.of(new SortedNumericDocValuesField(NUMERIC_FIELD_NAME, 4)));
        }
    }

}
