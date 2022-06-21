/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.sampler.random.InternalRandomSampler;
import org.elasticsearch.search.aggregations.bucket.sampler.random.RandomSamplerAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.search.aggregations.AggregationBuilders.avg;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.lessThan;

@ESIntegTestCase.SuiteScopeTestCase
public class RandomSamplerIT extends ESIntegTestCase {

    private static final String NUMERIC_VALUE = "number";
    private static final String MONOTONIC_VALUE = "monotonic";
    private static final String KEYWORD_VALUE = "kind";
    private static final String LOWER_KEYWORD = "lower";
    private static final String UPPER_KEYWORD = "upper";
    private static final double PROBABILITY = 0.25;
    private static int numDocs;

    private static final int NUM_SAMPLE_RUNS = 25;

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        numDocs = randomIntBetween(5000, 10000);
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            final String keywordValue;
            final double randomNumber;
            final double monotonicValue = randomDouble() + i;
            if (i % 2 == 0) {
                keywordValue = LOWER_KEYWORD;
                randomNumber = randomDoubleBetween(0.0, 3.0, false);
            } else {
                keywordValue = UPPER_KEYWORD;
                randomNumber = randomDoubleBetween(5.0, 10.0, false);
            }
            builders.add(
                client().prepareIndex("idx")
                    .setSource(
                        jsonBuilder().startObject()
                            .field(KEYWORD_VALUE, keywordValue)
                            .field(MONOTONIC_VALUE, monotonicValue)
                            .field(NUMERIC_VALUE, randomNumber)
                            .endObject()
                    )
            );
        }
        indexRandom(true, builders);
        ensureSearchable();
    }

    public void testRandomSampler() {
        double sampleMonotonicValue = 0.0;
        double sampleRandomValue = 0.0;
        double sampledDocCount = 0.0;

        SearchRequest sampledRequest = client().prepareSearch("idx")
            .addAggregation(
                new RandomSamplerAggregationBuilder("sampler").setProbability(PROBABILITY)
                    .subAggregation(avg("mean_monotonic").field(MONOTONIC_VALUE))
                    .subAggregation(avg("mean_numeric").field(NUMERIC_VALUE))
            )
            .request();
        for (int i = 0; i < NUM_SAMPLE_RUNS; i++) {
            InternalRandomSampler sampler = client().search(sampledRequest).actionGet().getAggregations().get("sampler");
            sampleMonotonicValue += ((Avg) sampler.getAggregations().get("mean_monotonic")).getValue();
            sampleRandomValue += ((Avg) sampler.getAggregations().get("mean_numeric")).getValue();
            sampledDocCount += sampler.getDocCount();
        }
        sampledDocCount /= NUM_SAMPLE_RUNS;
        sampleMonotonicValue /= NUM_SAMPLE_RUNS;
        sampleRandomValue /= NUM_SAMPLE_RUNS;
        double countError = (numDocs * 0.25) * 0.1;
        assertThat(Math.abs(sampledDocCount - (numDocs * 0.25)), lessThan(countError));

        SearchResponse trueValueResponse = client().prepareSearch("idx")
            .addAggregation(avg("mean_monotonic").field(MONOTONIC_VALUE))
            .addAggregation(avg("mean_numeric").field(NUMERIC_VALUE))
            .get();
        double trueMonotonic = ((Avg) trueValueResponse.getAggregations().get("mean_monotonic")).getValue();
        double trueRandom = ((Avg) trueValueResponse.getAggregations().get("mean_numeric")).getValue();
        assertThat(Math.abs(sampleMonotonicValue - trueMonotonic), lessThan(trueMonotonic * 0.1));
        assertThat(Math.abs(sampleRandomValue - trueRandom), lessThan(trueRandom * 0.1));
    }

    public void testRandomSamplerHistogram() {
        Map<String, Double> sampleMonotonicValue = new HashMap<>();
        Map<String, Double> sampleRandomValue = new HashMap<>();
        Map<String, Double> sampledDocCount = new HashMap<>();

        SearchRequest sampledRequest = client().prepareSearch("idx")
            .addAggregation(
                new RandomSamplerAggregationBuilder("sampler").setProbability(0.5)
                    .subAggregation(
                        histogram("histo").field(NUMERIC_VALUE)
                            .interval(5.0)
                            .subAggregation(avg("mean_monotonic").field(MONOTONIC_VALUE))
                            .subAggregation(avg("mean_numeric").field(NUMERIC_VALUE))
                    )
            )
            .request();
        for (int i = 0; i < NUM_SAMPLE_RUNS; i++) {
            InternalRandomSampler sampler = client().search(sampledRequest).actionGet().getAggregations().get("sampler");
            Histogram histo = sampler.getAggregations().get("histo");
            for (Histogram.Bucket bucket : histo.getBuckets()) {
                sampleMonotonicValue.compute(
                    bucket.getKeyAsString(),
                    (k, v) -> ((Avg) bucket.getAggregations().get("mean_monotonic")).getValue() + (v == null ? 0 : v)
                );
                sampleRandomValue.compute(
                    bucket.getKeyAsString(),
                    (k, v) -> ((Avg) bucket.getAggregations().get("mean_numeric")).getValue() + (v == null ? 0 : v)
                );
                sampledDocCount.compute(bucket.getKeyAsString(), (k, v) -> bucket.getDocCount() + (v == null ? 0 : v));
            }
        }
        for (String key : sampledDocCount.keySet()) {
            sampledDocCount.put(key, sampledDocCount.get(key) / NUM_SAMPLE_RUNS);
            sampleRandomValue.put(key, sampleRandomValue.get(key) / NUM_SAMPLE_RUNS);
            sampleMonotonicValue.put(key, sampleMonotonicValue.get(key) / NUM_SAMPLE_RUNS);
        }

        SearchResponse trueValueResponse = client().prepareSearch("idx")
            .addAggregation(
                histogram("histo").field(NUMERIC_VALUE)
                    .interval(5.0)
                    .subAggregation(avg("mean_monotonic").field(MONOTONIC_VALUE))
                    .subAggregation(avg("mean_numeric").field(NUMERIC_VALUE))
            )
            .get();
        Histogram histogram = trueValueResponse.getAggregations().get("histo");
        for (Histogram.Bucket bucket : histogram.getBuckets()) {
            long numDocs = bucket.getDocCount();
            double errorRate = Math.pow(1.0 / (0.5 * numDocs), 0.35);
            double countError = errorRate * numDocs;
            assertThat(Math.abs(sampledDocCount.get(bucket.getKeyAsString()) - numDocs), lessThan(countError));
            double trueMonotonic = ((Avg) bucket.getAggregations().get("mean_monotonic")).getValue();
            double trueRandom = ((Avg) bucket.getAggregations().get("mean_numeric")).getValue();
            assertThat(Math.abs(sampleMonotonicValue.get(bucket.getKeyAsString()) - trueMonotonic), lessThan(trueMonotonic * errorRate));
            assertThat(Math.abs(sampleRandomValue.get(bucket.getKeyAsString()) - trueRandom), lessThan(trueRandom * errorRate));
        }
    }

}
