/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Bucket;
import org.elasticsearch.search.aggregations.metrics.ExtendedStats.Bounds;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.IntToDoubleFunction;

import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.search.aggregations.PipelineAggregatorBuilders.extendedStatsBucket;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;

public class ExtendedStatsBucketIT extends BucketMetricsPipeLineAggregationTestCase<ExtendedStatsBucket> {

    @Override
    protected ExtendedStatsBucketPipelineAggregationBuilder BucketMetricsPipelineAgg(String name, String bucketsPath) {
        return extendedStatsBucket(name, bucketsPath);
    }

    @Override
    protected void assertResult(
        IntToDoubleFunction buckets,
        Function<Integer, String> bucketKeys,
        int numBuckets,
        ExtendedStatsBucket pipelineBucket
    ) {
        double sum = 0;
        int count = 0;
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        double sumOfSquares = 0;
        for (int i = 0; i < numBuckets; ++i) {
            double bucketValue = buckets.applyAsDouble(i);
            count++;
            sum += bucketValue;
            min = Math.min(min, bucketValue);
            max = Math.max(max, bucketValue);
            sumOfSquares += bucketValue * bucketValue;
        }
        double avgValue = count == 0 ? Double.NaN : (sum / count);
        assertThat(pipelineBucket.getAvg(), equalTo(avgValue));
        assertThat(pipelineBucket.getMin(), equalTo(min));
        assertThat(pipelineBucket.getMax(), equalTo(max));
        assertThat(pipelineBucket.getSumOfSquares(), equalTo(sumOfSquares));
    }

    @Override
    protected String nestedMetric() {
        return "avg";
    }

    @Override
    protected double getNestedMetric(ExtendedStatsBucket bucket) {
        return bucket.getAvg();
    }

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        super.setupSuiteScopeCluster();

        List<IndexRequestBuilder> builders = new ArrayList<>();

        for (int i = 0; i < 6; i++) {
            // creates 6 documents where the value of the field is 0, 1, 2, 3,
            // 3, 5
            builders.add(
                client().prepareIndex("idx_gappy")
                    .setId("" + i)
                    .setSource(jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, i == 4 ? 3 : i).endObject())
            );
        }

        indexRandom(true, builders);
        ensureSearchable();
    }

    /**
     * Test for https://github.com/elastic/elasticsearch/issues/17701
     */
    public void testGappyIndexWithSigma() {
        double sigma = randomDoubleBetween(1.0, 6.0, true);
        SearchResponse response = client().prepareSearch("idx_gappy")
            .addAggregation(histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(1L))
            .addAggregation(extendedStatsBucket("extended_stats_bucket", "histo>_count").sigma(sigma))
            .get();
        assertSearchResponse(response);
        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(6));

        for (int i = 0; i < 6; ++i) {
            long expectedDocCount;
            if (i == 3) {
                expectedDocCount = 2;
            } else if (i == 4) {
                expectedDocCount = 0;
            } else {
                expectedDocCount = 1;
            }
            Histogram.Bucket bucket = buckets.get(i);
            assertThat("i: " + i, bucket, notNullValue());
            assertThat("i: " + i, ((Number) bucket.getKey()).longValue(), equalTo((long) i));
            assertThat("i: " + i, bucket.getDocCount(), equalTo(expectedDocCount));
        }

        ExtendedStatsBucket extendedStatsBucketValue = response.getAggregations().get("extended_stats_bucket");
        long count = 6L;
        double sum = 1.0 + 1.0 + 1.0 + 2.0 + 0.0 + 1.0;
        double sumOfSqrs = 1.0 + 1.0 + 1.0 + 4.0 + 0.0 + 1.0;
        double avg = sum / count;
        double var = (sumOfSqrs - ((sum * sum) / count)) / count;
        var = var < 0 ? 0 : var;
        double stdDev = Math.sqrt(var);
        assertThat(extendedStatsBucketValue, notNullValue());
        assertThat(extendedStatsBucketValue.getName(), equalTo("extended_stats_bucket"));
        assertThat(extendedStatsBucketValue.getMin(), equalTo(0.0));
        assertThat(extendedStatsBucketValue.getMax(), equalTo(2.0));
        assertThat(extendedStatsBucketValue.getCount(), equalTo(count));
        assertThat(extendedStatsBucketValue.getSum(), equalTo(sum));
        assertThat(extendedStatsBucketValue.getAvg(), equalTo(avg));
        assertThat(extendedStatsBucketValue.getSumOfSquares(), equalTo(sumOfSqrs));
        assertThat(extendedStatsBucketValue.getVariance(), equalTo(var));
        assertThat(extendedStatsBucketValue.getStdDeviation(), equalTo(stdDev));
        assertThat(extendedStatsBucketValue.getStdDeviationBound(Bounds.LOWER), equalTo(avg - (sigma * stdDev)));
        assertThat(extendedStatsBucketValue.getStdDeviationBound(Bounds.UPPER), equalTo(avg + (sigma * stdDev)));
    }

    public void testBadSigmaAsSubAgg() throws Exception {
        Exception ex = expectThrows(
            Exception.class,
            () -> client().prepareSearch("idx")
                .addAggregation(
                    terms("terms").field("tag")
                        .order(BucketOrder.key(true))
                        .subAggregation(
                            histogram("histo").field(SINGLE_VALUED_FIELD_NAME)
                                .interval(interval)
                                .extendedBounds(minRandomValue, maxRandomValue)
                                .subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME))
                        )
                        .subAggregation(extendedStatsBucket("extended_stats_bucket", "histo>sum").sigma(-1.0))
                )
                .get()
        );
        Throwable cause = ExceptionsHelper.unwrapCause(ex);
        if (cause == null) {
            throw ex;
        } else if (cause instanceof SearchPhaseExecutionException) {
            SearchPhaseExecutionException spee = (SearchPhaseExecutionException) ex;
            Throwable rootCause = spee.getRootCause();
            if ((rootCause instanceof IllegalArgumentException) == false) {
                throw ex;
            }
        } else if ((cause instanceof IllegalArgumentException) == false) {
            throw ex;
        }
    }
}
