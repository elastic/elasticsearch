/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.IncludeExclude;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.Percentile;
import org.elasticsearch.search.aggregations.metrics.Sum;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.function.IntToDoubleFunction;

import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.search.aggregations.PipelineAggregatorBuilders.percentilesBucket;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.IsNull.notNullValue;

public class PercentilesBucketIT extends BucketMetricsPipeLineAggregationTestCase<PercentilesBucket> {

    private static final double[] PERCENTS = { 0.0, 1.0, 25.0, 50.0, 75.0, 99.0, 100.0 };

    @Override
    protected PercentilesBucketPipelineAggregationBuilder BucketMetricsPipelineAgg(String name, String bucketsPath) {
        return percentilesBucket(name, bucketsPath).setPercents(PERCENTS);
    }

    @Override
    protected void assertResult(
        IntToDoubleFunction bucketValues,
        Function<Integer, String> bucketKeys,
        int numBuckets,
        PercentilesBucket pipelineBucket
    ) {
        double[] values = new double[numBuckets];
        for (int i = 0; i < numBuckets; ++i) {
            values[i] = bucketValues.applyAsDouble(i);
        }
        Arrays.sort(values);
        assertPercentileBucket(PERCENTS, values, pipelineBucket);
    }

    @Override
    protected String nestedMetric() {
        return "50";
    }

    @Override
    protected double getNestedMetric(PercentilesBucket bucket) {
        return bucket.percentile(50);
    }

    public void testMetricTopLevelDefaultPercents() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(terms(termsName).field("tag").subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME)))
            .addAggregation(percentilesBucket("percentiles_bucket", termsName + ">sum"))
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get(termsName);
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo(termsName));
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(interval));

        double[] values = new double[interval];
        for (int i = 0; i < interval; ++i) {
            Terms.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat((String) bucket.getKey(), equalTo("tag" + (i % interval)));
            assertThat(bucket.getDocCount(), greaterThan(0L));
            Sum sum = bucket.getAggregations().get("sum");
            assertThat(sum, notNullValue());
            values[i] = sum.value();
        }

        Arrays.sort(values);

        PercentilesBucket percentilesBucketValue = response.getAggregations().get("percentiles_bucket");
        assertThat(percentilesBucketValue, notNullValue());
        assertThat(percentilesBucketValue.getName(), equalTo("percentiles_bucket"));
        assertPercentileBucket(values, percentilesBucketValue);
    }

    public void testWrongPercents() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                terms(termsName).field("tag")
                    .includeExclude(new IncludeExclude(null, "tag.*", null, null))
                    .subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME))
            )
            .addAggregation(percentilesBucket("percentiles_bucket", termsName + ">sum").setPercents(PERCENTS))
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get(termsName);
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo(termsName));
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(0));

        PercentilesBucket percentilesBucketValue = response.getAggregations().get("percentiles_bucket");
        assertThat(percentilesBucketValue, notNullValue());
        assertThat(percentilesBucketValue.getName(), equalTo("percentiles_bucket"));

        try {
            percentilesBucketValue.percentile(2.0);
            fail("2.0 was not a valid percent, should have thrown exception");
        } catch (IllegalArgumentException exception) {
            // All good
        }
    }

    public void testBadPercents() throws Exception {
        double[] badPercents = { -1.0, 110.0 };

        try {
            client().prepareSearch("idx")
                .addAggregation(terms(termsName).field("tag").subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME)))
                .addAggregation(percentilesBucket("percentiles_bucket", termsName + ">sum").setPercents(badPercents))
                .get();

            fail("Illegal percent's were provided but no exception was thrown.");
        } catch (Exception e) {
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            if (cause == null) {
                throw e;
            } else if (cause instanceof SearchPhaseExecutionException) {
                SearchPhaseExecutionException spee = (SearchPhaseExecutionException) e;
                Throwable rootCause = spee.getRootCause();
                if ((rootCause instanceof IllegalArgumentException) == false) {
                    throw e;
                }
            } else if ((cause instanceof IllegalArgumentException) == false) {
                throw e;
            }
        }

    }

    public void testBadPercents_asSubAgg() throws Exception {
        double[] badPercents = { -1.0, 110.0 };

        try {
            client().prepareSearch("idx")
                .addAggregation(
                    terms(termsName).field("tag")
                        .order(BucketOrder.key(true))
                        .subAggregation(
                            histogram(histoName).field(SINGLE_VALUED_FIELD_NAME)
                                .interval(interval)
                                .extendedBounds(minRandomValue, maxRandomValue)
                        )
                        .subAggregation(percentilesBucket("percentiles_bucket", histoName + ">_count").setPercents(badPercents))
                )
                .get();

            fail("Illegal percent's were provided but no exception was thrown.");
        } catch (Exception e) {
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            if (cause == null) {
                throw e;
            } else if (cause instanceof SearchPhaseExecutionException) {
                SearchPhaseExecutionException spee = (SearchPhaseExecutionException) e;
                Throwable rootCause = spee.getRootCause();
                if ((rootCause instanceof IllegalArgumentException) == false) {
                    throw e;
                }
            } else if ((cause instanceof IllegalArgumentException) == false) {
                throw e;
            }
        }

    }

    public void testNestedWithDecimal() throws Exception {
        double[] percent = { 99.9 };
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                terms(termsName).field("tag")
                    .order(BucketOrder.key(true))
                    .subAggregation(
                        histogram(histoName).field(SINGLE_VALUED_FIELD_NAME)
                            .interval(interval)
                            .extendedBounds(minRandomValue, maxRandomValue)
                    )
                    .subAggregation(percentilesBucket("percentile_histo_bucket", histoName + ">_count").setPercents(percent))
            )
            .addAggregation(percentilesBucket("percentile_terms_bucket", termsName + ">percentile_histo_bucket[99.9]").setPercents(percent))
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get(termsName);
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo(termsName));
        List<? extends Terms.Bucket> termsBuckets = terms.getBuckets();
        assertThat(termsBuckets.size(), equalTo(interval));

        double[] values = new double[termsBuckets.size()];
        for (int i = 0; i < interval; ++i) {
            Terms.Bucket termsBucket = termsBuckets.get(i);
            assertThat(termsBucket, notNullValue());
            assertThat((String) termsBucket.getKey(), equalTo("tag" + (i % interval)));

            Histogram histo = termsBucket.getAggregations().get(histoName);
            assertThat(histo, notNullValue());
            assertThat(histo.getName(), equalTo(histoName));
            List<? extends Histogram.Bucket> buckets = histo.getBuckets();

            double[] innerValues = new double[numValueBuckets];
            for (int j = 0; j < numValueBuckets; ++j) {
                Histogram.Bucket bucket = buckets.get(j);
                assertThat(bucket, notNullValue());
                assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) j * interval));

                innerValues[j] = bucket.getDocCount();
            }
            Arrays.sort(innerValues);

            PercentilesBucket percentilesBucketValue = termsBucket.getAggregations().get("percentile_histo_bucket");
            assertThat(percentilesBucketValue, notNullValue());
            assertThat(percentilesBucketValue.getName(), equalTo("percentile_histo_bucket"));
            assertPercentileBucket(innerValues, percentilesBucketValue);
            values[i] = percentilesBucketValue.percentile(99.9);
        }

        Arrays.sort(values);

        PercentilesBucket percentilesBucketValue = response.getAggregations().get("percentile_terms_bucket");
        assertThat(percentilesBucketValue, notNullValue());
        assertThat(percentilesBucketValue.getName(), equalTo("percentile_terms_bucket"));
        for (Double p : percent) {
            double expected = values[(int) ((p / 100) * values.length)];
            assertThat(percentilesBucketValue.percentile(p), equalTo(expected));
        }
    }

    private void assertPercentileBucket(double[] values, PercentilesBucket percentiles) {
        for (Percentile percentile : percentiles) {
            assertEquals(percentiles.percentile(percentile.getPercent()), percentile.getValue(), 0d);
            if (values.length == 0) {
                assertThat(percentile.getValue(), equalTo(Double.NaN));
            } else {
                int index = (int) Math.round((percentile.getPercent() / 100.0) * (values.length - 1));
                assertThat(percentile.getValue(), equalTo(values[index]));
            }
        }
    }

    private void assertPercentileBucket(double[] percents, double[] values, PercentilesBucket percentiles) {
        Iterator<Percentile> it = percentiles.iterator();
        for (int i = 0; i < percents.length; ++i) {
            assertTrue(it.hasNext());
            assertEquals(percents[i], it.next().getPercent(), 0d);
        }
        assertFalse(it.hasNext());
        assertPercentileBucket(values, percentiles);
    }
}
