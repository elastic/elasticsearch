package org.elasticsearch.search.aggregations.pipeline;

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentile;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.percentile.PercentilesBucket;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilders.percentilesBucket;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.IsNull.notNullValue;

@ESIntegTestCase.SuiteScopeTestCase
public class PercentilesBucketIT extends ESIntegTestCase {

    private static final String SINGLE_VALUED_FIELD_NAME = "l_value";
    private static final Double[] PERCENTS = {1.0, 25.0, 50.0, 75.0, 99.0};
    static int numDocs;
    static int interval;
    static int minRandomValue;
    static int maxRandomValue;
    static int numValueBuckets;
    static long[] valueCounts;

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        createIndex("idx_unmapped");

        numDocs = randomIntBetween(6, 20);
        interval = randomIntBetween(2, 5);

        minRandomValue = 0;
        maxRandomValue = 20;

        numValueBuckets = ((maxRandomValue - minRandomValue) / interval) + 1;
        valueCounts = new long[numValueBuckets];

        List<IndexRequestBuilder> builders = new ArrayList<>();

        for (int i = 0; i < numDocs; i++) {
            int fieldValue = randomIntBetween(minRandomValue, maxRandomValue);
            builders.add(client().prepareIndex("idx", "type").setSource(
                    jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, fieldValue).field("tag", "tag" + (i % interval))
                            .endObject()));
            final int bucket = (fieldValue / interval); // + (fieldValue < 0 ? -1 : 0) - (minRandomValue / interval - 1);
            valueCounts[bucket]++;
        }

        assertAcked(prepareCreate("empty_bucket_idx").addMapping("type", SINGLE_VALUED_FIELD_NAME, "type=integer"));
        for (int i = 0; i < 2; i++) {
            builders.add(client().prepareIndex("empty_bucket_idx", "type", "" + i).setSource(
                    jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, i * 2).endObject()));
        }
        indexRandom(true, builders);
        ensureSearchable();
    }

    @Test
    public void testDocCount_topLevel() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval)
                        .extendedBounds((long) minRandomValue, (long) maxRandomValue))
                .addAggregation(percentilesBucket("percentiles_bucket")
                        .setBucketsPaths("histo>_count")
                        .percents(PERCENTS)).execute().actionGet();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Histogram.Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(numValueBuckets));

        double[] values = new double[numValueBuckets];
        for (int i = 0; i < numValueBuckets; ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) i * interval));
            assertThat(bucket.getDocCount(), equalTo(valueCounts[i]));
            values[i] = bucket.getDocCount();
        }

        Arrays.sort(values);

        PercentilesBucket percentilesBucketValue = response.getAggregations().get("percentiles_bucket");
        assertThat(percentilesBucketValue, notNullValue());
        assertThat(percentilesBucketValue.getName(), equalTo("percentiles_bucket"));
        for (Double p : PERCENTS) {
            double expected = values[(int)((p / 100) * values.length)];
            assertThat(percentilesBucketValue.percentile(p), equalTo(expected));
        }

    }

    @Test
    public void testDocCount_asSubAgg() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        terms("terms")
                                .field("tag")
                                .order(Terms.Order.term(true))
                                .subAggregation(
                                        histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval)
                                                .extendedBounds((long) minRandomValue, (long) maxRandomValue))
                                .subAggregation(percentilesBucket("percentiles_bucket")
                                        .setBucketsPaths("histo>_count")
                                        .percents(PERCENTS))).execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        List<Terms.Bucket> termsBuckets = terms.getBuckets();
        assertThat(termsBuckets.size(), equalTo(interval));

        for (int i = 0; i < interval; ++i) {
            Terms.Bucket termsBucket = termsBuckets.get(i);
            assertThat(termsBucket, notNullValue());
            assertThat((String) termsBucket.getKey(), equalTo("tag" + (i % interval)));

            Histogram histo = termsBucket.getAggregations().get("histo");
            assertThat(histo, notNullValue());
            assertThat(histo.getName(), equalTo("histo"));
            List<? extends Histogram.Bucket> buckets = histo.getBuckets();

            double[] values = new double[numValueBuckets];
            for (int j = 0; j < numValueBuckets; ++j) {
                Histogram.Bucket bucket = buckets.get(j);
                assertThat(bucket, notNullValue());
                assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) j * interval));
                values[j] = bucket.getDocCount();
            }

            Arrays.sort(values);

            PercentilesBucket percentilesBucketValue = termsBucket.getAggregations().get("percentiles_bucket");
            assertThat(percentilesBucketValue, notNullValue());
            assertThat(percentilesBucketValue.getName(), equalTo("percentiles_bucket"));
            for (Double p : PERCENTS) {
                double expected = values[(int)((p / 100) * values.length)];
                assertThat(percentilesBucketValue.percentile(p), equalTo(expected));
            }
        }
    }

    @Test
    public void testMetric_topLevel() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(terms("terms").field("tag").subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME)))
                .addAggregation(percentilesBucket("percentiles_bucket")
                        .setBucketsPaths("terms>sum")
                        .percents(PERCENTS)).execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        List<Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(interval));

        double[] values = new double[interval];
        for (int i = 0; i < interval; ++i) {
            Terms.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat((String) bucket.getKey(), equalTo("tag" + (i % interval)));
            assertThat(bucket.getDocCount(), greaterThan(0l));
            Sum sum = bucket.getAggregations().get("sum");
            assertThat(sum, notNullValue());
            values[i] = sum.value();
        }

        Arrays.sort(values);

        PercentilesBucket percentilesBucketValue = response.getAggregations().get("percentiles_bucket");
        assertThat(percentilesBucketValue, notNullValue());
        assertThat(percentilesBucketValue.getName(), equalTo("percentiles_bucket"));
        for (Double p : PERCENTS) {
            double expected = values[(int)((p / 100) * values.length)];
            assertThat(percentilesBucketValue.percentile(p), equalTo(expected));
        }
    }

    @Test
    public void testMetric_topLevelDefaultPercents() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(terms("terms").field("tag").subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME)))
                .addAggregation(percentilesBucket("percentiles_bucket")
                        .setBucketsPaths("terms>sum")).execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        List<Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(interval));

        double[] values = new double[interval];
        for (int i = 0; i < interval; ++i) {
            Terms.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat((String) bucket.getKey(), equalTo("tag" + (i % interval)));
            assertThat(bucket.getDocCount(), greaterThan(0l));
            Sum sum = bucket.getAggregations().get("sum");
            assertThat(sum, notNullValue());
            values[i] = sum.value();
        }

        Arrays.sort(values);

        PercentilesBucket percentilesBucketValue = response.getAggregations().get("percentiles_bucket");
        assertThat(percentilesBucketValue, notNullValue());
        assertThat(percentilesBucketValue.getName(), equalTo("percentiles_bucket"));
        for (Percentile p : percentilesBucketValue) {
            double expected = values[(int)((p.getPercent() / 100) * values.length)];
            assertThat(percentilesBucketValue.percentile(p.getPercent()), equalTo(expected));
            assertThat(p.getValue(), equalTo(expected));
        }
    }

    @Test
    public void testMetric_asSubAgg() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        terms("terms")
                                .field("tag")
                                .order(Terms.Order.term(true))
                                .subAggregation(
                                        histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval)
                                                .extendedBounds((long) minRandomValue, (long) maxRandomValue)
                                                .subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME)))
                                .subAggregation(percentilesBucket("percentiles_bucket")
                                        .setBucketsPaths("histo>sum")
                                        .percents(PERCENTS))).execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        List<Terms.Bucket> termsBuckets = terms.getBuckets();
        assertThat(termsBuckets.size(), equalTo(interval));

        for (int i = 0; i < interval; ++i) {
            Terms.Bucket termsBucket = termsBuckets.get(i);
            assertThat(termsBucket, notNullValue());
            assertThat((String) termsBucket.getKey(), equalTo("tag" + (i % interval)));

            Histogram histo = termsBucket.getAggregations().get("histo");
            assertThat(histo, notNullValue());
            assertThat(histo.getName(), equalTo("histo"));
            List<? extends Histogram.Bucket> buckets = histo.getBuckets();

            List<Double> values = new ArrayList<>(numValueBuckets);
            for (int j = 0; j < numValueBuckets; ++j) {
                Histogram.Bucket bucket = buckets.get(j);
                assertThat(bucket, notNullValue());
                assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) j * interval));
                if (bucket.getDocCount() != 0) {
                    Sum sum = bucket.getAggregations().get("sum");
                    assertThat(sum, notNullValue());
                    values.add(sum.value());
                }
            }

            Collections.sort(values);

            PercentilesBucket percentilesBucketValue = termsBucket.getAggregations().get("percentiles_bucket");
            assertThat(percentilesBucketValue, notNullValue());
            assertThat(percentilesBucketValue.getName(), equalTo("percentiles_bucket"));
            for (Double p : PERCENTS) {
                double expected = values.get((int) ((p / 100) * values.size()));
                assertThat(percentilesBucketValue.percentile(p), equalTo(expected));
            }
        }
    }

    @Test
    public void testMetric_asSubAggWithInsertZeros() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        terms("terms")
                                .field("tag")
                                .order(Terms.Order.term(true))
                                .subAggregation(
                                        histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval)
                                                .extendedBounds((long) minRandomValue, (long) maxRandomValue)
                                                .subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME)))
                                .subAggregation(percentilesBucket("percentiles_bucket")
                                        .setBucketsPaths("histo>sum")
                                        .gapPolicy(BucketHelpers.GapPolicy.INSERT_ZEROS)
                                        .percents(PERCENTS)))
                .execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        List<Terms.Bucket> termsBuckets = terms.getBuckets();
        assertThat(termsBuckets.size(), equalTo(interval));

        for (int i = 0; i < interval; ++i) {
            Terms.Bucket termsBucket = termsBuckets.get(i);
            assertThat(termsBucket, notNullValue());
            assertThat((String) termsBucket.getKey(), equalTo("tag" + (i % interval)));

            Histogram histo = termsBucket.getAggregations().get("histo");
            assertThat(histo, notNullValue());
            assertThat(histo.getName(), equalTo("histo"));
            List<? extends Histogram.Bucket> buckets = histo.getBuckets();

            double[] values = new double[numValueBuckets];
            for (int j = 0; j < numValueBuckets; ++j) {
                Histogram.Bucket bucket = buckets.get(j);
                assertThat(bucket, notNullValue());
                assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) j * interval));
                Sum sum = bucket.getAggregations().get("sum");
                assertThat(sum, notNullValue());

                values[j] = sum.value();
            }

            Arrays.sort(values);

            PercentilesBucket percentilesBucketValue = termsBucket.getAggregations().get("percentiles_bucket");
            assertThat(percentilesBucketValue, notNullValue());
            assertThat(percentilesBucketValue.getName(), equalTo("percentiles_bucket"));
            for (Double p : PERCENTS) {
                double expected = values[(int)((p / 100) * values.length)];
                assertThat(percentilesBucketValue.percentile(p), equalTo(expected));
            }
        }
    }

    @Test
    public void testNoBuckets() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(terms("terms").field("tag").exclude("tag.*").subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME)))
                .addAggregation(percentilesBucket("percentiles_bucket")
                        .setBucketsPaths("terms>sum")
                        .percents(PERCENTS)).execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        List<Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(0));

        PercentilesBucket percentilesBucketValue = response.getAggregations().get("percentiles_bucket");
        assertThat(percentilesBucketValue, notNullValue());
        assertThat(percentilesBucketValue.getName(), equalTo("percentiles_bucket"));
        for (Double p : PERCENTS) {
            assertThat(percentilesBucketValue.percentile(p), equalTo(Double.NaN));
        }
    }

    @Test
    public void testWrongPercents() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(terms("terms").field("tag").exclude("tag.*").subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME)))
                .addAggregation(percentilesBucket("percentiles_bucket")
                        .setBucketsPaths("terms>sum")
                        .percents(PERCENTS)).execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        List<Terms.Bucket> buckets = terms.getBuckets();
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

    @Test
    public void testBadPercents() throws Exception {
        Double[] badPercents = {-1.0, 110.0};

        try {
            client().prepareSearch("idx")
                    .addAggregation(terms("terms").field("tag").subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME)))
                    .addAggregation(percentilesBucket("percentiles_bucket")
                            .setBucketsPaths("terms>sum")
                            .percents(badPercents)).execute().actionGet();

            fail("Illegal percent's were provided but no exception was thrown.");
        } catch (SearchPhaseExecutionException exception) {
            ElasticsearchException[] rootCauses = exception.guessRootCauses();
            assertThat(rootCauses.length, equalTo(1));
            ElasticsearchException rootCause = rootCauses[0];
            assertThat(rootCause.getMessage(), containsString("must only contain non-null doubles from 0.0-100.0 inclusive"));
        }

    }

    @Test
    public void testBadPercents_asSubAgg() throws Exception {
        Double[] badPercents = {-1.0, 110.0};

        try {
            client()
                .prepareSearch("idx")
                .addAggregation(
                        terms("terms")
                                .field("tag")
                                .order(Terms.Order.term(true))
                                .subAggregation(
                                        histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval)
                                                .extendedBounds((long) minRandomValue, (long) maxRandomValue))
                                .subAggregation(percentilesBucket("percentiles_bucket")
                                        .setBucketsPaths("histo>_count")
                                        .percents(badPercents))).execute().actionGet();

            fail("Illegal percent's were provided but no exception was thrown.");
        } catch (SearchPhaseExecutionException exception) {
            ElasticsearchException[] rootCauses = exception.guessRootCauses();
            assertThat(rootCauses.length, equalTo(1));
            ElasticsearchException rootCause = rootCauses[0];
            assertThat(rootCause.getMessage(), containsString("must only contain non-null doubles from 0.0-100.0 inclusive"));
        }

    }

    @Test
    public void testNested() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        terms("terms")
                                .field("tag")
                                .order(Terms.Order.term(true))
                                .subAggregation(
                                        histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval)
                                                .extendedBounds((long) minRandomValue, (long) maxRandomValue))
                                .subAggregation(percentilesBucket("percentile_histo_bucket").setBucketsPaths("histo>_count")))
                .addAggregation(percentilesBucket("percentile_terms_bucket")
                        .setBucketsPaths("terms>percentile_histo_bucket.50")
                        .percents(PERCENTS)).execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        List<Terms.Bucket> termsBuckets = terms.getBuckets();
        assertThat(termsBuckets.size(), equalTo(interval));

        double[] values = new double[termsBuckets.size()];
        for (int i = 0; i < interval; ++i) {
            Terms.Bucket termsBucket = termsBuckets.get(i);
            assertThat(termsBucket, notNullValue());
            assertThat((String) termsBucket.getKey(), equalTo("tag" + (i % interval)));

            Histogram histo = termsBucket.getAggregations().get("histo");
            assertThat(histo, notNullValue());
            assertThat(histo.getName(), equalTo("histo"));
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
            for (Double p : PERCENTS) {
                double expected = innerValues[(int)((p / 100) * innerValues.length)];
                assertThat(percentilesBucketValue.percentile(p), equalTo(expected));
            }
            values[i] = percentilesBucketValue.percentile(50.0);
        }

        Arrays.sort(values);

        PercentilesBucket percentilesBucketValue = response.getAggregations().get("percentile_terms_bucket");
        assertThat(percentilesBucketValue, notNullValue());
        assertThat(percentilesBucketValue.getName(), equalTo("percentile_terms_bucket"));
        for (Double p : PERCENTS) {
            double expected = values[(int)((p / 100) * values.length)];
            assertThat(percentilesBucketValue.percentile(p), equalTo(expected));
        }
    }

    @Test
    public void testNestedWithDecimal() throws Exception {
        Double[] percent = {99.9};
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        terms("terms")
                                .field("tag")
                                .order(Terms.Order.term(true))
                                .subAggregation(
                                        histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval)
                                                .extendedBounds((long) minRandomValue, (long) maxRandomValue))
                                .subAggregation(percentilesBucket("percentile_histo_bucket")
                                        .percents(percent)
                                        .setBucketsPaths("histo>_count")))
                .addAggregation(percentilesBucket("percentile_terms_bucket")
                        .setBucketsPaths("terms>percentile_histo_bucket[99.9]")
                        .percents(percent)).execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        List<Terms.Bucket> termsBuckets = terms.getBuckets();
        assertThat(termsBuckets.size(), equalTo(interval));

        double[] values = new double[termsBuckets.size()];
        for (int i = 0; i < interval; ++i) {
            Terms.Bucket termsBucket = termsBuckets.get(i);
            assertThat(termsBucket, notNullValue());
            assertThat((String) termsBucket.getKey(), equalTo("tag" + (i % interval)));

            Histogram histo = termsBucket.getAggregations().get("histo");
            assertThat(histo, notNullValue());
            assertThat(histo.getName(), equalTo("histo"));
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
            for (Double p : percent) {
                double expected = innerValues[(int)((p / 100) * innerValues.length)];
                assertThat(percentilesBucketValue.percentile(p), equalTo(expected));
            }
            values[i] = percentilesBucketValue.percentile(99.9);
        }

        Arrays.sort(values);

        PercentilesBucket percentilesBucketValue = response.getAggregations().get("percentile_terms_bucket");
        assertThat(percentilesBucketValue, notNullValue());
        assertThat(percentilesBucketValue.getName(), equalTo("percentile_terms_bucket"));
        for (Double p : percent) {
            double expected = values[(int)((p / 100) * values.length)];
            assertThat(percentilesBucketValue.percentile(p), equalTo(expected));
        }
    }
}
