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

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Order;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.InternalBucketMetricValue;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilders.minBucket;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.IsNull.notNullValue;

@ESIntegTestCase.SuiteScopeTestCase
public class MinBucketIT extends ESIntegTestCase {

    private static final String SINGLE_VALUED_FIELD_NAME = "l_value";

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
                .addAggregation(minBucket("min_bucket").setBucketsPaths("histo>_count")).execute().actionGet();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(numValueBuckets));

        List<String> minKeys = new ArrayList<>();
        double minValue = Double.POSITIVE_INFINITY;
        for (int i = 0; i < numValueBuckets; ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) i * interval));
            assertThat(bucket.getDocCount(), equalTo(valueCounts[i]));
            if (bucket.getDocCount() < minValue) {
                minValue = bucket.getDocCount();
                minKeys = new ArrayList<>();
                minKeys.add(bucket.getKeyAsString());
            } else if (bucket.getDocCount() == minValue) {
                minKeys.add(bucket.getKeyAsString());
            }
        }

        InternalBucketMetricValue minBucketValue = response.getAggregations().get("min_bucket");
        assertThat(minBucketValue, notNullValue());
        assertThat(minBucketValue.getName(), equalTo("min_bucket"));
        assertThat(minBucketValue.value(), equalTo(minValue));
        assertThat(minBucketValue.keys(), equalTo(minKeys.toArray(new String[minKeys.size()])));
    }

    @Test
    public void testDocCount_asSubAgg() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        terms("terms")
                                .field("tag")
                                .order(Order.term(true))
                                .subAggregation(
                                        histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval)
                                                .extendedBounds((long) minRandomValue, (long) maxRandomValue))
                                .subAggregation(minBucket("min_bucket").setBucketsPaths("histo>_count"))).execute().actionGet();

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
            List<? extends Bucket> buckets = histo.getBuckets();

            List<String> minKeys = new ArrayList<>();
            double minValue = Double.POSITIVE_INFINITY;
            for (int j = 0; j < numValueBuckets; ++j) {
                Histogram.Bucket bucket = buckets.get(j);
                assertThat(bucket, notNullValue());
                assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) j * interval));
                if (bucket.getDocCount() < minValue) {
                    minValue = bucket.getDocCount();
                    minKeys = new ArrayList<>();
                    minKeys.add(bucket.getKeyAsString());
                } else if (bucket.getDocCount() == minValue) {
                    minKeys.add(bucket.getKeyAsString());
                }
            }

            InternalBucketMetricValue minBucketValue = termsBucket.getAggregations().get("min_bucket");
            assertThat(minBucketValue, notNullValue());
            assertThat(minBucketValue.getName(), equalTo("min_bucket"));
            assertThat(minBucketValue.value(), equalTo(minValue));
            assertThat(minBucketValue.keys(), equalTo(minKeys.toArray(new String[minKeys.size()])));
        }
    }

    @Test
    public void testMetric_topLevel() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(terms("terms").field("tag").subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME)))
                .addAggregation(minBucket("min_bucket").setBucketsPaths("terms>sum")).execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        List<Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(interval));

        List<String> minKeys = new ArrayList<>();
        double minValue = Double.POSITIVE_INFINITY;
        for (int i = 0; i < interval; ++i) {
            Terms.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat((String) bucket.getKey(), equalTo("tag" + (i % interval)));
            assertThat(bucket.getDocCount(), greaterThan(0l));
            Sum sum = bucket.getAggregations().get("sum");
            assertThat(sum, notNullValue());
            if (sum.value() < minValue) {
                minValue = sum.value();
                minKeys = new ArrayList<>();
                minKeys.add(bucket.getKeyAsString());
            } else if (sum.value() == minValue) {
                minKeys.add(bucket.getKeyAsString());
            }
        }

        InternalBucketMetricValue minBucketValue = response.getAggregations().get("min_bucket");
        assertThat(minBucketValue, notNullValue());
        assertThat(minBucketValue.getName(), equalTo("min_bucket"));
        assertThat(minBucketValue.value(), equalTo(minValue));
        assertThat(minBucketValue.keys(), equalTo(minKeys.toArray(new String[minKeys.size()])));
    }

    @Test
    public void testMetric_asSubAgg() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        terms("terms")
                                .field("tag")
                                .order(Order.term(true))
                                .subAggregation(
                                        histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval)
                                                .extendedBounds((long) minRandomValue, (long) maxRandomValue)
                                                .subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME)))
                                .subAggregation(minBucket("min_bucket").setBucketsPaths("histo>sum"))).execute().actionGet();

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
            List<? extends Bucket> buckets = histo.getBuckets();

            List<String> minKeys = new ArrayList<>();
            double minValue = Double.POSITIVE_INFINITY;
            for (int j = 0; j < numValueBuckets; ++j) {
                Histogram.Bucket bucket = buckets.get(j);
                assertThat(bucket, notNullValue());
                assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) j * interval));
                if (bucket.getDocCount() != 0) {
                    Sum sum = bucket.getAggregations().get("sum");
                    assertThat(sum, notNullValue());
                    if (sum.value() < minValue) {
                        minValue = sum.value();
                        minKeys = new ArrayList<>();
                        minKeys.add(bucket.getKeyAsString());
                    } else if (sum.value() == minValue) {
                        minKeys.add(bucket.getKeyAsString());
                    }
                }
            }

            InternalBucketMetricValue minBucketValue = termsBucket.getAggregations().get("min_bucket");
            assertThat(minBucketValue, notNullValue());
            assertThat(minBucketValue.getName(), equalTo("min_bucket"));
            assertThat(minBucketValue.value(), equalTo(minValue));
            assertThat(minBucketValue.keys(), equalTo(minKeys.toArray(new String[minKeys.size()])));
        }
    }

    @Test
    public void testMetric_asSubAggWithInsertZeros() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        terms("terms")
                                .field("tag")
                                .order(Order.term(true))
                                .subAggregation(
                                        histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval)
                                                .extendedBounds((long) minRandomValue, (long) maxRandomValue)
                                                .subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME)))
                                .subAggregation(minBucket("min_bucket").setBucketsPaths("histo>sum").gapPolicy(GapPolicy.INSERT_ZEROS)))
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
            List<? extends Bucket> buckets = histo.getBuckets();

            List<String> minKeys = new ArrayList<>();
            double minValue = Double.POSITIVE_INFINITY;
            for (int j = 0; j < numValueBuckets; ++j) {
                Histogram.Bucket bucket = buckets.get(j);
                assertThat(bucket, notNullValue());
                assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) j * interval));
                Sum sum = bucket.getAggregations().get("sum");
                assertThat(sum, notNullValue());
                if (sum.value() < minValue) {
                    minValue = sum.value();
                    minKeys = new ArrayList<>();
                    minKeys.add(bucket.getKeyAsString());
                } else if (sum.value() == minValue) {
                    minKeys.add(bucket.getKeyAsString());
                }
            }

            InternalBucketMetricValue minBucketValue = termsBucket.getAggregations().get("min_bucket");
            assertThat(minBucketValue, notNullValue());
            assertThat(minBucketValue.getName(), equalTo("min_bucket"));
            assertThat(minBucketValue.value(), equalTo(minValue));
            assertThat(minBucketValue.keys(), equalTo(minKeys.toArray(new String[minKeys.size()])));
        }
    }

    @Test
    public void testNoBuckets() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(terms("terms").field("tag").exclude("tag.*").subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME)))
                .addAggregation(minBucket("min_bucket").setBucketsPaths("terms>sum")).execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        List<Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(0));

        InternalBucketMetricValue minBucketValue = response.getAggregations().get("min_bucket");
        assertThat(minBucketValue, notNullValue());
        assertThat(minBucketValue.getName(), equalTo("min_bucket"));
        assertThat(minBucketValue.value(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(minBucketValue.keys(), equalTo(new String[0]));
    }

    @Test
    public void testNested() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        terms("terms")
                                .field("tag")
                                .order(Order.term(true))
                                .subAggregation(
                                        histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval)
                                                .extendedBounds((long) minRandomValue, (long) maxRandomValue))
                                .subAggregation(minBucket("min_histo_bucket").setBucketsPaths("histo>_count")))
                .addAggregation(minBucket("min_terms_bucket").setBucketsPaths("terms>min_histo_bucket")).execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        List<Terms.Bucket> termsBuckets = terms.getBuckets();
        assertThat(termsBuckets.size(), equalTo(interval));

        List<String> minTermsKeys = new ArrayList<>();
        double minTermsValue = Double.POSITIVE_INFINITY;
        for (int i = 0; i < interval; ++i) {
            Terms.Bucket termsBucket = termsBuckets.get(i);
            assertThat(termsBucket, notNullValue());
            assertThat((String) termsBucket.getKey(), equalTo("tag" + (i % interval)));

            Histogram histo = termsBucket.getAggregations().get("histo");
            assertThat(histo, notNullValue());
            assertThat(histo.getName(), equalTo("histo"));
            List<? extends Bucket> buckets = histo.getBuckets();

            List<String> minHistoKeys = new ArrayList<>();
            double minHistoValue = Double.POSITIVE_INFINITY;
            for (int j = 0; j < numValueBuckets; ++j) {
                Histogram.Bucket bucket = buckets.get(j);
                assertThat(bucket, notNullValue());
                assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) j * interval));
                if (bucket.getDocCount() < minHistoValue) {
                    minHistoValue = bucket.getDocCount();
                    minHistoKeys = new ArrayList<>();
                    minHistoKeys.add(bucket.getKeyAsString());
                } else if (bucket.getDocCount() == minHistoValue) {
                    minHistoKeys.add(bucket.getKeyAsString());
                }
            }

            InternalBucketMetricValue minBucketValue = termsBucket.getAggregations().get("min_histo_bucket");
            assertThat(minBucketValue, notNullValue());
            assertThat(minBucketValue.getName(), equalTo("min_histo_bucket"));
            assertThat(minBucketValue.value(), equalTo(minHistoValue));
            assertThat(minBucketValue.keys(), equalTo(minHistoKeys.toArray(new String[minHistoKeys.size()])));
            if (minHistoValue < minTermsValue) {
                minTermsValue = minHistoValue;
                minTermsKeys = new ArrayList<>();
                minTermsKeys.add(termsBucket.getKeyAsString());
            } else if (minHistoValue == minTermsValue) {
                minTermsKeys.add(termsBucket.getKeyAsString());
            }
        }

        InternalBucketMetricValue minBucketValue = response.getAggregations().get("min_terms_bucket");
        assertThat(minBucketValue, notNullValue());
        assertThat(minBucketValue.getName(), equalTo("min_terms_bucket"));
        assertThat(minBucketValue.value(), equalTo(minTermsValue));
        assertThat(minBucketValue.keys(), equalTo(minTermsKeys.toArray(new String[minTermsKeys.size()])));
    }
}
