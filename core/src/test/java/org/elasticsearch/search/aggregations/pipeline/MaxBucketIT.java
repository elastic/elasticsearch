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
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Order;
import org.elasticsearch.search.aggregations.bucket.terms.support.IncludeExclude;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.InternalBucketMetricValue;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.filter;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilders.maxBucket;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.IsNull.notNullValue;

@ESIntegTestCase.SuiteScopeTestCase
public class MaxBucketIT extends ESIntegTestCase {

    private static final String SINGLE_VALUED_FIELD_NAME = "l_value";

    static int numDocs;
    static int interval;
    static int minRandomValue;
    static int maxRandomValue;
    static int numValueBuckets;
    static long[] valueCounts;

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("idx")
                .addMapping("type", "tag", "type=keyword").get());
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

    public void testDocCountTopLevel() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval)
                        .extendedBounds(minRandomValue, maxRandomValue))
                .addAggregation(maxBucket("max_bucket", "histo>_count")).execute().actionGet();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(numValueBuckets));

        List<String> maxKeys = new ArrayList<>();
        double maxValue = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < numValueBuckets; ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) i * interval));
            assertThat(bucket.getDocCount(), equalTo(valueCounts[i]));
            if (bucket.getDocCount() > maxValue) {
                maxValue = bucket.getDocCount();
                maxKeys = new ArrayList<>();
                maxKeys.add(bucket.getKeyAsString());
            } else if (bucket.getDocCount() == maxValue) {
                maxKeys.add(bucket.getKeyAsString());
            }
        }

        InternalBucketMetricValue maxBucketValue = response.getAggregations().get("max_bucket");
        assertThat(maxBucketValue, notNullValue());
        assertThat(maxBucketValue.getName(), equalTo("max_bucket"));
        assertThat(maxBucketValue.value(), equalTo(maxValue));
        assertThat(maxBucketValue.keys(), equalTo(maxKeys.toArray(new String[maxKeys.size()])));
    }

    public void testDocCountAsSubAgg() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        terms("terms")
                                .field("tag")
                                .order(Order.term(true))
                                .subAggregation(
                                        histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval)
                                .extendedBounds(minRandomValue, maxRandomValue))
                                .subAggregation(maxBucket("max_bucket", "histo>_count"))).execute().actionGet();

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

            List<String> maxKeys = new ArrayList<>();
            double maxValue = Double.NEGATIVE_INFINITY;
            for (int j = 0; j < numValueBuckets; ++j) {
                Histogram.Bucket bucket = buckets.get(j);
                assertThat(bucket, notNullValue());
                assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) j * interval));
                if (bucket.getDocCount() > maxValue) {
                    maxValue = bucket.getDocCount();
                    maxKeys = new ArrayList<>();
                    maxKeys.add(bucket.getKeyAsString());
                } else if (bucket.getDocCount() == maxValue) {
                    maxKeys.add(bucket.getKeyAsString());
                }
            }

            InternalBucketMetricValue maxBucketValue = termsBucket.getAggregations().get("max_bucket");
            assertThat(maxBucketValue, notNullValue());
            assertThat(maxBucketValue.getName(), equalTo("max_bucket"));
            assertThat(maxBucketValue.value(), equalTo(maxValue));
            assertThat(maxBucketValue.keys(), equalTo(maxKeys.toArray(new String[maxKeys.size()])));
        }
    }

    public void testMetricTopLevel() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(terms("terms").field("tag").subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME)))
                .addAggregation(maxBucket("max_bucket", "terms>sum")).execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        List<Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(interval));

        List<String> maxKeys = new ArrayList<>();
        double maxValue = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < interval; ++i) {
            Terms.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat((String) bucket.getKey(), equalTo("tag" + (i % interval)));
            assertThat(bucket.getDocCount(), greaterThan(0L));
            Sum sum = bucket.getAggregations().get("sum");
            assertThat(sum, notNullValue());
            if (sum.value() > maxValue) {
                maxValue = sum.value();
                maxKeys = new ArrayList<>();
                maxKeys.add(bucket.getKeyAsString());
            } else if (sum.value() == maxValue) {
                maxKeys.add(bucket.getKeyAsString());
            }
        }

        InternalBucketMetricValue maxBucketValue = response.getAggregations().get("max_bucket");
        assertThat(maxBucketValue, notNullValue());
        assertThat(maxBucketValue.getName(), equalTo("max_bucket"));
        assertThat(maxBucketValue.value(), equalTo(maxValue));
        assertThat(maxBucketValue.keys(), equalTo(maxKeys.toArray(new String[maxKeys.size()])));
    }

    public void testMetricAsSubAgg() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        terms("terms")
                                .field("tag")
                                .order(Order.term(true))
                                .subAggregation(
                                        histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval)
                                .extendedBounds(minRandomValue, maxRandomValue)
                                                .subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME)))
                                .subAggregation(maxBucket("max_bucket", "histo>sum"))).execute().actionGet();

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

            List<String> maxKeys = new ArrayList<>();
            double maxValue = Double.NEGATIVE_INFINITY;
            for (int j = 0; j < numValueBuckets; ++j) {
                Histogram.Bucket bucket = buckets.get(j);
                assertThat(bucket, notNullValue());
                assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) j * interval));
                if (bucket.getDocCount() != 0) {
                    Sum sum = bucket.getAggregations().get("sum");
                    assertThat(sum, notNullValue());
                    if (sum.value() > maxValue) {
                        maxValue = sum.value();
                        maxKeys = new ArrayList<>();
                        maxKeys.add(bucket.getKeyAsString());
                    } else if (sum.value() == maxValue) {
                        maxKeys.add(bucket.getKeyAsString());
                    }
                }
            }

            InternalBucketMetricValue maxBucketValue = termsBucket.getAggregations().get("max_bucket");
            assertThat(maxBucketValue, notNullValue());
            assertThat(maxBucketValue.getName(), equalTo("max_bucket"));
            assertThat(maxBucketValue.value(), equalTo(maxValue));
            assertThat(maxBucketValue.keys(), equalTo(maxKeys.toArray(new String[maxKeys.size()])));
        }
    }

    public void testMetricAsSubAggOfSingleBucketAgg() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        filter("filter", termQuery("tag", "tag0"))
                                .subAggregation(
                                        histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval)
                                .extendedBounds(minRandomValue, maxRandomValue)
                                                .subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME)))
                                .subAggregation(maxBucket("max_bucket", "histo>sum"))).execute().actionGet();

        assertSearchResponse(response);

        Filter filter = response.getAggregations().get("filter");
        assertThat(filter, notNullValue());
        assertThat(filter.getName(), equalTo("filter"));
        Histogram histo = filter.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();

        List<String> maxKeys = new ArrayList<>();
        double maxValue = Double.NEGATIVE_INFINITY;
        for (int j = 0; j < numValueBuckets; ++j) {
            Histogram.Bucket bucket = buckets.get(j);
            assertThat(bucket, notNullValue());
            assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) j * interval));
            if (bucket.getDocCount() != 0) {
                Sum sum = bucket.getAggregations().get("sum");
                assertThat(sum, notNullValue());
                if (sum.value() > maxValue) {
                    maxValue = sum.value();
                    maxKeys = new ArrayList<>();
                    maxKeys.add(bucket.getKeyAsString());
                } else if (sum.value() == maxValue) {
                    maxKeys.add(bucket.getKeyAsString());
                }
            }
        }

        InternalBucketMetricValue maxBucketValue = filter.getAggregations().get("max_bucket");
        assertThat(maxBucketValue, notNullValue());
        assertThat(maxBucketValue.getName(), equalTo("max_bucket"));
        assertThat(maxBucketValue.value(), equalTo(maxValue));
        assertThat(maxBucketValue.keys(), equalTo(maxKeys.toArray(new String[maxKeys.size()])));
    }

    public void testMetricAsSubAggWithInsertZeros() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        terms("terms")
                                .field("tag")
                                .order(Order.term(true))
                                .subAggregation(
                                        histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval)
                                .extendedBounds(minRandomValue, maxRandomValue)
                                                .subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME)))
                                .subAggregation(maxBucket("max_bucket", "histo>sum").gapPolicy(GapPolicy.INSERT_ZEROS)))
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

            List<String> maxKeys = new ArrayList<>();
            double maxValue = Double.NEGATIVE_INFINITY;
            for (int j = 0; j < numValueBuckets; ++j) {
                Histogram.Bucket bucket = buckets.get(j);
                assertThat(bucket, notNullValue());
                assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) j * interval));
                Sum sum = bucket.getAggregations().get("sum");
                assertThat(sum, notNullValue());
                if (sum.value() > maxValue) {
                    maxValue = sum.value();
                    maxKeys = new ArrayList<>();
                    maxKeys.add(bucket.getKeyAsString());
                } else if (sum.value() == maxValue) {
                    maxKeys.add(bucket.getKeyAsString());
                }
            }

            InternalBucketMetricValue maxBucketValue = termsBucket.getAggregations().get("max_bucket");
            assertThat(maxBucketValue, notNullValue());
            assertThat(maxBucketValue.getName(), equalTo("max_bucket"));
            assertThat(maxBucketValue.value(), equalTo(maxValue));
            assertThat(maxBucketValue.keys(), equalTo(maxKeys.toArray(new String[maxKeys.size()])));
        }
    }

    public void testNoBuckets() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(terms("terms").field("tag").includeExclude(new IncludeExclude(null, "tag.*"))
                        .subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME)))
                .addAggregation(maxBucket("max_bucket", "terms>sum")).execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        List<Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(0));

        InternalBucketMetricValue maxBucketValue = response.getAggregations().get("max_bucket");
        assertThat(maxBucketValue, notNullValue());
        assertThat(maxBucketValue.getName(), equalTo("max_bucket"));
        assertThat(maxBucketValue.value(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(maxBucketValue.keys(), equalTo(new String[0]));
    }

    public void testNested() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        terms("terms")
                                .field("tag")
                                .order(Order.term(true))
                                .subAggregation(
                                        histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval)
                                .extendedBounds(minRandomValue, maxRandomValue))
                                .subAggregation(maxBucket("max_histo_bucket", "histo>_count")))
                .addAggregation(maxBucket("max_terms_bucket", "terms>max_histo_bucket")).execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        List<Terms.Bucket> termsBuckets = terms.getBuckets();
        assertThat(termsBuckets.size(), equalTo(interval));

        List<String> maxTermsKeys = new ArrayList<>();
        double maxTermsValue = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < interval; ++i) {
            Terms.Bucket termsBucket = termsBuckets.get(i);
            assertThat(termsBucket, notNullValue());
            assertThat((String) termsBucket.getKey(), equalTo("tag" + (i % interval)));

            Histogram histo = termsBucket.getAggregations().get("histo");
            assertThat(histo, notNullValue());
            assertThat(histo.getName(), equalTo("histo"));
            List<? extends Bucket> buckets = histo.getBuckets();

            List<String> maxHistoKeys = new ArrayList<>();
            double maxHistoValue = Double.NEGATIVE_INFINITY;
            for (int j = 0; j < numValueBuckets; ++j) {
                Histogram.Bucket bucket = buckets.get(j);
                assertThat(bucket, notNullValue());
                assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) j * interval));
                if (bucket.getDocCount() > maxHistoValue) {
                    maxHistoValue = bucket.getDocCount();
                    maxHistoKeys = new ArrayList<>();
                    maxHistoKeys.add(bucket.getKeyAsString());
                } else if (bucket.getDocCount() == maxHistoValue) {
                    maxHistoKeys.add(bucket.getKeyAsString());
                }
            }

            InternalBucketMetricValue maxBucketValue = termsBucket.getAggregations().get("max_histo_bucket");
            assertThat(maxBucketValue, notNullValue());
            assertThat(maxBucketValue.getName(), equalTo("max_histo_bucket"));
            assertThat(maxBucketValue.value(), equalTo(maxHistoValue));
            assertThat(maxBucketValue.keys(), equalTo(maxHistoKeys.toArray(new String[maxHistoKeys.size()])));
            if (maxHistoValue > maxTermsValue) {
                maxTermsValue = maxHistoValue;
                maxTermsKeys = new ArrayList<>();
                maxTermsKeys.add(termsBucket.getKeyAsString());
            } else if (maxHistoValue == maxTermsValue) {
                maxTermsKeys.add(termsBucket.getKeyAsString());
            }
        }

        InternalBucketMetricValue maxBucketValue = response.getAggregations().get("max_terms_bucket");
        assertThat(maxBucketValue, notNullValue());
        assertThat(maxBucketValue.getName(), equalTo("max_terms_bucket"));
        assertThat(maxBucketValue.value(), equalTo(maxTermsValue));
        assertThat(maxBucketValue.keys(), equalTo(maxTermsKeys.toArray(new String[maxTermsKeys.size()])));
    }
}
