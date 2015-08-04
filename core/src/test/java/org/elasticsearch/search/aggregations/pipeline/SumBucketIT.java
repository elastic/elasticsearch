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
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilders.sumBucket;

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
public class SumBucketIT extends ESIntegTestCase {

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
                .addAggregation(sumBucket("sum_bucket").setBucketsPaths("histo>_count")).execute().actionGet();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = histo.getBuckets();
        assertThat(buckets.size(), equalTo(numValueBuckets));

        double sum = 0;
        for (int i = 0; i < numValueBuckets; ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) i * interval));
            assertThat(bucket.getDocCount(), equalTo(valueCounts[i]));
            sum += bucket.getDocCount();
        }

        InternalSimpleValue sumBucketValue = response.getAggregations().get("sum_bucket");
        assertThat(sumBucketValue, notNullValue());
        assertThat(sumBucketValue.getName(), equalTo("sum_bucket"));
        assertThat(sumBucketValue.value(), equalTo(sum));
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
                                .subAggregation(sumBucket("sum_bucket").setBucketsPaths("histo>_count"))).execute().actionGet();

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

            double sum = 0;
            for (int j = 0; j < numValueBuckets; ++j) {
                Histogram.Bucket bucket = buckets.get(j);
                assertThat(bucket, notNullValue());
                assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) j * interval));
                sum += bucket.getDocCount();
            }

            InternalSimpleValue sumBucketValue = termsBucket.getAggregations().get("sum_bucket");
            assertThat(sumBucketValue, notNullValue());
            assertThat(sumBucketValue.getName(), equalTo("sum_bucket"));
            assertThat(sumBucketValue.value(), equalTo(sum));
        }
    }

    @Test
    public void testMetric_topLevel() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(terms("terms").field("tag").subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME)))
                .addAggregation(sumBucket("sum_bucket").setBucketsPaths("terms>sum")).execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        List<Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(interval));

        double bucketSum = 0;
        for (int i = 0; i < interval; ++i) {
            Terms.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat((String) bucket.getKey(), equalTo("tag" + (i % interval)));
            assertThat(bucket.getDocCount(), greaterThan(0l));
            Sum sum = bucket.getAggregations().get("sum");
            assertThat(sum, notNullValue());
            bucketSum += sum.value();
        }

        InternalSimpleValue sumBucketValue = response.getAggregations().get("sum_bucket");
        assertThat(sumBucketValue, notNullValue());
        assertThat(sumBucketValue.getName(), equalTo("sum_bucket"));
        assertThat(sumBucketValue.value(), equalTo(bucketSum));
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
                                .subAggregation(sumBucket("sum_bucket").setBucketsPaths("histo>sum"))).execute().actionGet();

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

            double bucketSum = 0;
            for (int j = 0; j < numValueBuckets; ++j) {
                Histogram.Bucket bucket = buckets.get(j);
                assertThat(bucket, notNullValue());
                assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) j * interval));
                if (bucket.getDocCount() != 0) {
                    Sum sum = bucket.getAggregations().get("sum");
                    assertThat(sum, notNullValue());
                    bucketSum += sum.value();
                }
            }

            InternalSimpleValue sumBucketValue = termsBucket.getAggregations().get("sum_bucket");
            assertThat(sumBucketValue, notNullValue());
            assertThat(sumBucketValue.getName(), equalTo("sum_bucket"));
            assertThat(sumBucketValue.value(), equalTo(bucketSum));
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
                                .subAggregation(sumBucket("sum_bucket").setBucketsPaths("histo>sum").gapPolicy(GapPolicy.INSERT_ZEROS)))
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

            double bucketSum = 0;
            for (int j = 0; j < numValueBuckets; ++j) {
                Histogram.Bucket bucket = buckets.get(j);
                assertThat(bucket, notNullValue());
                assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) j * interval));
                Sum sum = bucket.getAggregations().get("sum");
                assertThat(sum, notNullValue());

                bucketSum += sum.value();
            }

            InternalSimpleValue sumBucketValue = termsBucket.getAggregations().get("sum_bucket");
            assertThat(sumBucketValue, notNullValue());
            assertThat(sumBucketValue.getName(), equalTo("sum_bucket"));
            assertThat(sumBucketValue.value(), equalTo(bucketSum));
        }
    }

    @Test
    public void testNoBuckets() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
                .addAggregation(terms("terms").field("tag").exclude("tag.*").subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME)))
                .addAggregation(sumBucket("sum_bucket").setBucketsPaths("terms>sum")).execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        List<Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets.size(), equalTo(0));

        InternalSimpleValue sumBucketValue = response.getAggregations().get("sum_bucket");
        assertThat(sumBucketValue, notNullValue());
        assertThat(sumBucketValue.getName(), equalTo("sum_bucket"));
        assertThat(sumBucketValue.value(), equalTo(0.0));
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
                                .subAggregation(sumBucket("sum_histo_bucket").setBucketsPaths("histo>_count")))
                .addAggregation(sumBucket("sum_terms_bucket").setBucketsPaths("terms>sum_histo_bucket")).execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        List<Terms.Bucket> termsBuckets = terms.getBuckets();
        assertThat(termsBuckets.size(), equalTo(interval));

        double aggTermsSum = 0;
        for (int i = 0; i < interval; ++i) {
            Terms.Bucket termsBucket = termsBuckets.get(i);
            assertThat(termsBucket, notNullValue());
            assertThat((String) termsBucket.getKey(), equalTo("tag" + (i % interval)));

            Histogram histo = termsBucket.getAggregations().get("histo");
            assertThat(histo, notNullValue());
            assertThat(histo.getName(), equalTo("histo"));
            List<? extends Bucket> buckets = histo.getBuckets();

            double aggHistoSum = 0;
            for (int j = 0; j < numValueBuckets; ++j) {
                Histogram.Bucket bucket = buckets.get(j);
                assertThat(bucket, notNullValue());
                assertThat(((Number) bucket.getKey()).longValue(), equalTo((long) j * interval));

                aggHistoSum += bucket.getDocCount();
            }

            InternalSimpleValue sumBucketValue = termsBucket.getAggregations().get("sum_histo_bucket");
            assertThat(sumBucketValue, notNullValue());
            assertThat(sumBucketValue.getName(), equalTo("sum_histo_bucket"));
            assertThat(sumBucketValue.value(), equalTo(aggHistoSum));

            aggTermsSum += aggHistoSum;
        }

        InternalSimpleValue sumBucketValue = response.getAggregations().get("sum_terms_bucket");
        assertThat(sumBucketValue, notNullValue());
        assertThat(sumBucketValue.getName(), equalTo("sum_terms_bucket"));
        assertThat(sumBucketValue.value(), equalTo(aggTermsSum));
    }
}
