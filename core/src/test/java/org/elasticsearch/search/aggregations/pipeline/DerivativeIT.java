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

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram.Bucket;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.pipeline.derivative.Derivative;
import org.elasticsearch.search.aggregations.pipeline.movavg.models.SimpleModel;
import org.elasticsearch.search.aggregations.support.AggregationPath;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.filters;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.stats;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.search.aggregations.AggregationBuilders.avg;
import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilders.derivative;
import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorBuilders.movingAvg;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;

@ESIntegTestCase.SuiteScopeTestCase
public class DerivativeIT extends ESIntegTestCase {

    private static final String SINGLE_VALUED_FIELD_NAME = "l_value";

    private static int interval;
    private static int numValueBuckets;
    private static int numFirstDerivValueBuckets;
    private static int numSecondDerivValueBuckets;
    private static long[] valueCounts;
    private static long[] firstDerivValueCounts;
    private static long[] secondDerivValueCounts;

    private static Long[] valueCounts_empty;
    private static long numDocsEmptyIdx;
    private static Double[] firstDerivValueCounts_empty;

    // expected bucket values for random setup with gaps
    private static int numBuckets_empty_rnd;
    private static Long[] valueCounts_empty_rnd;
    private static Double[] firstDerivValueCounts_empty_rnd;
    private static long numDocsEmptyIdx_rnd;

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        createIndex("idx_unmapped");

        interval = 5;
        numValueBuckets = randomIntBetween(6, 80);

        valueCounts = new long[numValueBuckets];
        for (int i = 0; i < numValueBuckets; i++) {
            valueCounts[i] = randomIntBetween(1, 20);
        }

        numFirstDerivValueBuckets = numValueBuckets - 1;
        firstDerivValueCounts = new long[numFirstDerivValueBuckets];
        Long lastValueCount = null;
        for (int i = 0; i < numValueBuckets; i++) {
            long thisValue = valueCounts[i];
            if (lastValueCount != null) {
                long diff = thisValue - lastValueCount;
                firstDerivValueCounts[i - 1] = diff;
            }
            lastValueCount = thisValue;
        }

        numSecondDerivValueBuckets = numFirstDerivValueBuckets - 1;
        secondDerivValueCounts = new long[numSecondDerivValueBuckets];
        Long lastFirstDerivativeValueCount = null;
        for (int i = 0; i < numFirstDerivValueBuckets; i++) {
            long thisFirstDerivativeValue = firstDerivValueCounts[i];
            if (lastFirstDerivativeValueCount != null) {
                long diff = thisFirstDerivativeValue - lastFirstDerivativeValueCount;
                secondDerivValueCounts[i - 1] = diff;
            }
            lastFirstDerivativeValueCount = thisFirstDerivativeValue;
        }

        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < numValueBuckets; i++) {
            for (int docs = 0; docs < valueCounts[i]; docs++) {
                builders.add(client().prepareIndex("idx", "type").setSource(newDocBuilder(i * interval)));
            }
        }

        // setup for index with empty buckets
        valueCounts_empty = new Long[] { 1L, 1L, 2L, 0L, 2L, 2L, 0L, 0L, 0L, 3L, 2L, 1L };
        firstDerivValueCounts_empty = new Double[] { null, 0d, 1d, -2d, 2d, 0d, -2d, 0d, 0d, 3d, -1d, -1d };

        assertAcked(prepareCreate("empty_bucket_idx").addMapping("type", SINGLE_VALUED_FIELD_NAME, "type=integer"));
        for (int i = 0; i < valueCounts_empty.length; i++) {
            for (int docs = 0; docs < valueCounts_empty[i]; docs++) {
                builders.add(client().prepareIndex("empty_bucket_idx", "type").setSource(newDocBuilder(i)));
                numDocsEmptyIdx++;
            }
        }

        // randomized setup for index with empty buckets
        numBuckets_empty_rnd = randomIntBetween(20, 100);
        valueCounts_empty_rnd = new Long[numBuckets_empty_rnd];
        firstDerivValueCounts_empty_rnd = new Double[numBuckets_empty_rnd];
        firstDerivValueCounts_empty_rnd[0] = null;

        assertAcked(prepareCreate("empty_bucket_idx_rnd").addMapping("type", SINGLE_VALUED_FIELD_NAME, "type=integer"));
        for (int i = 0; i < numBuckets_empty_rnd; i++) {
            valueCounts_empty_rnd[i] = (long) randomIntBetween(1, 10);
            // make approximately half of the buckets empty
            if (randomBoolean())
                valueCounts_empty_rnd[i] = 0L;
            for (int docs = 0; docs < valueCounts_empty_rnd[i]; docs++) {
                builders.add(client().prepareIndex("empty_bucket_idx_rnd", "type").setSource(newDocBuilder(i)));
                numDocsEmptyIdx_rnd++;
            }
            if (i > 0) {
                firstDerivValueCounts_empty_rnd[i] = (double) valueCounts_empty_rnd[i] - valueCounts_empty_rnd[i - 1];
            }
        }

        indexRandom(true, builders);
        ensureSearchable();
    }

    private XContentBuilder newDocBuilder(int singleValueFieldValue) throws IOException {
        return jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, singleValueFieldValue).endObject();
    }

    /**
     * test first and second derivative on the sing
     */
    public void testDocCountDerivative() {

        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval)
                                .subAggregation(derivative("deriv", "_count"))
                                .subAggregation(derivative("2nd_deriv", "deriv"))).execute().actionGet();

        assertSearchResponse(response);

        Histogram deriv = response.getAggregations().get("histo");
        assertThat(deriv, notNullValue());
        assertThat(deriv.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = deriv.getBuckets();
        assertThat(buckets.size(), equalTo(numValueBuckets));

        for (int i = 0; i < numValueBuckets; ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            checkBucketKeyAndDocCount("InternalBucket " + i, bucket, i * interval, valueCounts[i]);
            SimpleValue docCountDeriv = bucket.getAggregations().get("deriv");
            if (i > 0) {
                assertThat(docCountDeriv, notNullValue());
                assertThat(docCountDeriv.value(), equalTo((double) firstDerivValueCounts[i - 1]));
            } else {
                assertThat(docCountDeriv, nullValue());
            }
            SimpleValue docCount2ndDeriv = bucket.getAggregations().get("2nd_deriv");
            if (i > 1) {
                assertThat(docCount2ndDeriv, notNullValue());
                assertThat(docCount2ndDeriv.value(), equalTo((double) secondDerivValueCounts[i - 2]));
            } else {
                assertThat(docCount2ndDeriv, nullValue());
            }
        }
    }

    /**
     * test first and second derivative on the sing
     */
    public void testSingleValuedField_normalised() {
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval).minDocCount(0)
                                .subAggregation(derivative("deriv", "_count").unit("1ms"))
                                .subAggregation(derivative("2nd_deriv", "deriv").unit("10ms"))).execute().actionGet();

        assertSearchResponse(response);

        Histogram deriv = response.getAggregations().get("histo");
        assertThat(deriv, notNullValue());
        assertThat(deriv.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = deriv.getBuckets();
        assertThat(buckets.size(), equalTo(numValueBuckets));

        for (int i = 0; i < numValueBuckets; ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            checkBucketKeyAndDocCount("InternalBucket " + i, bucket, i * interval, valueCounts[i]);
            Derivative docCountDeriv = bucket.getAggregations().get("deriv");
            if (i > 0) {
                assertThat(docCountDeriv, notNullValue());
                assertThat(docCountDeriv.value(), closeTo((firstDerivValueCounts[i - 1]), 0.00001));
                assertThat(docCountDeriv.normalizedValue(), closeTo((double) (firstDerivValueCounts[i - 1]) / 5, 0.00001));
            } else {
                assertThat(docCountDeriv, nullValue());
            }
            Derivative docCount2ndDeriv = bucket.getAggregations().get("2nd_deriv");
            if (i > 1) {
                assertThat(docCount2ndDeriv, notNullValue());
                assertThat(docCount2ndDeriv.value(), closeTo((secondDerivValueCounts[i - 2]), 0.00001));
                assertThat(docCount2ndDeriv.normalizedValue(), closeTo((double) (secondDerivValueCounts[i - 2]) * 2, 0.00001));
            } else {
                assertThat(docCount2ndDeriv, nullValue());
            }
        }
    }

    public void testSingleValueAggDerivative() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval)
                                .subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME))
                                .subAggregation(derivative("deriv", "sum"))).execute().actionGet();

        assertSearchResponse(response);

        Histogram deriv = response.getAggregations().get("histo");
        assertThat(deriv, notNullValue());
        assertThat(deriv.getName(), equalTo("histo"));
        assertThat(deriv.getBuckets().size(), equalTo(numValueBuckets));
        Object[] propertiesKeys = (Object[]) deriv.getProperty("_key");
        Object[] propertiesDocCounts = (Object[]) deriv.getProperty("_count");
        Object[] propertiesSumCounts = (Object[]) deriv.getProperty("sum.value");

        List<Bucket> buckets = new ArrayList<Bucket>(deriv.getBuckets());
        Long expectedSumPreviousBucket = Long.MIN_VALUE; // start value, gets
                                                         // overwritten
        for (int i = 0; i < numValueBuckets; ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            checkBucketKeyAndDocCount("InternalBucket " + i, bucket, i * interval, valueCounts[i]);
            Sum sum = bucket.getAggregations().get("sum");
            assertThat(sum, notNullValue());
            long expectedSum = valueCounts[i] * (i * interval);
            assertThat(sum.getValue(), equalTo((double) expectedSum));
            SimpleValue sumDeriv = bucket.getAggregations().get("deriv");
            if (i > 0) {
                assertThat(sumDeriv, notNullValue());
                long sumDerivValue = expectedSum - expectedSumPreviousBucket;
                assertThat(sumDeriv.value(), equalTo((double) sumDerivValue));
                assertThat((double) bucket.getProperty("histo", AggregationPath.parse("deriv.value").getPathElementsAsStringList()),
                        equalTo((double) sumDerivValue));
            } else {
                assertThat(sumDeriv, nullValue());
            }
            expectedSumPreviousBucket = expectedSum;
            assertThat(propertiesKeys[i], equalTo((double) i * interval));
            assertThat((long) propertiesDocCounts[i], equalTo(valueCounts[i]));
            assertThat((double) propertiesSumCounts[i], equalTo((double) expectedSum));
        }
    }

    public void testMultiValueAggDerivative() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx")
                .addAggregation(
                        histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval)
                                .subAggregation(stats("stats").field(SINGLE_VALUED_FIELD_NAME))
                                .subAggregation(derivative("deriv", "stats.sum"))).execute().actionGet();

        assertSearchResponse(response);

        Histogram deriv = response.getAggregations().get("histo");
        assertThat(deriv, notNullValue());
        assertThat(deriv.getName(), equalTo("histo"));
        assertThat(deriv.getBuckets().size(), equalTo(numValueBuckets));
        Object[] propertiesKeys = (Object[]) deriv.getProperty("_key");
        Object[] propertiesDocCounts = (Object[]) deriv.getProperty("_count");
        Object[] propertiesSumCounts = (Object[]) deriv.getProperty("stats.sum");

        List<Bucket> buckets = new ArrayList<Bucket>(deriv.getBuckets());
        Long expectedSumPreviousBucket = Long.MIN_VALUE; // start value, gets
                                                         // overwritten
        for (int i = 0; i < numValueBuckets; ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            checkBucketKeyAndDocCount("InternalBucket " + i, bucket, i * interval, valueCounts[i]);
            Stats stats = bucket.getAggregations().get("stats");
            assertThat(stats, notNullValue());
            long expectedSum = valueCounts[i] * (i * interval);
            assertThat(stats.getSum(), equalTo((double) expectedSum));
            SimpleValue sumDeriv = bucket.getAggregations().get("deriv");
            if (i > 0) {
                assertThat(sumDeriv, notNullValue());
                long sumDerivValue = expectedSum - expectedSumPreviousBucket;
                assertThat(sumDeriv.value(), equalTo((double) sumDerivValue));
                assertThat((double) bucket.getProperty("histo", AggregationPath.parse("deriv.value").getPathElementsAsStringList()),
                        equalTo((double) sumDerivValue));
            } else {
                assertThat(sumDeriv, nullValue());
            }
            expectedSumPreviousBucket = expectedSum;
            assertThat(propertiesKeys[i], equalTo((double) i * interval));
            assertThat((long) propertiesDocCounts[i], equalTo(valueCounts[i]));
            assertThat((double) propertiesSumCounts[i], equalTo((double) expectedSum));
        }
    }

    public void testUnmapped() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx_unmapped")
                .addAggregation(
                        histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval)
                                .subAggregation(derivative("deriv", "_count"))).execute().actionGet();

        assertSearchResponse(response);

        Histogram deriv = response.getAggregations().get("histo");
        assertThat(deriv, notNullValue());
        assertThat(deriv.getName(), equalTo("histo"));
        assertThat(deriv.getBuckets().size(), equalTo(0));
    }

    public void testPartiallyUnmapped() throws Exception {
        SearchResponse response = client()
                .prepareSearch("idx", "idx_unmapped")
                .addAggregation(
                        histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(interval)
                                .subAggregation(derivative("deriv", "_count"))).execute().actionGet();

        assertSearchResponse(response);

        Histogram deriv = response.getAggregations().get("histo");
        assertThat(deriv, notNullValue());
        assertThat(deriv.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = deriv.getBuckets();
        assertThat(deriv.getBuckets().size(), equalTo(numValueBuckets));

        for (int i = 0; i < numValueBuckets; ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            checkBucketKeyAndDocCount("InternalBucket " + i, bucket, i * interval, valueCounts[i]);
            SimpleValue docCountDeriv = bucket.getAggregations().get("deriv");
            if (i > 0) {
                assertThat(docCountDeriv, notNullValue());
                assertThat(docCountDeriv.value(), equalTo((double) firstDerivValueCounts[i - 1]));
            } else {
                assertThat(docCountDeriv, nullValue());
            }
        }
    }

    public void testDocCountDerivativeWithGaps() throws Exception {
        SearchResponse searchResponse = client()
                .prepareSearch("empty_bucket_idx")
                .setQuery(matchAllQuery())
                .addAggregation(
                        histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(1)
                                .subAggregation(derivative("deriv", "_count"))).execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(numDocsEmptyIdx));

        Histogram deriv = searchResponse.getAggregations().get("histo");
        assertThat(deriv, Matchers.notNullValue());
        assertThat(deriv.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = deriv.getBuckets();
        assertThat(buckets.size(), equalTo(valueCounts_empty.length));

        for (int i = 0; i < valueCounts_empty.length; i++) {
            Histogram.Bucket bucket = buckets.get(i);
            checkBucketKeyAndDocCount("InternalBucket " + i, bucket, i, valueCounts_empty[i]);
            SimpleValue docCountDeriv = bucket.getAggregations().get("deriv");
            if (firstDerivValueCounts_empty[i] == null) {
                assertThat(docCountDeriv, nullValue());
            } else {
                assertThat(docCountDeriv.value(), equalTo(firstDerivValueCounts_empty[i]));
            }
        }
    }

    public void testDocCountDerivativeWithGaps_random() throws Exception {
        SearchResponse searchResponse = client()
                .prepareSearch("empty_bucket_idx_rnd")
                .setQuery(matchAllQuery())
                .addAggregation(
                        histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(1)
                                .extendedBounds(0L, numBuckets_empty_rnd - 1)
                                .subAggregation(derivative("deriv", "_count").gapPolicy(randomFrom(GapPolicy.values()))))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(numDocsEmptyIdx_rnd));

        Histogram deriv = searchResponse.getAggregations().get("histo");
        assertThat(deriv, Matchers.notNullValue());
        assertThat(deriv.getName(), equalTo("histo"));
        List<? extends Bucket> buckets = deriv.getBuckets();
        assertThat(buckets.size(), equalTo(numBuckets_empty_rnd));

        for (int i = 0; i < valueCounts_empty_rnd.length; i++) {
            Histogram.Bucket bucket = buckets.get(i);
            checkBucketKeyAndDocCount("InternalBucket " + i, bucket, i, valueCounts_empty_rnd[i]);
            SimpleValue docCountDeriv = bucket.getAggregations().get("deriv");
            if (firstDerivValueCounts_empty_rnd[i] == null) {
                assertThat(docCountDeriv, nullValue());
            } else {
                assertThat(docCountDeriv.value(), equalTo(firstDerivValueCounts_empty_rnd[i]));
            }
        }
    }

    public void testDocCountDerivativeWithGaps_insertZeros() throws Exception {
        SearchResponse searchResponse = client()
                .prepareSearch("empty_bucket_idx")
                .setQuery(matchAllQuery())
                .addAggregation(
                        histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(1)
                                .subAggregation(derivative("deriv", "_count").gapPolicy(GapPolicy.INSERT_ZEROS))).execute()
                                .actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(numDocsEmptyIdx));

        Histogram deriv = searchResponse.getAggregations().get("histo");
        assertThat(deriv, Matchers.notNullValue());
        assertThat(deriv.getName(), equalTo("histo"));
        List<Bucket> buckets = deriv.getBuckets();
        assertThat(buckets.size(), equalTo(valueCounts_empty.length));

        for (int i = 0; i < valueCounts_empty.length; i++) {
            Histogram.Bucket bucket = buckets.get(i);
            checkBucketKeyAndDocCount("InternalBucket " + i + ": ", bucket, i, valueCounts_empty[i]);
            SimpleValue docCountDeriv = bucket.getAggregations().get("deriv");
            if (firstDerivValueCounts_empty[i] == null) {
                assertThat(docCountDeriv, nullValue());
            } else {
                assertThat(docCountDeriv.value(), equalTo(firstDerivValueCounts_empty[i]));
            }
        }
    }

    public void testSingleValueAggDerivativeWithGaps() throws Exception {
        SearchResponse searchResponse = client()
                .prepareSearch("empty_bucket_idx")
                .setQuery(matchAllQuery())
                .addAggregation(
                        histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(1)
                                .subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME))
                                .subAggregation(derivative("deriv", "sum"))).execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(numDocsEmptyIdx));

        Histogram deriv = searchResponse.getAggregations().get("histo");
        assertThat(deriv, Matchers.notNullValue());
        assertThat(deriv.getName(), equalTo("histo"));
        List<Bucket> buckets = deriv.getBuckets();
        assertThat(buckets.size(), equalTo(valueCounts_empty.length));

        double lastSumValue = Double.NaN;
        for (int i = 0; i < valueCounts_empty.length; i++) {
            Histogram.Bucket bucket = buckets.get(i);
            checkBucketKeyAndDocCount("InternalBucket " + i, bucket, i, valueCounts_empty[i]);
            Sum sum = bucket.getAggregations().get("sum");
            double thisSumValue = sum.value();
            if (bucket.getDocCount() == 0) {
                thisSumValue = Double.NaN;
            }
            SimpleValue sumDeriv = bucket.getAggregations().get("deriv");
            if (i == 0) {
                assertThat(sumDeriv, nullValue());
            } else {
                double expectedDerivative = thisSumValue - lastSumValue;
                if (Double.isNaN(expectedDerivative)) {
                    assertThat(sumDeriv.value(), equalTo(expectedDerivative));
                } else {
                    assertThat(sumDeriv.value(), closeTo(expectedDerivative, 0.00001));
                }
            }
            lastSumValue = thisSumValue;
        }
    }

    public void testSingleValueAggDerivativeWithGaps_insertZeros() throws Exception {
        SearchResponse searchResponse = client()
                .prepareSearch("empty_bucket_idx")
                .setQuery(matchAllQuery())
                .addAggregation(
                        histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(1)
                                .subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME))
                                .subAggregation(derivative("deriv", "sum").gapPolicy(GapPolicy.INSERT_ZEROS))).execute()
                .actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(numDocsEmptyIdx));

        Histogram deriv = searchResponse.getAggregations().get("histo");
        assertThat(deriv, Matchers.notNullValue());
        assertThat(deriv.getName(), equalTo("histo"));
        List<Bucket> buckets = deriv.getBuckets();
        assertThat(buckets.size(), equalTo(valueCounts_empty.length));

        double lastSumValue = Double.NaN;
        for (int i = 0; i < valueCounts_empty.length; i++) {
            Histogram.Bucket bucket = buckets.get(i);
            checkBucketKeyAndDocCount("InternalBucket " + i, bucket, i, valueCounts_empty[i]);
            Sum sum = bucket.getAggregations().get("sum");
            double thisSumValue = sum.value();
            if (bucket.getDocCount() == 0) {
                thisSumValue = 0;
            }
            SimpleValue sumDeriv = bucket.getAggregations().get("deriv");
            if (i == 0) {
                assertThat(sumDeriv, nullValue());
            } else {
                double expectedDerivative = thisSumValue - lastSumValue;
                assertThat(sumDeriv.value(), closeTo(expectedDerivative, 0.00001));
            }
            lastSumValue = thisSumValue;
        }
    }

    public void testSingleValueAggDerivativeWithGaps_random() throws Exception {
        GapPolicy gapPolicy = randomFrom(GapPolicy.values());
        SearchResponse searchResponse = client()
                .prepareSearch("empty_bucket_idx_rnd")
                .setQuery(matchAllQuery())
                .addAggregation(
                        histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(1)
                                .extendedBounds(0L, (long) numBuckets_empty_rnd - 1)
                                .subAggregation(sum("sum").field(SINGLE_VALUED_FIELD_NAME))
                                .subAggregation(derivative("deriv", "sum").gapPolicy(gapPolicy))).execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(numDocsEmptyIdx_rnd));

        Histogram deriv = searchResponse.getAggregations().get("histo");
        assertThat(deriv, Matchers.notNullValue());
        assertThat(deriv.getName(), equalTo("histo"));
        List<Bucket> buckets = deriv.getBuckets();
        assertThat(buckets.size(), equalTo(numBuckets_empty_rnd));

        double lastSumValue = Double.NaN;
        for (int i = 0; i < valueCounts_empty_rnd.length; i++) {
            Histogram.Bucket bucket = buckets.get(i);
            checkBucketKeyAndDocCount("InternalBucket " + i, bucket, i, valueCounts_empty_rnd[i]);
            Sum sum = bucket.getAggregations().get("sum");
            double thisSumValue = sum.value();
            if (bucket.getDocCount() == 0) {
                thisSumValue = gapPolicy == GapPolicy.INSERT_ZEROS ? 0 : Double.NaN;
            }
            SimpleValue sumDeriv = bucket.getAggregations().get("deriv");
            if (i == 0) {
                assertThat(sumDeriv, nullValue());
            } else {
                double expectedDerivative = thisSumValue - lastSumValue;
                if (Double.isNaN(expectedDerivative)) {
                    assertThat(sumDeriv.value(), equalTo(expectedDerivative));
                } else {
                    assertThat(sumDeriv.value(), closeTo(expectedDerivative, 0.00001));
                }
            }
            lastSumValue = thisSumValue;
        }
    }

    public void testSingleValueAggDerivative_invalidPath() throws Exception {
        try {
            client().prepareSearch("idx")
                    .addAggregation(
                            histogram("histo")
                                    .field(SINGLE_VALUED_FIELD_NAME)
                                    .interval(interval)
                                    .subAggregation(
                                            filters("filters", QueryBuilders.termQuery("tag", "foo")).subAggregation(
                                                    sum("sum").field(SINGLE_VALUED_FIELD_NAME)))
                                    .subAggregation(derivative("deriv", "filters>get>sum"))).execute().actionGet();
            fail("Expected an Exception but didn't get one");
        } catch (Exception e) {
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            if (cause == null) {
                throw e;
            } else if (cause instanceof SearchPhaseExecutionException) {
                SearchPhaseExecutionException spee = (SearchPhaseExecutionException) e;
                Throwable rootCause = spee.getRootCause();
                if (!(rootCause instanceof IllegalArgumentException)) {
                    throw e;
                }
            } else if (!(cause instanceof IllegalArgumentException)) {
                throw e;
            }
        }
    }

    public void testAvgMovavgDerivNPE() throws Exception {
        createIndex("movavg_npe");

        for (int i = 0; i < 10; i++) {
            Integer value = i;
            if (i == 1 || i == 3) {
                value = null;
            }

            XContentBuilder doc = jsonBuilder()
                    .startObject()
                    .field("tick", i)
                    .field("value", value)
                    .endObject();
            client().prepareIndex("movavg_npe", "type").setSource(doc).get();
        }

        refresh();

        SearchResponse response = client()
                .prepareSearch("movavg_npe")
                .addAggregation(
                        histogram("histo").field("tick").interval(1)
                                .subAggregation(avg("avg").field("value"))
                                .subAggregation(movingAvg("movavg", "avg").modelBuilder(new SimpleModel.SimpleModelBuilder()).window(3))
                                .subAggregation(derivative("deriv", "movavg"))).execute().actionGet();

        assertSearchResponse(response);
    }

    private void checkBucketKeyAndDocCount(final String msg, final Histogram.Bucket bucket, final long expectedKey,
            final long expectedDocCount) {
        assertThat(msg, bucket, notNullValue());
        assertThat(msg + " key", ((Number) bucket.getKey()).longValue(), equalTo(expectedKey));
        assertThat(msg + " docCount", bucket.getDocCount(), equalTo(expectedDocCount));
    }
}
