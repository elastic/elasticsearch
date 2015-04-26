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
package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.functionScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.avg;
import static org.elasticsearch.search.aggregations.AggregationBuilders.extendedStats;
import static org.elasticsearch.search.aggregations.AggregationBuilders.filter;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.max;
import static org.elasticsearch.search.aggregations.AggregationBuilders.stats;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.notNullValue;

/**
 *
 */
@ElasticsearchIntegrationTest.SuiteScopeTest
public class DoubleTermsTests extends AbstractTermsTests {

    private static final int NUM_DOCS = 5; // TODO: randomize the size?
    private static final String SINGLE_VALUED_FIELD_NAME = "d_value";
    private static final String MULTI_VALUED_FIELD_NAME = "d_values";
    private static HashMap<Double, Map<String, Object>> expectedMultiSortBuckets;

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < NUM_DOCS; i++) {
            builders.add(client().prepareIndex("idx", "type").setSource(jsonBuilder()
                    .startObject()
                    .field(SINGLE_VALUED_FIELD_NAME, (double) i)
                    .field("num_tag", i < NUM_DOCS/2 + 1 ? 1 : 0) // used to test order by single-bucket sub agg
                    .startArray(MULTI_VALUED_FIELD_NAME).value((double) i).value(i + 1d).endArray()
                    .endObject()));

        }
        for (int i = 0; i < 100; i++) {
            builders.add(client().prepareIndex("idx", "high_card_type").setSource(jsonBuilder()
                    .startObject()
                    .field(SINGLE_VALUED_FIELD_NAME, (double) i)
                    .startArray(MULTI_VALUED_FIELD_NAME).value((double)i).value(i + 1d).endArray()
                    .endObject()));
        }

        createIndex("idx_unmapped");
        assertAcked(prepareCreate("empty_bucket_idx").addMapping("type", SINGLE_VALUED_FIELD_NAME, "type=integer"));
        for (int i = 0; i < 2; i++) {
            builders.add(client().prepareIndex("empty_bucket_idx", "type", ""+i).setSource(jsonBuilder()
                    .startObject()
                    .field(SINGLE_VALUED_FIELD_NAME, i*2)
                    .endObject()));
        }

        getMultiSortDocs(builders);

        indexRandom(true, builders);
        ensureSearchable();
    }

    private void getMultiSortDocs(List<IndexRequestBuilder> builders) throws IOException {
        expectedMultiSortBuckets = new HashMap<>();
        Map<String, Object> bucketProps = new HashMap<>();
        bucketProps.put("_term", 1d);
        bucketProps.put("_count", 3l);
        bucketProps.put("avg_l", 1d);
        bucketProps.put("sum_d", 6d);
        expectedMultiSortBuckets.put((Double) bucketProps.get("_term"), bucketProps);
        bucketProps = new HashMap<>();
        bucketProps.put("_term", 2d);
        bucketProps.put("_count", 3l);
        bucketProps.put("avg_l", 2d);
        bucketProps.put("sum_d", 6d);
        expectedMultiSortBuckets.put((Double) bucketProps.get("_term"), bucketProps);
        bucketProps = new HashMap<>();
        bucketProps.put("_term", 3d);
        bucketProps.put("_count", 2l);
        bucketProps.put("avg_l", 3d);
        bucketProps.put("sum_d", 3d);
        expectedMultiSortBuckets.put((Double) bucketProps.get("_term"), bucketProps);
        bucketProps = new HashMap<>();
        bucketProps.put("_term", 4d);
        bucketProps.put("_count", 2l);
        bucketProps.put("avg_l", 3d);
        bucketProps.put("sum_d", 4d);
        expectedMultiSortBuckets.put((Double) bucketProps.get("_term"), bucketProps);
        bucketProps = new HashMap<>();
        bucketProps.put("_term", 5d);
        bucketProps.put("_count", 2l);
        bucketProps.put("avg_l", 5d);
        bucketProps.put("sum_d", 3d);
        expectedMultiSortBuckets.put((Double) bucketProps.get("_term"), bucketProps);
        bucketProps = new HashMap<>();
        bucketProps.put("_term", 6d);
        bucketProps.put("_count", 1l);
        bucketProps.put("avg_l", 5d);
        bucketProps.put("sum_d", 1d);
        expectedMultiSortBuckets.put((Double) bucketProps.get("_term"), bucketProps);
        bucketProps = new HashMap<>();
        bucketProps.put("_term", 7d);
        bucketProps.put("_count", 1l);
        bucketProps.put("avg_l", 5d);
        bucketProps.put("sum_d", 1d);
        expectedMultiSortBuckets.put((Double) bucketProps.get("_term"), bucketProps);

        assertAcked(prepareCreate("sort_idx").addMapping("multi_sort_type", SINGLE_VALUED_FIELD_NAME, "type=double"));
        for (int i = 1; i <= 3; i++) {
            builders.add(client().prepareIndex("sort_idx", "multi_sort_type").setSource(jsonBuilder()
                    .startObject()
                    .field(SINGLE_VALUED_FIELD_NAME, 1)
                    .field("l", 1)
                    .field("d", i)
                    .endObject()));
            builders.add(client().prepareIndex("sort_idx", "multi_sort_type").setSource(jsonBuilder()
                    .startObject()
                    .field(SINGLE_VALUED_FIELD_NAME, 2)
                    .field("l", 2)
                    .field("d", i)
                    .endObject()));
        }
        builders.add(client().prepareIndex("sort_idx", "multi_sort_type").setSource(jsonBuilder()
                .startObject()
                .field(SINGLE_VALUED_FIELD_NAME, 3)
                .field("l", 3)
                .field("d", 1)
                .endObject()));
        builders.add(client().prepareIndex("sort_idx", "multi_sort_type").setSource(jsonBuilder()
                .startObject()
                .field(SINGLE_VALUED_FIELD_NAME, 3)
                .field("l", 3)
                .field("d", 2)
                .endObject()));
        builders.add(client().prepareIndex("sort_idx", "multi_sort_type").setSource(jsonBuilder()
                .startObject()
                .field(SINGLE_VALUED_FIELD_NAME, 4)
                .field("l", 3)
                .field("d", 1)
                .endObject()));
        builders.add(client().prepareIndex("sort_idx", "multi_sort_type").setSource(jsonBuilder()
                .startObject()
                .field(SINGLE_VALUED_FIELD_NAME, 4)
                .field("l", 3)
                .field("d", 3)
                .endObject()));
        builders.add(client().prepareIndex("sort_idx", "multi_sort_type").setSource(jsonBuilder()
                .startObject()
                .field(SINGLE_VALUED_FIELD_NAME, 5)
                .field("l", 5)
                .field("d", 1)
                .endObject()));
        builders.add(client().prepareIndex("sort_idx", "multi_sort_type").setSource(jsonBuilder()
                .startObject()
                .field(SINGLE_VALUED_FIELD_NAME, 5)
                .field("l", 5)
                .field("d", 2)
                .endObject()));
        builders.add(client().prepareIndex("sort_idx", "multi_sort_type").setSource(jsonBuilder()
                .startObject()
                .field(SINGLE_VALUED_FIELD_NAME, 6)
                .field("l", 5)
                .field("d", 1)
                .endObject()));
        builders.add(client().prepareIndex("sort_idx", "multi_sort_type").setSource(jsonBuilder()
                .startObject()
                .field(SINGLE_VALUED_FIELD_NAME, 7)
                .field("l", 5)
                .field("d", 1)
                .endObject()));
    }

    private String key(Terms.Bucket bucket) {
        return bucket.getKeyAsString();
    }

    @Test
    // the main purpose of this test is to make sure we're not allocating 2GB of memory per shard
    public void sizeIsZero() {
        SearchResponse response = client().prepareSearch("idx").setTypes("high_card_type")
                .addAggregation(terms("terms")
                        .field(SINGLE_VALUED_FIELD_NAME)
                        .minDocCount(randomInt(1))
                        .size(0)
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(100));
    }

    @Test
    public void singleValueField() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field(SINGLE_VALUED_FIELD_NAME)
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .execute().actionGet();

        assertSearchResponse(response);


        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("" + (double) i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("" + (double)i));
            assertThat(bucket.getKeyAsNumber().intValue(), equalTo(i));
            assertThat(bucket.getDocCount(), equalTo(1l));
        }
    }

    @Test
    public void singleValueField_WithMaxSize() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("high_card_type")
                .addAggregation(terms("terms")
                        .field(SINGLE_VALUED_FIELD_NAME)
                        .size(20)
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .order(Terms.Order.term(true))) // we need to sort by terms cause we're checking the first 20 values
                .execute().actionGet();

        assertSearchResponse(response);


        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(20));

        for (int i = 0; i < 20; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("" + (double) i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("" + (double) i));
            assertThat(bucket.getKeyAsNumber().intValue(), equalTo(i));
            assertThat(bucket.getDocCount(), equalTo(1l));
        }
    }
    
    @Test
    public void singleValueFieldWithFiltering() throws Exception {
        double includes[] = { 1, 2, 3, 98.2 };
        double excludes[] = { 2, 4, 99 };
        double empty[] = {};
        testIncludeExcludeResults(includes, empty, new double[] { 1, 2, 3 });
        testIncludeExcludeResults(includes, excludes, new double[] { 1, 3 });
        testIncludeExcludeResults(empty, excludes, new double[] { 0, 1, 3 });
    }

    private void testIncludeExcludeResults(double[] includes, double[] excludes, double[] expecteds) {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field(SINGLE_VALUED_FIELD_NAME)
                        .include(includes)
                        .exclude(excludes)
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .execute().actionGet();
        assertSearchResponse(response);
        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(expecteds.length));

        for (int i = 0; i < expecteds.length; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("" + expecteds[i]);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getDocCount(), equalTo(1l));
        }
    }
    

    @Test
    public void singleValueField_OrderedByTermAsc() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field(SINGLE_VALUED_FIELD_NAME)
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .order(Terms.Order.term(true)))
                .execute().actionGet();

        assertSearchResponse(response);


        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        int i = 0;
        for (Terms.Bucket bucket : terms.getBuckets()) {
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("" + (double)i));
            assertThat(bucket.getKeyAsNumber().intValue(), equalTo(i));
            assertThat(bucket.getDocCount(), equalTo(1l));
            i++;
        }
    }

    @Test
    public void singleValueField_OrderedByTermDesc() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field(SINGLE_VALUED_FIELD_NAME)
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .order(Terms.Order.term(false)))
                .execute().actionGet();

        assertSearchResponse(response);


        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        int i = 4;
        for (Terms.Bucket bucket : terms.getBuckets()) {
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("" + (double) i));
            assertThat(bucket.getKeyAsNumber().intValue(), equalTo(i));
            assertThat(bucket.getDocCount(), equalTo(1l));
            i--;
        }
    }

    @Test
    public void singleValuedField_WithSubAggregation() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field(SINGLE_VALUED_FIELD_NAME)
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .subAggregation(sum("sum").field(MULTI_VALUED_FIELD_NAME)))
                .execute().actionGet();

        assertSearchResponse(response);


        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));
        Object[] propertiesKeys = (Object[]) terms.getProperty("_key");
        Object[] propertiesDocCounts = (Object[]) terms.getProperty("_count");
        Object[] propertiesCounts = (Object[]) terms.getProperty("sum.value");

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("" + (double) i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("" + (double) i));
            assertThat(bucket.getKeyAsNumber().intValue(), equalTo(i));
            assertThat(bucket.getDocCount(), equalTo(1l));
            Sum sum = bucket.getAggregations().get("sum");
            assertThat(sum, notNullValue());
            assertThat((long) sum.getValue(), equalTo(i+i+1l));
            assertThat((double) propertiesKeys[i], equalTo((double) i));
            assertThat((long) propertiesDocCounts[i], equalTo(1l));
            assertThat((double) propertiesCounts[i], equalTo((double) i + i + 1l));
        }
    }

    @Test
    public void singleValuedField_WithSubAggregation_Inherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field(SINGLE_VALUED_FIELD_NAME)
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .subAggregation(sum("sum")))
                .execute().actionGet();

        assertSearchResponse(response);


        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("" + (double) i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("" + (double) i));
            assertThat(bucket.getKeyAsNumber().intValue(), equalTo(i));
            assertThat(bucket.getDocCount(), equalTo(1l));
            Sum sum = bucket.getAggregations().get("sum");
            assertThat(sum, notNullValue());
            assertThat(sum.getValue(), equalTo((double) i));
        }
    }

    @Test
    public void singleValuedField_WithValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field(SINGLE_VALUED_FIELD_NAME)
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .script("_value + 1"))
                .execute().actionGet();

        assertSearchResponse(response);


        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("" + (i + 1d));
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("" + (i+1d)));
            assertThat(bucket.getKeyAsNumber().intValue(), equalTo(i + 1));
            assertThat(bucket.getDocCount(), equalTo(1l));
        }
    }

    @Test
    public void multiValuedField() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field(MULTI_VALUED_FIELD_NAME)
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .execute().actionGet();

        assertSearchResponse(response);


        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(6));

        for (int i = 0; i < 6; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("" + (double) i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("" + (double) i));
            assertThat(bucket.getKeyAsNumber().intValue(), equalTo(i));
            if (i == 0 || i == 5) {
                assertThat(bucket.getDocCount(), equalTo(1l));
            } else {
                assertThat(bucket.getDocCount(), equalTo(2l));
            }
        }
    }

    @Test
    public void multiValuedField_WithValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field(MULTI_VALUED_FIELD_NAME)
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .script("_value + 1"))
                .execute().actionGet();

        assertSearchResponse(response);


        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(6));

        for (int i = 0; i < 6; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("" + (i + 1d));
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("" + (i+1d)));
            assertThat(bucket.getKeyAsNumber().intValue(), equalTo(i + 1));
            if (i == 0 || i == 5) {
                assertThat(bucket.getDocCount(), equalTo(1l));
            } else {
                assertThat(bucket.getDocCount(), equalTo(2l));
            }
        }
    }

    @Test
    public void multiValuedField_WithValueScript_NotUnique() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field(MULTI_VALUED_FIELD_NAME)
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .script("(long) _value / 1000 + 1"))
                .execute().actionGet();

        assertSearchResponse(response);


        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(1));

        Terms.Bucket bucket = terms.getBucketByKey("1.0");
        assertThat(bucket, notNullValue());
        assertThat(key(bucket), equalTo("1.0"));
        assertThat(bucket.getKeyAsNumber().intValue(), equalTo(1));
        assertThat(bucket.getDocCount(), equalTo(5l));
    }

    /*

    [1, 2]
    [2, 3]
    [3, 4]
    [4, 5]
    [5, 6]

    1 - count: 1 - sum: 1
    2 - count: 2 - sum: 4
    3 - count: 2 - sum: 6
    4 - count: 2 - sum: 8
    5 - count: 2 - sum: 10
    6 - count: 1 - sum: 6

    */

    @Test
    public void multiValuedField_WithValueScript_WithInheritedSubAggregator() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field(MULTI_VALUED_FIELD_NAME)
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .script("_value + 1")
                        .subAggregation(sum("sum")))
                .execute().actionGet();

        assertSearchResponse(response);


        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(6));

        for (int i = 0; i < 6; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("" + (i + 1d));
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("" + (i+1d)));
            assertThat(bucket.getKeyAsNumber().doubleValue(), equalTo(i + 1d));
            final long count = i == 0 || i == 5 ? 1 : 2;
            double s = 0;
            for (int j = 0; j < NUM_DOCS; ++j) {
                if (i == j || i == j+1) {
                    s += j + 1;
                    s += j+1 + 1;
                }
            }
            assertThat(bucket.getDocCount(), equalTo(count));
            Sum sum = bucket.getAggregations().get("sum");
            assertThat(sum, notNullValue());
            assertThat(sum.getValue(), equalTo(s));
        }
    }

    @Test
    public void script_SingleValue() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .script("doc['" + MULTI_VALUED_FIELD_NAME + "'].value"))
                .execute().actionGet();

        assertSearchResponse(response);


        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("" + (double) i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("" + (double) i));
            assertThat(bucket.getKeyAsNumber().intValue(), equalTo(i));
            assertThat(bucket.getDocCount(), equalTo(1l));
        }
    }

    @Test
    public void script_SingleValue_WithSubAggregator_Inherited() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field(SINGLE_VALUED_FIELD_NAME)
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .subAggregation(sum("sum")))
                .execute().actionGet();

        assertSearchResponse(response);


        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("" + (double) i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("" + (double) i));
            assertThat(bucket.getKeyAsNumber().intValue(), equalTo(i));
            assertThat(bucket.getDocCount(), equalTo(1l));
            Sum sum = bucket.getAggregations().get("sum");
            assertThat(sum, notNullValue());
            assertThat(sum.getValue(), equalTo((double) i));
        }
    }

    @Test
    public void script_MultiValued() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .script("doc['" + MULTI_VALUED_FIELD_NAME + "']"))
                .execute().actionGet();

        assertSearchResponse(response);


        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(6));

        for (int i = 0; i < 6; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("" + (double) i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("" + (double) i));
            assertThat(bucket.getKeyAsNumber().intValue(), equalTo(i));
            if (i == 0 || i == 5) {
                assertThat(bucket.getDocCount(), equalTo(1l));
            } else {
                assertThat(bucket.getDocCount(), equalTo(2l));
            }
        }
    }

    @Test
    public void script_MultiValued_WithAggregatorInherited_NoExplicitType() throws Exception {

        // since no type is explicitly defined, es will assume all values returned by the script to be strings (bytes),
        // so the aggregation should fail, since the "sum" aggregation can only operation on numeric values.

        try {

            SearchResponse response = client().prepareSearch("idx").setTypes("type")
                    .addAggregation(terms("terms")
                            .collectMode(randomFrom(SubAggCollectionMode.values()))
                            .script("doc['" + MULTI_VALUED_FIELD_NAME + "']")
                            .subAggregation(sum("sum")))
                    .execute().actionGet();


            fail("expected to fail as sub-aggregation sum requires a numeric value source context, but there is none");

        } catch (Exception e) {
            // expected
        }

    }

    @Test
    public void script_MultiValued_WithAggregatorInherited_WithExplicitType() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .script("doc['" + MULTI_VALUED_FIELD_NAME + "']")
                        .valueType(Terms.ValueType.DOUBLE)
                        .subAggregation(sum("sum")))
                .execute().actionGet();

        assertSearchResponse(response);


        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(6));

        for (int i = 0; i < 6; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("" + i + ".0");
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("" + i + ".0"));
            assertThat(bucket.getKeyAsNumber().intValue(), equalTo(i));
            final long count = i == 0 || i == 5 ? 1 : 2;
            double s = 0;
            for (int j = 0; j < NUM_DOCS; ++j) {
                if (i == j || i == j+1) {
                    s += j;
                    s += j+1;
                }
            }
            assertThat(bucket.getDocCount(), equalTo(count));
            Sum sum = bucket.getAggregations().get("sum");
            assertThat(sum, notNullValue());
            assertThat(sum.getValue(), equalTo(s));
        }
    }

    @Test
    public void unmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx_unmapped").setTypes("type")
                .addAggregation(terms("terms")
                        .field(SINGLE_VALUED_FIELD_NAME)
                        .size(randomInt(5))
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .execute().actionGet();

        assertSearchResponse(response);


        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(0));
    }

    @Test
    public void partiallyUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx_unmapped", "idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field(SINGLE_VALUED_FIELD_NAME)
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .execute().actionGet();

        assertSearchResponse(response);


        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("" + (double) i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("" + (double) i));
            assertThat(bucket.getKeyAsNumber().intValue(), equalTo(i));
            assertThat(bucket.getDocCount(), equalTo(1l));
        }
    }

    @Test
    public void emptyAggregation() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("empty_bucket_idx")
                .setQuery(matchAllQuery())
                .addAggregation(histogram("histo").field(SINGLE_VALUED_FIELD_NAME).interval(1l).minDocCount(0)
                        .subAggregation(terms("terms")))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        Histogram histo = searchResponse.getAggregations().get("histo");
        assertThat(histo, Matchers.notNullValue());
        Histogram.Bucket bucket = histo.getBuckets().get(1);
        assertThat(bucket, Matchers.notNullValue());

        Terms terms = bucket.getAggregations().get("terms");
        assertThat(terms, Matchers.notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().isEmpty(), is(true));
    }

    @Test
    public void singleValuedField_OrderedBySingleValueSubAggregationAsc() throws Exception {
        boolean asc = true;
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field(SINGLE_VALUED_FIELD_NAME)
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .order(Terms.Order.aggregation("avg_i", asc))
                        .subAggregation(avg("avg_i").field(SINGLE_VALUED_FIELD_NAME))
                ).execute().actionGet();


        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("" + (double) i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("" + (double)i));
            assertThat(bucket.getDocCount(), equalTo(1l));
            Avg avg = bucket.getAggregations().get("avg_i");
            assertThat(avg, notNullValue());
            assertThat(avg.getValue(), equalTo((double) i));
        }
    }

    @Test
    public void singleValuedField_OrderedBySingleValueSubAggregationAscWithSubTermsAgg() throws Exception {
        boolean asc = true;
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field(SINGLE_VALUED_FIELD_NAME)
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .order(Terms.Order.aggregation("avg_i", asc))
                        .subAggregation(avg("avg_i").field(SINGLE_VALUED_FIELD_NAME)).subAggregation(terms("subTerms").field(MULTI_VALUED_FIELD_NAME)
                                .collectMode(randomFrom(SubAggCollectionMode.values())))
                ).execute().actionGet();


        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("" + (double) i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("" + (double)i));
            assertThat(bucket.getDocCount(), equalTo(1l));

            Avg avg = bucket.getAggregations().get("avg_i");
            assertThat(avg, notNullValue());
            assertThat(avg.getValue(), equalTo((double) i));

            Terms subTermsAgg = bucket.getAggregations().get("subTerms");
            assertThat(subTermsAgg, notNullValue());
            assertThat(subTermsAgg.getBuckets().size(), equalTo(2));
            double j = i;
            for (Terms.Bucket subBucket : subTermsAgg.getBuckets()) {
                assertThat(subBucket, notNullValue());
                assertThat(key(subBucket), equalTo(String.valueOf(j)));
                assertThat(subBucket.getDocCount(), equalTo(1l));
                j++;
            }
        }
    }

    @Test
    public void singleValuedField_OrderedBySingleBucketSubAggregationAsc() throws Exception {
        boolean asc = randomBoolean();
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("num_tags")
                        .field("num_tag")
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .order(Terms.Order.aggregation("filter", asc))
                        .subAggregation(filter("filter").filter(FilterBuilders.matchAllFilter()))
                ).execute().actionGet();


        assertSearchResponse(response);

        Terms tags = response.getAggregations().get("num_tags");
        assertThat(tags, notNullValue());
        assertThat(tags.getName(), equalTo("num_tags"));
        assertThat(tags.getBuckets().size(), equalTo(2));

        Iterator<Terms.Bucket> iters = tags.getBuckets().iterator();

        Terms.Bucket tag = iters.next();
        assertThat(tag, notNullValue());
        assertThat(key(tag), equalTo(asc ? "0" : "1"));
        assertThat(tag.getDocCount(), equalTo(asc ? 2l : 3l));
        Filter filter = tag.getAggregations().get("filter");
        assertThat(filter, notNullValue());
        assertThat(filter.getDocCount(), equalTo(asc ? 2l : 3l));

        tag = iters.next();
        assertThat(tag, notNullValue());
        assertThat(key(tag), equalTo(asc ? "1" : "0"));
        assertThat(tag.getDocCount(), equalTo(asc ? 3l : 2l));
        filter = tag.getAggregations().get("filter");
        assertThat(filter, notNullValue());
        assertThat(filter.getDocCount(), equalTo(asc ? 3l : 2l));
    }

    @Test
    public void singleValuedField_OrderedBySubAggregationAsc_MultiHierarchyLevels() throws Exception {
        boolean asc = randomBoolean();
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("tags")
                        .field("num_tag")
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .order(Terms.Order.aggregation("filter1>filter2>max", asc))
                        .subAggregation(filter("filter1").filter(FilterBuilders.matchAllFilter())
                                .subAggregation(filter("filter2").filter(FilterBuilders.matchAllFilter())
                                        .subAggregation(max("max").field(SINGLE_VALUED_FIELD_NAME))))
                ).execute().actionGet();


        assertSearchResponse(response);

        Terms tags = response.getAggregations().get("tags");
        assertThat(tags, notNullValue());
        assertThat(tags.getName(), equalTo("tags"));
        assertThat(tags.getBuckets().size(), equalTo(2));

        Iterator<Terms.Bucket> iters = tags.getBuckets().iterator();

        // the max for "1" is 2
        // the max for "0" is 4

        Terms.Bucket tag = iters.next();
        assertThat(tag, notNullValue());
        assertThat(key(tag), equalTo(asc ? "1" : "0"));
        assertThat(tag.getDocCount(), equalTo(asc ? 3l : 2l));
        Filter filter1 = tag.getAggregations().get("filter1");
        assertThat(filter1, notNullValue());
        assertThat(filter1.getDocCount(), equalTo(asc ? 3l : 2l));
        Filter filter2 = filter1.getAggregations().get("filter2");
        assertThat(filter2, notNullValue());
        assertThat(filter2.getDocCount(), equalTo(asc ? 3l : 2l));
        Max max = filter2.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo(asc ? 2.0 : 4.0));

        tag = iters.next();
        assertThat(tag, notNullValue());
        assertThat(key(tag), equalTo(asc ? "0" : "1"));
        assertThat(tag.getDocCount(), equalTo(asc ? 2l : 3l));
        filter1 = tag.getAggregations().get("filter1");
        assertThat(filter1, notNullValue());
        assertThat(filter1.getDocCount(), equalTo(asc ? 2l : 3l));
        filter2 = filter1.getAggregations().get("filter2");
        assertThat(filter2, notNullValue());
        assertThat(filter2.getDocCount(), equalTo(asc ? 2l : 3l));
        max = filter2.getAggregations().get("max");
        assertThat(max, notNullValue());
        assertThat(max.getValue(), equalTo(asc ? 4.0 : 2.0));
    }

    @Test
    public void singleValuedField_OrderedByMissingSubAggregation() throws Exception {
        for (String index : Arrays.asList("idx", "idx_unmapped")) {
            try {
                client().prepareSearch(index).setTypes("type")
                        .addAggregation(terms("terms")
                                .field(SINGLE_VALUED_FIELD_NAME)
                                .collectMode(randomFrom(SubAggCollectionMode.values()))
                                .order(Terms.Order.aggregation("avg_i", true))
                        ).execute().actionGet();

                fail("Expected search to fail when trying to sort terms aggregation by sug-aggregation that doesn't exist");

            } catch (ElasticsearchException e) {
                // expected
            }
        }
    }

    @Test
    public void singleValuedField_OrderedByNonMetricsOrMultiBucketSubAggregation() throws Exception {
        for (String index : Arrays.asList("idx", "idx_unmapped")) {
            try {
                client().prepareSearch(index).setTypes("type")
                        .addAggregation(terms("terms")
                                .field(SINGLE_VALUED_FIELD_NAME)
                                .collectMode(randomFrom(SubAggCollectionMode.values()))
                                .order(Terms.Order.aggregation("num_tags", true))
                                .subAggregation(terms("num_tags").field("num_tags")
                                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                        ).execute().actionGet();

                fail("Expected search to fail when trying to sort terms aggregation by sug-aggregation which is not of a metrics type");

            } catch (ElasticsearchException e) {
                // expected
            }
        }
    }

    @Test
    public void singleValuedField_OrderedByMultiValuedSubAggregation_WithUknownMetric() throws Exception {
        for (String index : Arrays.asList("idx", "idx_unmapped")) {
            try {
                client().prepareSearch(index).setTypes("type")
                        .addAggregation(terms("terms")
                                .field(SINGLE_VALUED_FIELD_NAME + "2")
                                .collectMode(randomFrom(SubAggCollectionMode.values()))
                                .order(Terms.Order.aggregation("stats.foo", true))
                                .subAggregation(stats("stats").field(SINGLE_VALUED_FIELD_NAME))
                        ).execute().actionGet();

                fail("Expected search to fail when trying to sort terms aggregation by multi-valued sug-aggregation " +
                        "with an unknown specified metric to order by");

            } catch (ElasticsearchException e) {
                // expected
            }
        }
    }

    @Test
    public void singleValuedField_OrderedByMultiValuedSubAggregation_WithoutMetric() throws Exception {
        for (String index : Arrays.asList("idx", "idx_unmapped")) {
            try {
                client().prepareSearch(index).setTypes("type")
                        .addAggregation(terms("terms")
                                .field(SINGLE_VALUED_FIELD_NAME)
                                .collectMode(randomFrom(SubAggCollectionMode.values()))
                                .order(Terms.Order.aggregation("stats", true))
                                .subAggregation(stats("stats").field(SINGLE_VALUED_FIELD_NAME))
                        ).execute().actionGet();

                fail("Expected search to fail when trying to sort terms aggregation by multi-valued sug-aggregation " +
                        "where the metric name is not specified");

            } catch (ElasticsearchException e) {
                // expected
            }
        }
    }

    @Test
    public void singleValuedField_OrderedBySingleValueSubAggregationDesc() throws Exception {
        boolean asc = false;
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field(SINGLE_VALUED_FIELD_NAME)
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .order(Terms.Order.aggregation("avg_i", asc))
                        .subAggregation(avg("avg_i").field(SINGLE_VALUED_FIELD_NAME))
                ).execute().actionGet();


        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        for (int i = 4; i >= 0; i--) {

            Terms.Bucket bucket = terms.getBucketByKey("" + (double) i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("" + (double)i));
            assertThat(bucket.getDocCount(), equalTo(1l));

            Avg avg = bucket.getAggregations().get("avg_i");
            assertThat(avg, notNullValue());
            assertThat(avg.getValue(), equalTo((double) i));
        }

    }

    @Test
    public void singleValuedField_OrderedByMultiValueSubAggregationAsc() throws Exception {
        boolean asc = true;
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field(SINGLE_VALUED_FIELD_NAME)
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .order(Terms.Order.aggregation("stats.avg", asc))
                        .subAggregation(stats("stats").field(SINGLE_VALUED_FIELD_NAME))
                ).execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("" + (double) i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("" + (double)i));
            assertThat(bucket.getDocCount(), equalTo(1l));

            Stats stats = bucket.getAggregations().get("stats");
            assertThat(stats, notNullValue());
            assertThat(stats.getMax(), equalTo((double) i));
        }

    }

    @Test
    public void singleValuedField_OrderedByMultiValueSubAggregationDesc() throws Exception {
        boolean asc = false;
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field(SINGLE_VALUED_FIELD_NAME)
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .order(Terms.Order.aggregation("stats.avg", asc))
                        .subAggregation(stats("stats").field(SINGLE_VALUED_FIELD_NAME))
                ).execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        for (int i = 4; i >= 0; i--) {
            Terms.Bucket bucket = terms.getBucketByKey("" + (double) i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("" + (double)i));
            assertThat(bucket.getDocCount(), equalTo(1l));

            Stats stats = bucket.getAggregations().get("stats");
            assertThat(stats, notNullValue());
            assertThat(stats.getMax(), equalTo((double) i));
        }

    }

    @Test
    public void singleValuedField_OrderedByMultiValueExtendedStatsAsc() throws Exception {
        boolean asc = true;
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .field(SINGLE_VALUED_FIELD_NAME)
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .order(Terms.Order.aggregation("stats.variance", asc))
                        .subAggregation(extendedStats("stats").field(SINGLE_VALUED_FIELD_NAME))
                ).execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("" + (double) i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("" + (double)i));
            assertThat(bucket.getDocCount(), equalTo(1l));

            ExtendedStats stats = bucket.getAggregations().get("stats");
            assertThat(stats, notNullValue());
            assertThat(stats.getMax(), equalTo((double) i));
        }

    }

    @Test
    public void script_Score() {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .setQuery(functionScoreQuery(matchAllQuery()).add(ScoreFunctionBuilders.scriptFunction("doc['" + SINGLE_VALUED_FIELD_NAME + "'].value")))
                .addAggregation(terms("terms")
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .script("ceil(_score.doubleValue()/3)")
                ).execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(3));

        for (int i = 0; i < 3; i++) {
            Terms.Bucket bucket = terms.getBucketByKey("" + (double) i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo("" + (double)i));
            assertThat(bucket.getKeyAsNumber().intValue(), equalTo(i));
            assertThat(bucket.getDocCount(), equalTo(i == 1 ? 3L : 1L));
        }
    }

    @Test
    public void singleValuedField_OrderedBySingleValueSubAggregationAscAndTermsDesc() throws Exception {
        double[] expectedKeys = new double[] { 1, 2, 4, 3, 7, 6, 5 };
        assertMultiSortResponse(expectedKeys, Terms.Order.aggregation("avg_l", true), Terms.Order.term(false));
    }

    @Test
    public void singleValuedField_OrderedBySingleValueSubAggregationAscAndTermsAsc() throws Exception {
        double[] expectedKeys = new double[] { 1, 2, 3, 4, 5, 6, 7 };
        assertMultiSortResponse(expectedKeys, Terms.Order.aggregation("avg_l", true), Terms.Order.term(true));
    }

    @Test
    public void singleValuedField_OrderedBySingleValueSubAggregationDescAndTermsAsc() throws Exception {
        double[] expectedKeys = new double[] { 5, 6, 7, 3, 4, 2, 1 };
        assertMultiSortResponse(expectedKeys, Terms.Order.aggregation("avg_l", false), Terms.Order.term(true));
    }

    @Test
    public void singleValuedField_OrderedByCountAscAndSingleValueSubAggregationAsc() throws Exception {
        double[] expectedKeys = new double[] { 6, 7, 3, 4, 5, 1, 2 };
        assertMultiSortResponse(expectedKeys, Terms.Order.count(true), Terms.Order.aggregation("avg_l", true));
    }

    @Test
    public void singleValuedField_OrderedBySingleValueSubAggregationAscSingleValueSubAggregationAsc() throws Exception {
        double[] expectedKeys = new double[] { 6, 7, 3, 5, 4, 1, 2 };
        assertMultiSortResponse(expectedKeys, Terms.Order.aggregation("sum_d", true), Terms.Order.aggregation("avg_l", true));
    }

    @Test
    public void singleValuedField_OrderedByThreeCriteria() throws Exception {
        double[] expectedKeys = new double[] { 2, 1, 4, 5, 3, 6, 7 };
        assertMultiSortResponse(expectedKeys, Terms.Order.count(false), Terms.Order.aggregation("sum_d", false), Terms.Order.aggregation("avg_l", false));
    }

    @Test
    public void singleValuedField_OrderedBySingleValueSubAggregationAscAsCompound() throws Exception {
        double[] expectedKeys = new double[] { 1, 2, 3, 4, 5, 6, 7 };
        assertMultiSortResponse(expectedKeys, Terms.Order.aggregation("avg_l", true));
    }

    private void assertMultiSortResponse(double[] expectedKeys, Terms.Order... order) {
        SearchResponse response = client().prepareSearch("sort_idx").setTypes("multi_sort_type")
                .addAggregation(terms("terms")
                        .field(SINGLE_VALUED_FIELD_NAME)
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .order(Terms.Order.compound(order))
                        .subAggregation(avg("avg_l").field("l"))
                        .subAggregation(sum("sum_d").field("d"))
                ).execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("terms"));
        assertThat(terms.getBuckets().size(), equalTo(expectedKeys.length));

        int i = 0;
        for (Terms.Bucket bucket : terms.getBuckets()) {
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo(String.valueOf(expectedKeys[i])));
            assertThat(bucket.getDocCount(), equalTo(expectedMultiSortBuckets.get(expectedKeys[i]).get("_count")));
            Avg avg = bucket.getAggregations().get("avg_l");
            assertThat(avg, notNullValue());
            assertThat(avg.getValue(), equalTo(expectedMultiSortBuckets.get(expectedKeys[i]).get("avg_l")));
            Sum sum = bucket.getAggregations().get("sum_d");
            assertThat(sum, notNullValue());
            assertThat(sum.getValue(), equalTo(expectedMultiSortBuckets.get(expectedKeys[i]).get("sum_d")));
            i++;
        }
    }

    @Test
    public void otherDocCount() {
        testOtherDocCount(SINGLE_VALUED_FIELD_NAME, MULTI_VALUED_FIELD_NAME);
    }
}
