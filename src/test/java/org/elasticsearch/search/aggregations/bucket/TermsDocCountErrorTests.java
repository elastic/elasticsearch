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

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Order;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregatorFactory.ExecutionMode;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.IsNull.notNullValue;

@ElasticsearchIntegrationTest.SuiteScopeTest
public class TermsDocCountErrorTests extends ElasticsearchIntegrationTest{

    private static final String STRING_FIELD_NAME = "s_value";
    private static final String LONG_FIELD_NAME = "l_value";
    private static final String DOUBLE_FIELD_NAME = "d_value";
    private static final String ROUTING_FIELD_NAME = "route";

    public static String randomExecutionHint() {
        return randomBoolean() ? null : randomFrom(ExecutionMode.values()).toString();
    }

    private static int numRoutingValues;

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        List<IndexRequestBuilder> builders = new ArrayList<>();
        int numDocs = between(10, 200);
        int numUniqueTerms = between(2,numDocs/2);
        for (int i = 0; i < numDocs; i++) {
            builders.add(client().prepareIndex("idx", "type", ""+i).setSource(jsonBuilder()
                    .startObject()
                    .field(STRING_FIELD_NAME, "val" + randomInt(numUniqueTerms))
                    .field(LONG_FIELD_NAME, randomInt(numUniqueTerms))
                    .field(DOUBLE_FIELD_NAME, 1.0 * randomInt(numUniqueTerms))
                    .endObject()));
        }
        assertAcked(prepareCreate("idx_single_shard").setSettings(ImmutableSettings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)));
        for (int i = 0; i < numDocs; i++) {
            builders.add(client().prepareIndex("idx_single_shard", "type", ""+i).setSource(jsonBuilder()
                    .startObject()
                    .field(STRING_FIELD_NAME, "val" + randomInt(numUniqueTerms))
                    .field(LONG_FIELD_NAME, randomInt(numUniqueTerms))
                    .field(DOUBLE_FIELD_NAME, 1.0 * randomInt(numUniqueTerms))
                    .endObject()));
        }
        numRoutingValues = between(1,40);
        assertAcked(prepareCreate("idx_with_routing").addMapping("type", "{ \"type\" : { \"_routing\" : { \"required\" : true, \"path\" : \"" + ROUTING_FIELD_NAME + "\" } } }"));
        for (int i = 0; i < numDocs; i++) {
            builders.add(client().prepareIndex("idx_single_shard", "type", ""+i).setSource(jsonBuilder()
                    .startObject()
                    .field(STRING_FIELD_NAME, "val" + randomInt(numUniqueTerms))
                    .field(LONG_FIELD_NAME, randomInt(numUniqueTerms))
                    .field(DOUBLE_FIELD_NAME, 1.0 * randomInt(numUniqueTerms))
                    .field(ROUTING_FIELD_NAME, String.valueOf(randomInt(numRoutingValues)))
                    .endObject()));
        }
        indexRandom(true, builders);
        ensureSearchable();
    }

    private void assertDocCountErrorWithinBounds(int size, SearchResponse accurateResponse, SearchResponse testResponse) {
        Terms accurateTerms = accurateResponse.getAggregations().get("terms");
        assertThat(accurateTerms, notNullValue());
        assertThat(accurateTerms.getName(), equalTo("terms"));
        assertThat(accurateTerms.getDocCountError(), equalTo(0l));
    
        Terms testTerms = testResponse.getAggregations().get("terms");
        assertThat(testTerms, notNullValue());
        assertThat(testTerms.getName(), equalTo("terms"));
        assertThat(testTerms.getDocCountError(), greaterThanOrEqualTo(0l));
        Collection<Bucket> testBuckets = testTerms.getBuckets();
        assertThat(testBuckets.size(), lessThanOrEqualTo(size));
        assertThat(accurateTerms.getBuckets().size(), greaterThanOrEqualTo(testBuckets.size()));
    
        for (Terms.Bucket testBucket : testBuckets) {
            assertThat(testBucket, notNullValue());
            Terms.Bucket accurateBucket = accurateTerms.getBucketByKey(testBucket.getKey());
            assertThat(accurateBucket, notNullValue());
            assertThat(accurateBucket.getDocCountError(), equalTo(0l));
            assertThat(testBucket.getDocCountError(), lessThanOrEqualTo(testTerms.getDocCountError()));
            assertThat(testBucket.getDocCount() + testBucket.getDocCountError(), greaterThanOrEqualTo(accurateBucket.getDocCount()));
            assertThat(testBucket.getDocCount() - testBucket.getDocCountError(), lessThanOrEqualTo(accurateBucket.getDocCount()));
        }
        
        for (Terms.Bucket accurateBucket: accurateTerms.getBuckets()) {
            assertThat(accurateBucket, notNullValue());
            Terms.Bucket testBucket = accurateTerms.getBucketByKey(accurateBucket.getKey());
            if (testBucket == null) {
                assertThat(accurateBucket.getDocCount(), lessThanOrEqualTo(testTerms.getDocCountError()));
            }
            
        }
    }

    private void assertNoDocCountError(int size, SearchResponse accurateResponse, SearchResponse testResponse) {
        Terms accurateTerms = accurateResponse.getAggregations().get("terms");
        assertThat(accurateTerms, notNullValue());
        assertThat(accurateTerms.getName(), equalTo("terms"));
        assertThat(accurateTerms.getDocCountError(), equalTo(0l));
    
        Terms testTerms = testResponse.getAggregations().get("terms");
        assertThat(testTerms, notNullValue());
        assertThat(testTerms.getName(), equalTo("terms"));
        assertThat(testTerms.getDocCountError(), equalTo(0l));
        Collection<Bucket> testBuckets = testTerms.getBuckets();
        assertThat(testBuckets.size(), lessThanOrEqualTo(size));
        assertThat(accurateTerms.getBuckets().size(), greaterThanOrEqualTo(testBuckets.size()));
    
        for (Terms.Bucket testBucket : testBuckets) {
            assertThat(testBucket, notNullValue());
            Terms.Bucket accurateBucket = accurateTerms.getBucketByKey(testBucket.getKey());
            assertThat(accurateBucket, notNullValue());
            assertThat(accurateBucket.getDocCountError(), equalTo(0l));
            assertThat(testBucket.getDocCountError(), equalTo(0l));
        }
    }

    private void assertNoDocCountErrorSingleResponse(int size, SearchResponse testResponse) {
        Terms testTerms = testResponse.getAggregations().get("terms");
        assertThat(testTerms, notNullValue());
        assertThat(testTerms.getName(), equalTo("terms"));
        assertThat(testTerms.getDocCountError(), equalTo(0l));
        Collection<Bucket> testBuckets = testTerms.getBuckets();
        assertThat(testBuckets.size(), lessThanOrEqualTo(size));
    
        for (Terms.Bucket testBucket : testBuckets) {
            assertThat(testBucket, notNullValue());
            assertThat(testBucket.getDocCountError(), equalTo(0l));
        }
    }

    private void assertUnboundedDocCountError(int size, SearchResponse accurateResponse, SearchResponse testResponse) {
        Terms accurateTerms = accurateResponse.getAggregations().get("terms");
        assertThat(accurateTerms, notNullValue());
        assertThat(accurateTerms.getName(), equalTo("terms"));
        assertThat(accurateTerms.getDocCountError(), equalTo(0l));
    
        Terms testTerms = testResponse.getAggregations().get("terms");
        assertThat(testTerms, notNullValue());
        assertThat(testTerms.getName(), equalTo("terms"));
        assertThat(testTerms.getDocCountError(),anyOf(equalTo(-1l), equalTo(0l)));
        Collection<Bucket> testBuckets = testTerms.getBuckets();
        assertThat(testBuckets.size(), lessThanOrEqualTo(size));
        assertThat(accurateTerms.getBuckets().size(), greaterThanOrEqualTo(testBuckets.size()));
    
        for (Terms.Bucket testBucket : testBuckets) {
            assertThat(testBucket, notNullValue());
            Terms.Bucket accurateBucket = accurateTerms.getBucketByKey(testBucket.getKey());
            assertThat(accurateBucket, notNullValue());
            assertThat(accurateBucket.getDocCountError(), equalTo(0l));
            assertThat(testBucket.getDocCountError(), anyOf(equalTo(-1l), equalTo(0l)));
        }
    }

    @Test
    public void stringValueField() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);
        SearchResponse accurateResponse = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(STRING_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(0)
                        .shardSize(0)
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .execute().actionGet();

        assertSearchResponse(accurateResponse);
        
        SearchResponse testResponse = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(STRING_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .execute().actionGet();

        assertSearchResponse(testResponse);

        assertDocCountErrorWithinBounds(size, accurateResponse, testResponse);
    }

    @Test
    public void stringValueField_singleShard() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);
        SearchResponse accurateResponse = client().prepareSearch("idx_single_shard").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(STRING_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(0)
                        .shardSize(0)
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .execute().actionGet();

        assertSearchResponse(accurateResponse);
        
        SearchResponse testResponse = client().prepareSearch("idx_single_shard").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(STRING_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .execute().actionGet();

        assertSearchResponse(testResponse);

        assertNoDocCountError(size, accurateResponse, testResponse);
    }

    @Test
    public void stringValueField_withRouting() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);
        
        SearchResponse testResponse = client().prepareSearch("idx_with_routing").setTypes("type").setRouting(String.valueOf(between(1, numRoutingValues)))
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(STRING_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .execute().actionGet();

        assertSearchResponse(testResponse);

        assertNoDocCountErrorSingleResponse(size, testResponse);
    }

    @Test
    public void stringValueField_docCountAsc() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);
        SearchResponse accurateResponse = client().prepareSearch("idx_single_shard").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(STRING_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(0)
                        .shardSize(0)
                        .order(Order.count(true))
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .execute().actionGet();

        assertSearchResponse(accurateResponse);
        
        SearchResponse testResponse = client().prepareSearch("idx_single_shard").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(STRING_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .order(Order.count(true))
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .execute().actionGet();

        assertSearchResponse(testResponse);

        assertUnboundedDocCountError(size, accurateResponse, testResponse);
    }

    @Test
    public void stringValueField_termSortAsc() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);
        SearchResponse accurateResponse = client().prepareSearch("idx_single_shard").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(STRING_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(0)
                        .shardSize(0)
                        .order(Order.term(true))
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .execute().actionGet();

        assertSearchResponse(accurateResponse);
        
        SearchResponse testResponse = client().prepareSearch("idx_single_shard").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(STRING_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .order(Order.term(true))
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .execute().actionGet();

        assertSearchResponse(testResponse);

        assertNoDocCountError(size, accurateResponse, testResponse);
    }

    @Test
    public void stringValueField_termSortDesc() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);
        SearchResponse accurateResponse = client().prepareSearch("idx_single_shard").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(STRING_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(0)
                        .shardSize(0)
                        .order(Order.term(false))
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .execute().actionGet();

        assertSearchResponse(accurateResponse);
        
        SearchResponse testResponse = client().prepareSearch("idx_single_shard").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(STRING_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .order(Order.term(false))
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .execute().actionGet();

        assertSearchResponse(testResponse);

        assertNoDocCountError(size, accurateResponse, testResponse);
    }

    @Test
    public void stringValueField_subAggAsc() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);
        SearchResponse accurateResponse = client().prepareSearch("idx_single_shard").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(STRING_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(0)
                        .shardSize(0)
                        .order(Order.aggregation("sortAgg", true))
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .subAggregation(sum("sortAgg").field(LONG_FIELD_NAME)))
                .execute().actionGet();

        assertSearchResponse(accurateResponse);
        
        SearchResponse testResponse = client().prepareSearch("idx_single_shard").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(STRING_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .order(Order.aggregation("sortAgg", true))
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .subAggregation(sum("sortAgg").field(LONG_FIELD_NAME)))
                .execute().actionGet();

        assertSearchResponse(testResponse);

        assertUnboundedDocCountError(size, accurateResponse, testResponse);
    }

    @Test
    public void stringValueField_subAggDesc() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);
        SearchResponse accurateResponse = client().prepareSearch("idx_single_shard").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(STRING_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(0)
                        .shardSize(0)
                        .order(Order.aggregation("sortAgg", false))
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .subAggregation(sum("sortAgg").field(LONG_FIELD_NAME)))
                .execute().actionGet();

        assertSearchResponse(accurateResponse);
        
        SearchResponse testResponse = client().prepareSearch("idx_single_shard").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(STRING_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .order(Order.aggregation("sortAgg", false))
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .subAggregation(sum("sortAgg").field(LONG_FIELD_NAME)))
                .execute().actionGet();

        assertSearchResponse(testResponse);

        assertUnboundedDocCountError(size, accurateResponse, testResponse);
    }

    @Test
    public void longValueField() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);
        SearchResponse accurateResponse = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(LONG_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(0)
                        .shardSize(0)
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .execute().actionGet();

        assertSearchResponse(accurateResponse);
        
        SearchResponse testResponse = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(LONG_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .execute().actionGet();

        assertSearchResponse(testResponse);

        assertDocCountErrorWithinBounds(size, accurateResponse, testResponse);
    }

    @Test
    public void longValueField_singleShard() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);
        SearchResponse accurateResponse = client().prepareSearch("idx_single_shard").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(LONG_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(0)
                        .shardSize(0)
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .execute().actionGet();

        assertSearchResponse(accurateResponse);
        
        SearchResponse testResponse = client().prepareSearch("idx_single_shard").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(LONG_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .execute().actionGet();

        assertSearchResponse(testResponse);

        assertNoDocCountError(size, accurateResponse, testResponse);
    }

    @Test
    public void longValueField_withRouting() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);
        
        SearchResponse testResponse = client().prepareSearch("idx_with_routing").setTypes("type").setRouting(String.valueOf(between(1, numRoutingValues)))
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(LONG_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .execute().actionGet();

        assertSearchResponse(testResponse);

        assertNoDocCountErrorSingleResponse(size, testResponse);
    }

    @Test
    public void longValueField_docCountAsc() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);
        SearchResponse accurateResponse = client().prepareSearch("idx_single_shard").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(LONG_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(0)
                        .shardSize(0)
                        .order(Order.count(true))
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .execute().actionGet();

        assertSearchResponse(accurateResponse);
        
        SearchResponse testResponse = client().prepareSearch("idx_single_shard").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(LONG_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .order(Order.count(true))
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .execute().actionGet();

        assertSearchResponse(testResponse);

        assertUnboundedDocCountError(size, accurateResponse, testResponse);
    }

    @Test
    public void longValueField_termSortAsc() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);
        SearchResponse accurateResponse = client().prepareSearch("idx_single_shard").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(LONG_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(0)
                        .shardSize(0)
                        .order(Order.term(true))
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .execute().actionGet();

        assertSearchResponse(accurateResponse);
        
        SearchResponse testResponse = client().prepareSearch("idx_single_shard").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(LONG_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .order(Order.term(true))
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .execute().actionGet();

        assertSearchResponse(testResponse);

        assertNoDocCountError(size, accurateResponse, testResponse);
    }

    @Test
    public void longValueField_termSortDesc() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);
        SearchResponse accurateResponse = client().prepareSearch("idx_single_shard").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(LONG_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(0)
                        .shardSize(0)
                        .order(Order.term(false))
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .execute().actionGet();

        assertSearchResponse(accurateResponse);
        
        SearchResponse testResponse = client().prepareSearch("idx_single_shard").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(LONG_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .order(Order.term(false))
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .execute().actionGet();

        assertSearchResponse(testResponse);

        assertNoDocCountError(size, accurateResponse, testResponse);
    }

    @Test
    public void longValueField_subAggAsc() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);
        SearchResponse accurateResponse = client().prepareSearch("idx_single_shard").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(LONG_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(0)
                        .shardSize(0)
                        .order(Order.aggregation("sortAgg", true))
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .subAggregation(sum("sortAgg").field(LONG_FIELD_NAME)))
                .execute().actionGet();

        assertSearchResponse(accurateResponse);
        
        SearchResponse testResponse = client().prepareSearch("idx_single_shard").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(LONG_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .order(Order.aggregation("sortAgg", true))
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .subAggregation(sum("sortAgg").field(LONG_FIELD_NAME)))
                .execute().actionGet();

        assertSearchResponse(testResponse);

        assertUnboundedDocCountError(size, accurateResponse, testResponse);
    }

    @Test
    public void longValueField_subAggDesc() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);
        SearchResponse accurateResponse = client().prepareSearch("idx_single_shard").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(LONG_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(0)
                        .shardSize(0)
                        .order(Order.aggregation("sortAgg", false))
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .subAggregation(sum("sortAgg").field(DOUBLE_FIELD_NAME)))
                .execute().actionGet();

        assertSearchResponse(accurateResponse);
        
        SearchResponse testResponse = client().prepareSearch("idx_single_shard").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(LONG_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .order(Order.aggregation("sortAgg", false))
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .subAggregation(sum("sortAgg").field(DOUBLE_FIELD_NAME)))
                .execute().actionGet();

        assertSearchResponse(testResponse);

        assertUnboundedDocCountError(size, accurateResponse, testResponse);
    }

    @Test
    public void doubleValueField() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);
        SearchResponse accurateResponse = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(DOUBLE_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(0)
                        .shardSize(0)
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .execute().actionGet();

        assertSearchResponse(accurateResponse);
        
        SearchResponse testResponse = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(DOUBLE_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .execute().actionGet();

        assertSearchResponse(testResponse);

        assertDocCountErrorWithinBounds(size, accurateResponse, testResponse);
    }

    @Test
    public void doubleValueField_singleShard() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);
        SearchResponse accurateResponse = client().prepareSearch("idx_single_shard").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(DOUBLE_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(0)
                        .shardSize(0)
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .execute().actionGet();

        assertSearchResponse(accurateResponse);
        
        SearchResponse testResponse = client().prepareSearch("idx_single_shard").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(DOUBLE_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .execute().actionGet();

        assertSearchResponse(testResponse);

        assertNoDocCountError(size, accurateResponse, testResponse);
    }

    @Test
    public void doubleValueField_withRouting() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);
        
        SearchResponse testResponse = client().prepareSearch("idx_with_routing").setTypes("type").setRouting(String.valueOf(between(1, numRoutingValues)))
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(DOUBLE_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .execute().actionGet();

        assertSearchResponse(testResponse);

        assertNoDocCountErrorSingleResponse(size, testResponse);
    }

    @Test
    public void doubleValueField_docCountAsc() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);
        SearchResponse accurateResponse = client().prepareSearch("idx_single_shard").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(DOUBLE_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(0)
                        .shardSize(0)
                        .order(Order.count(true))
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .execute().actionGet();

        assertSearchResponse(accurateResponse);
        
        SearchResponse testResponse = client().prepareSearch("idx_single_shard").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(DOUBLE_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .order(Order.count(true))
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .execute().actionGet();

        assertSearchResponse(testResponse);

        assertUnboundedDocCountError(size, accurateResponse, testResponse);
    }

    @Test
    public void doubleValueField_termSortAsc() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);
        SearchResponse accurateResponse = client().prepareSearch("idx_single_shard").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(DOUBLE_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(0)
                        .shardSize(0)
                        .order(Order.term(true))
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .execute().actionGet();

        assertSearchResponse(accurateResponse);
        
        SearchResponse testResponse = client().prepareSearch("idx_single_shard").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(DOUBLE_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .order(Order.term(true))
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .execute().actionGet();

        assertSearchResponse(testResponse);

        assertNoDocCountError(size, accurateResponse, testResponse);
    }

    @Test
    public void doubleValueField_termSortDesc() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);
        SearchResponse accurateResponse = client().prepareSearch("idx_single_shard").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(DOUBLE_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(0)
                        .shardSize(0)
                        .order(Order.term(false))
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .execute().actionGet();

        assertSearchResponse(accurateResponse);
        
        SearchResponse testResponse = client().prepareSearch("idx_single_shard").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(DOUBLE_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .order(Order.term(false))
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .execute().actionGet();

        assertSearchResponse(testResponse);

        assertNoDocCountError(size, accurateResponse, testResponse);
    }

    @Test
    public void doubleValueField_subAggAsc() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);
        SearchResponse accurateResponse = client().prepareSearch("idx_single_shard").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(DOUBLE_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(0)
                        .shardSize(0)
                        .order(Order.aggregation("sortAgg", true))
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .subAggregation(sum("sortAgg").field(LONG_FIELD_NAME)))
                .execute().actionGet();

        assertSearchResponse(accurateResponse);
        
        SearchResponse testResponse = client().prepareSearch("idx_single_shard").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(DOUBLE_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .order(Order.aggregation("sortAgg", true))
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .subAggregation(sum("sortAgg").field(LONG_FIELD_NAME)))
                .execute().actionGet();

        assertSearchResponse(testResponse);

        assertUnboundedDocCountError(size, accurateResponse, testResponse);
    }

    @Test
    public void doubleValueField_subAggDesc() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);
        SearchResponse accurateResponse = client().prepareSearch("idx_single_shard").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(DOUBLE_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(0)
                        .shardSize(0)
                        .order(Order.aggregation("sortAgg", false))
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .subAggregation(sum("sortAgg").field(LONG_FIELD_NAME)))
                .execute().actionGet();

        assertSearchResponse(accurateResponse);
        
        SearchResponse testResponse = client().prepareSearch("idx_single_shard").setTypes("type")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(DOUBLE_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .order(Order.aggregation("sortAgg", false))
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .subAggregation(sum("sortAgg").field(LONG_FIELD_NAME)))
                .execute().actionGet();

        assertSearchResponse(testResponse);

        assertUnboundedDocCountError(size, accurateResponse, testResponse);
    }

}
