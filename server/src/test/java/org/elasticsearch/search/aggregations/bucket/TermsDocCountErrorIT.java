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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregatorFactory.ExecutionMode;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.search.aggregations.AggregationBuilders.sum;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.core.IsNull.notNullValue;

@ESIntegTestCase.SuiteScopeTestCase
public class TermsDocCountErrorIT extends ESIntegTestCase {

    private static final String STRING_FIELD_NAME = "s_value";
    private static final String LONG_FIELD_NAME = "l_value";
    private static final String DOUBLE_FIELD_NAME = "d_value";


    public static String randomExecutionHint() {
        return randomBoolean() ? null : randomFrom(ExecutionMode.values()).toString();
    }

    private static int numRoutingValues;

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("idx")
                .addMapping("type", STRING_FIELD_NAME, "type=keyword").get());
        List<IndexRequestBuilder> builders = new ArrayList<>();
        int numDocs = between(10, 200);
        int numUniqueTerms = between(2,numDocs/2);
        for (int i = 0; i < numDocs; i++) {
            builders.add(client().prepareIndex("idx").setId(""+i).setSource(jsonBuilder()
                    .startObject()
                    .field(STRING_FIELD_NAME, "val" + randomInt(numUniqueTerms))
                    .field(LONG_FIELD_NAME, randomInt(numUniqueTerms))
                    .field(DOUBLE_FIELD_NAME, 1.0 * randomInt(numUniqueTerms))
                    .endObject()));
        }
        assertAcked(prepareCreate("idx_single_shard")
                .addMapping("type", STRING_FIELD_NAME, "type=keyword")
                .setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)));
        for (int i = 0; i < numDocs; i++) {
            builders.add(client().prepareIndex("idx_single_shard").setId(""+i).setSource(jsonBuilder()
                    .startObject()
                    .field(STRING_FIELD_NAME, "val" + randomInt(numUniqueTerms))
                    .field(LONG_FIELD_NAME, randomInt(numUniqueTerms))
                    .field(DOUBLE_FIELD_NAME, 1.0 * randomInt(numUniqueTerms))
                    .endObject()));
        }
        numRoutingValues = between(1,40);
        assertAcked(prepareCreate("idx_with_routing")
            .addMapping("type", "{ \"type\" : { \"_routing\" : { \"required\" : true } } }", XContentType.JSON));
        for (int i = 0; i < numDocs; i++) {
            builders.add(client().prepareIndex("idx_single_shard").setId("" + i)
                .setRouting(String.valueOf(randomInt(numRoutingValues)))
                .setSource(jsonBuilder()
                    .startObject()
                    .field(STRING_FIELD_NAME, "val" + randomInt(numUniqueTerms))
                    .field(LONG_FIELD_NAME, randomInt(numUniqueTerms))
                    .field(DOUBLE_FIELD_NAME, 1.0 * randomInt(numUniqueTerms))
                    .endObject()));
        }
        Map<String, Integer> shard0DocsPerTerm = Map.of(
            "A", 25,
            "B", 18,
            "C", 6,
            "D", 3,
            "E", 2,
            "F", 2,
            "G", 2,
            "H", 2,
            "I", 1,
            "J", 1
        );
        buildIndex(shard0DocsPerTerm, "idx_fixed_docs_0", 0, builders);

        Map<String, Integer> shard1DocsPerTerm = Map.of(
            "A", 30,
            "B", 25,
            "F", 17,
            "Z", 16,
            "G", 15,
            "H", 14,
            "I", 10,
            "Q", 6,
            "J", 8,
            "C", 4
        );
        buildIndex(shard1DocsPerTerm, "idx_fixed_docs_1", 1, builders);

        Map<String, Integer> shard2DocsPerTerm = Map.of(
            "A", 45,
            "C", 44,
            "Z", 36,
            "G", 30,
            "E", 29,
            "H", 28,
            "Q", 2,
            "D", 1
        );
        buildIndex(shard2DocsPerTerm, "idx_fixed_docs_2", 2, builders);

        //------

        Map<String, Integer> shard3DocsPerTerm = Map.of(
            "A", 1,
            "B", 1,
            "C", 1
        );
        buildIndex(shard3DocsPerTerm, "idx_fixed_docs_3", 3, builders);

        Map<String, Integer> shard4DocsPerTerm = Map.of(
            "K", 1,
            "L", 1,
            "M", 1
        );
        buildIndex(shard4DocsPerTerm, "idx_fixed_docs_4", 4, builders);

        Map<String, Integer> shard5DocsPerTerm = Map.of(
            "X", 1,
            "Y", 1,
            "Z", 1
        );
        buildIndex(shard5DocsPerTerm, "idx_fixed_docs_5", 5, builders);

        indexRandom(true, builders);
        ensureSearchable();
    }

    private void buildIndex(Map<String, Integer> docsPerTerm, String index, int shard, List<IndexRequestBuilder> builders) throws IOException {
        assertAcked(prepareCreate(index).addMapping("type", STRING_FIELD_NAME, "type=keyword")
            .setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)));
        for (Map.Entry<String, Integer> entry : docsPerTerm.entrySet()) {
            for (int i = 0; i < entry.getValue(); i++) {
                String term = entry.getKey();
                builders.add(client().prepareIndex(index).setId(term + "-" + i)
                    .setSource(jsonBuilder().startObject().field(STRING_FIELD_NAME, term).field("shard", shard).endObject()));
            }
        }
    }

    private void assertDocCountErrorWithinBounds(int size, SearchResponse accurateResponse, SearchResponse testResponse) {
        Terms accurateTerms = accurateResponse.getAggregations().get("terms");
        assertThat(accurateTerms, notNullValue());
        assertThat(accurateTerms.getName(), equalTo("terms"));
        assertThat(accurateTerms.getDocCountError(), equalTo(0L));

        Terms testTerms = testResponse.getAggregations().get("terms");
        assertThat(testTerms, notNullValue());
        assertThat(testTerms.getName(), equalTo("terms"));
        assertThat(testTerms.getDocCountError(), greaterThanOrEqualTo(0L));
        List<? extends Bucket> testBuckets = testTerms.getBuckets();
        assertThat(testBuckets.size(), lessThanOrEqualTo(size));
        assertThat(accurateTerms.getBuckets().size(), greaterThanOrEqualTo(testBuckets.size()));

        for (Terms.Bucket testBucket : testBuckets) {
            assertThat(testBucket, notNullValue());
            Terms.Bucket accurateBucket = accurateTerms.getBucketByKey(testBucket.getKeyAsString());
            assertThat(accurateBucket, notNullValue());
            assertThat(accurateBucket.getDocCountError(), equalTo(0L));
            assertThat(testBucket.getDocCountError(), lessThanOrEqualTo(testTerms.getDocCountError()));
            assertThat(testBucket.getDocCount() + testBucket.getDocCountError(), greaterThanOrEqualTo(accurateBucket.getDocCount()));
            assertThat(testBucket.getDocCount() - testBucket.getDocCountError(), lessThanOrEqualTo(accurateBucket.getDocCount()));
        }

        for (Terms.Bucket accurateBucket: accurateTerms.getBuckets()) {
            assertThat(accurateBucket, notNullValue());
            Terms.Bucket testBucket = accurateTerms.getBucketByKey(accurateBucket.getKeyAsString());
            if (testBucket == null) {
                assertThat(accurateBucket.getDocCount(), lessThanOrEqualTo(testTerms.getDocCountError()));
            }

        }
    }

    private void assertNoDocCountError(int size, SearchResponse accurateResponse, SearchResponse testResponse) {
        Terms accurateTerms = accurateResponse.getAggregations().get("terms");
        assertThat(accurateTerms, notNullValue());
        assertThat(accurateTerms.getName(), equalTo("terms"));
        assertThat(accurateTerms.getDocCountError(), equalTo(0L));

        Terms testTerms = testResponse.getAggregations().get("terms");
        assertThat(testTerms, notNullValue());
        assertThat(testTerms.getName(), equalTo("terms"));
        assertThat(testTerms.getDocCountError(), equalTo(0L));
        List<? extends Bucket> testBuckets = testTerms.getBuckets();
        assertThat(testBuckets.size(), lessThanOrEqualTo(size));
        assertThat(accurateTerms.getBuckets().size(), greaterThanOrEqualTo(testBuckets.size()));

        for (Terms.Bucket testBucket : testBuckets) {
            assertThat(testBucket, notNullValue());
            Terms.Bucket accurateBucket = accurateTerms.getBucketByKey(testBucket.getKeyAsString());
            assertThat(accurateBucket, notNullValue());
            assertThat(accurateBucket.getDocCountError(), equalTo(0L));
            assertThat(testBucket.getDocCountError(), equalTo(0L));
        }
    }

    private void assertNoDocCountErrorSingleResponse(int size, SearchResponse testResponse) {
        Terms testTerms = testResponse.getAggregations().get("terms");
        assertThat(testTerms, notNullValue());
        assertThat(testTerms.getName(), equalTo("terms"));
        assertThat(testTerms.getDocCountError(), equalTo(0L));
        List<? extends Bucket> testBuckets = testTerms.getBuckets();
        assertThat(testBuckets.size(), lessThanOrEqualTo(size));

        for (Terms.Bucket testBucket : testBuckets) {
            assertThat(testBucket, notNullValue());
            assertThat(testBucket.getDocCountError(), equalTo(0L));
        }
    }

    private void assertUnboundedDocCountError(int size, SearchResponse accurateResponse, SearchResponse testResponse) {
        Terms accurateTerms = accurateResponse.getAggregations().get("terms");
        assertThat(accurateTerms, notNullValue());
        assertThat(accurateTerms.getName(), equalTo("terms"));
        assertThat(accurateTerms.getDocCountError(), equalTo(0L));

        Terms testTerms = testResponse.getAggregations().get("terms");
        assertThat(testTerms, notNullValue());
        assertThat(testTerms.getName(), equalTo("terms"));
        assertThat(testTerms.getDocCountError(),anyOf(equalTo(-1L), equalTo(0L)));
        List<? extends Bucket> testBuckets = testTerms.getBuckets();
        assertThat(testBuckets.size(), lessThanOrEqualTo(size));
        assertThat(accurateTerms.getBuckets().size(), greaterThanOrEqualTo(testBuckets.size()));

        for (Terms.Bucket testBucket : testBuckets) {
            assertThat(testBucket, notNullValue());
            Terms.Bucket accurateBucket = accurateTerms.getBucketByKey(testBucket.getKeyAsString());
            assertThat(accurateBucket, notNullValue());
            assertThat(accurateBucket.getDocCountError(), equalTo(0L));
            assertThat(testBucket.getDocCountError(), anyOf(equalTo(-1L), equalTo(0L)));
        }
    }

    public void testStringValueField() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);
        SearchResponse accurateResponse = client().prepareSearch("idx")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(STRING_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(10000).shardSize(10000)
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .get();

        assertSearchResponse(accurateResponse);

        SearchResponse testResponse = client().prepareSearch("idx")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(STRING_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .get();

        assertSearchResponse(testResponse);

        assertDocCountErrorWithinBounds(size, accurateResponse, testResponse);
    }

    public void testStringValueFieldSingleShard() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);
        SearchResponse accurateResponse = client().prepareSearch("idx_single_shard")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(STRING_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(10000).shardSize(10000)
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .get();

        assertSearchResponse(accurateResponse);

        SearchResponse testResponse = client().prepareSearch("idx_single_shard")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(STRING_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .get();

        assertSearchResponse(testResponse);

        assertNoDocCountError(size, accurateResponse, testResponse);
    }

    public void testStringValueFieldWithRouting() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);

        SearchResponse testResponse = client().prepareSearch("idx_with_routing")
                .setRouting(String.valueOf(between(1, numRoutingValues)))
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(STRING_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .get();

        assertSearchResponse(testResponse);

        assertNoDocCountErrorSingleResponse(size, testResponse);
    }

    public void testStringValueFieldDocCountAsc() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);
        SearchResponse accurateResponse = client().prepareSearch("idx_single_shard")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(STRING_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(10000).shardSize(10000)
                        .order(BucketOrder.count(true))
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .get();

        assertSearchResponse(accurateResponse);

        SearchResponse testResponse = client().prepareSearch("idx_single_shard")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(STRING_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .order(BucketOrder.count(true))
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .get();

        assertSearchResponse(testResponse);

        assertUnboundedDocCountError(size, accurateResponse, testResponse);
    }

    public void testStringValueFieldTermSortAsc() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);
        SearchResponse accurateResponse = client().prepareSearch("idx_single_shard")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(STRING_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(10000).shardSize(10000)
                        .order(BucketOrder.key(true))
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .get();

        assertSearchResponse(accurateResponse);

        SearchResponse testResponse = client().prepareSearch("idx_single_shard")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(STRING_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .order(BucketOrder.key(true))
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .get();

        assertSearchResponse(testResponse);

        assertNoDocCountError(size, accurateResponse, testResponse);
    }

    public void testStringValueFieldTermSortDesc() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);
        SearchResponse accurateResponse = client().prepareSearch("idx_single_shard")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(STRING_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(10000).shardSize(10000)
                        .order(BucketOrder.key(false))
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .get();

        assertSearchResponse(accurateResponse);

        SearchResponse testResponse = client().prepareSearch("idx_single_shard")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(STRING_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .order(BucketOrder.key(false))
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .get();

        assertSearchResponse(testResponse);

        assertNoDocCountError(size, accurateResponse, testResponse);
    }

    public void testStringValueFieldSubAggAsc() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);
        SearchResponse accurateResponse = client().prepareSearch("idx_single_shard")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(STRING_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(10000).shardSize(10000)
                        .order(BucketOrder.aggregation("sortAgg", true))
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .subAggregation(sum("sortAgg").field(LONG_FIELD_NAME)))
                .get();

        assertSearchResponse(accurateResponse);

        SearchResponse testResponse = client().prepareSearch("idx_single_shard")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(STRING_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .order(BucketOrder.aggregation("sortAgg", true))
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .subAggregation(sum("sortAgg").field(LONG_FIELD_NAME)))
                .get();

        assertSearchResponse(testResponse);

        assertUnboundedDocCountError(size, accurateResponse, testResponse);
    }

    public void testStringValueFieldSubAggDesc() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);
        SearchResponse accurateResponse = client().prepareSearch("idx_single_shard")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(STRING_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(10000).shardSize(10000)
                        .order(BucketOrder.aggregation("sortAgg", false))
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .subAggregation(sum("sortAgg").field(LONG_FIELD_NAME)))
                .get();

        assertSearchResponse(accurateResponse);

        SearchResponse testResponse = client().prepareSearch("idx_single_shard")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(STRING_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .order(BucketOrder.aggregation("sortAgg", false))
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .subAggregation(sum("sortAgg").field(LONG_FIELD_NAME)))
                .get();

        assertSearchResponse(testResponse);

        assertUnboundedDocCountError(size, accurateResponse, testResponse);
    }

    public void testLongValueField() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);
        SearchResponse accurateResponse = client().prepareSearch("idx")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(LONG_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(10000).shardSize(10000)
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .get();

        assertSearchResponse(accurateResponse);

        SearchResponse testResponse = client().prepareSearch("idx")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(LONG_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .get();

        assertSearchResponse(testResponse);

        assertDocCountErrorWithinBounds(size, accurateResponse, testResponse);
    }

    public void testLongValueFieldSingleShard() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);
        SearchResponse accurateResponse = client().prepareSearch("idx_single_shard")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(LONG_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(10000).shardSize(10000)
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .get();

        assertSearchResponse(accurateResponse);

        SearchResponse testResponse = client().prepareSearch("idx_single_shard")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(LONG_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .get();

        assertSearchResponse(testResponse);

        assertNoDocCountError(size, accurateResponse, testResponse);
    }

    public void testLongValueFieldWithRouting() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);

        SearchResponse testResponse = client().prepareSearch("idx_with_routing")
                .setRouting(String.valueOf(between(1, numRoutingValues)))
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(LONG_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .get();

        assertSearchResponse(testResponse);

        assertNoDocCountErrorSingleResponse(size, testResponse);
    }

    public void testLongValueFieldDocCountAsc() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);
        SearchResponse accurateResponse = client().prepareSearch("idx_single_shard")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(LONG_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(10000).shardSize(10000)
                        .order(BucketOrder.count(true))
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .get();

        assertSearchResponse(accurateResponse);

        SearchResponse testResponse = client().prepareSearch("idx_single_shard")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(LONG_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .order(BucketOrder.count(true))
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .get();

        assertSearchResponse(testResponse);

        assertUnboundedDocCountError(size, accurateResponse, testResponse);
    }

    public void testLongValueFieldTermSortAsc() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);
        SearchResponse accurateResponse = client().prepareSearch("idx_single_shard")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(LONG_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(10000).shardSize(10000)
                        .order(BucketOrder.key(true))
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .get();

        assertSearchResponse(accurateResponse);

        SearchResponse testResponse = client().prepareSearch("idx_single_shard")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(LONG_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .order(BucketOrder.key(true))
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .get();

        assertSearchResponse(testResponse);

        assertNoDocCountError(size, accurateResponse, testResponse);
    }

    public void testLongValueFieldTermSortDesc() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);
        SearchResponse accurateResponse = client().prepareSearch("idx_single_shard")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(LONG_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(10000).shardSize(10000)
                        .order(BucketOrder.key(false))
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .get();

        assertSearchResponse(accurateResponse);

        SearchResponse testResponse = client().prepareSearch("idx_single_shard")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(LONG_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .order(BucketOrder.key(false))
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .get();

        assertSearchResponse(testResponse);

        assertNoDocCountError(size, accurateResponse, testResponse);
    }

    public void testLongValueFieldSubAggAsc() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);
        SearchResponse accurateResponse = client().prepareSearch("idx_single_shard")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(LONG_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(10000).shardSize(10000)
                        .order(BucketOrder.aggregation("sortAgg", true))
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .subAggregation(sum("sortAgg").field(LONG_FIELD_NAME)))
                .get();

        assertSearchResponse(accurateResponse);

        SearchResponse testResponse = client().prepareSearch("idx_single_shard")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(LONG_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .order(BucketOrder.aggregation("sortAgg", true))
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .subAggregation(sum("sortAgg").field(LONG_FIELD_NAME)))
                .get();

        assertSearchResponse(testResponse);

        assertUnboundedDocCountError(size, accurateResponse, testResponse);
    }

    public void testLongValueFieldSubAggDesc() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);
        SearchResponse accurateResponse = client().prepareSearch("idx_single_shard")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(LONG_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(10000).shardSize(10000)
                        .order(BucketOrder.aggregation("sortAgg", false))
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .subAggregation(sum("sortAgg").field(DOUBLE_FIELD_NAME)))
                .get();

        assertSearchResponse(accurateResponse);

        SearchResponse testResponse = client().prepareSearch("idx_single_shard")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(LONG_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .order(BucketOrder.aggregation("sortAgg", false))
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .subAggregation(sum("sortAgg").field(DOUBLE_FIELD_NAME)))
                .get();

        assertSearchResponse(testResponse);

        assertUnboundedDocCountError(size, accurateResponse, testResponse);
    }

    public void testDoubleValueField() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);
        SearchResponse accurateResponse = client().prepareSearch("idx")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(DOUBLE_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(10000).shardSize(10000)
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .get();

        assertSearchResponse(accurateResponse);

        SearchResponse testResponse = client().prepareSearch("idx")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(DOUBLE_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .get();

        assertSearchResponse(testResponse);

        assertDocCountErrorWithinBounds(size, accurateResponse, testResponse);
    }

    public void testDoubleValueFieldSingleShard() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);
        SearchResponse accurateResponse = client().prepareSearch("idx_single_shard")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(DOUBLE_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(10000).shardSize(10000)
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .get();

        assertSearchResponse(accurateResponse);

        SearchResponse testResponse = client().prepareSearch("idx_single_shard")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(DOUBLE_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .get();

        assertSearchResponse(testResponse);

        assertNoDocCountError(size, accurateResponse, testResponse);
    }

    public void testDoubleValueFieldWithRouting() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);

        SearchResponse testResponse = client().prepareSearch("idx_with_routing")
                .setRouting(String.valueOf(between(1, numRoutingValues)))
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(DOUBLE_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .get();

        assertSearchResponse(testResponse);

        assertNoDocCountErrorSingleResponse(size, testResponse);
    }

    public void testDoubleValueFieldDocCountAsc() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);
        SearchResponse accurateResponse = client().prepareSearch("idx_single_shard")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(DOUBLE_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(10000).shardSize(10000)
                        .order(BucketOrder.count(true))
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .get();

        assertSearchResponse(accurateResponse);

        SearchResponse testResponse = client().prepareSearch("idx_single_shard")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(DOUBLE_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .order(BucketOrder.count(true))
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .get();

        assertSearchResponse(testResponse);

        assertUnboundedDocCountError(size, accurateResponse, testResponse);
    }

    public void testDoubleValueFieldTermSortAsc() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);
        SearchResponse accurateResponse = client().prepareSearch("idx_single_shard")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(DOUBLE_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(10000).shardSize(10000)
                        .order(BucketOrder.key(true))
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .get();

        assertSearchResponse(accurateResponse);

        SearchResponse testResponse = client().prepareSearch("idx_single_shard")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(DOUBLE_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .order(BucketOrder.key(true))
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .get();

        assertSearchResponse(testResponse);

        assertNoDocCountError(size, accurateResponse, testResponse);
    }

    public void testDoubleValueFieldTermSortDesc() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);
        SearchResponse accurateResponse = client().prepareSearch("idx_single_shard")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(DOUBLE_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(10000).shardSize(10000)
                        .order(BucketOrder.key(false))
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .get();

        assertSearchResponse(accurateResponse);

        SearchResponse testResponse = client().prepareSearch("idx_single_shard")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(DOUBLE_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .order(BucketOrder.key(false))
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .get();

        assertSearchResponse(testResponse);

        assertNoDocCountError(size, accurateResponse, testResponse);
    }

    public void testDoubleValueFieldSubAggAsc() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);
        SearchResponse accurateResponse = client().prepareSearch("idx_single_shard")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(DOUBLE_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(10000).shardSize(10000)
                        .order(BucketOrder.aggregation("sortAgg", true))
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .subAggregation(sum("sortAgg").field(LONG_FIELD_NAME)))
                .get();

        assertSearchResponse(accurateResponse);

        SearchResponse testResponse = client().prepareSearch("idx_single_shard")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(DOUBLE_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .order(BucketOrder.aggregation("sortAgg", true))
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .subAggregation(sum("sortAgg").field(LONG_FIELD_NAME)))
                .get();

        assertSearchResponse(testResponse);

        assertUnboundedDocCountError(size, accurateResponse, testResponse);
    }

    public void testDoubleValueFieldSubAggDesc() throws Exception {
        int size = randomIntBetween(1, 20);
        int shardSize = randomIntBetween(size, size * 2);
        SearchResponse accurateResponse = client().prepareSearch("idx_single_shard")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(DOUBLE_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(10000).shardSize(10000)
                        .order(BucketOrder.aggregation("sortAgg", false))
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .subAggregation(sum("sortAgg").field(LONG_FIELD_NAME)))
                .get();

        assertSearchResponse(accurateResponse);

        SearchResponse testResponse = client().prepareSearch("idx_single_shard")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(DOUBLE_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(size)
                        .shardSize(shardSize)
                        .order(BucketOrder.aggregation("sortAgg", false))
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .subAggregation(sum("sortAgg").field(LONG_FIELD_NAME)))
                .get();

        assertSearchResponse(testResponse);

        assertUnboundedDocCountError(size, accurateResponse, testResponse);
    }

    /**
     * Test a case where we know exactly how many of each term is on each shard
     * so we know the exact error value for each term. To do this we search over
     * 3 one-shard indices.
     */
    public void testFixedDocs() throws Exception {
        SearchResponse response = client().prepareSearch("idx_fixed_docs_0", "idx_fixed_docs_1", "idx_fixed_docs_2")
                .addAggregation(terms("terms")
                        .executionHint(randomExecutionHint())
                        .field(STRING_FIELD_NAME)
                        .showTermDocCountError(true)
                        .size(5).shardSize(5)
                        .collectMode(randomFrom(SubAggCollectionMode.values())))
                .get();
        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        assertThat(terms.getDocCountError(), equalTo(46L));
        List<? extends Bucket> buckets = terms.getBuckets();
        assertThat(buckets, notNullValue());
        assertThat(buckets.size(), equalTo(5));

        Bucket bucket = buckets.get(0);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("A"));
        assertThat(bucket.getDocCount(), equalTo(100L));
        assertThat(bucket.getDocCountError(), equalTo(0L));

        bucket = buckets.get(1);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("Z"));
        assertThat(bucket.getDocCount(), equalTo(52L));
        assertThat(bucket.getDocCountError(), equalTo(2L));

        bucket = buckets.get(2);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("C"));
        assertThat(bucket.getDocCount(), equalTo(50L));
        assertThat(bucket.getDocCountError(), equalTo(15L));


        bucket = buckets.get(3);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("G"));
        assertThat(bucket.getDocCount(), equalTo(45L));
        assertThat(bucket.getDocCountError(), equalTo(2L));

        bucket = buckets.get(4);
        assertThat(bucket, notNullValue());
        assertThat(bucket.getKey(), equalTo("B"));
        assertThat(bucket.getDocCount(), equalTo(43L));
        assertThat(bucket.getDocCountError(), equalTo(29L));
    }

    public void testIncrementalReductionBug() {
        SearchResponse response = client().prepareSearch("idx_fixed_docs_3", "idx_fixed_docs_4", "idx_fixed_docs_5")
            .addAggregation(terms("terms")
                .executionHint(randomExecutionHint())
                .field(STRING_FIELD_NAME)
                .showTermDocCountError(true)
                .size(5).shardSize(5)
                .collectMode(randomFrom(SubAggCollectionMode.values())))
            .get();
        assertSearchResponse(response);
        Terms terms = response.getAggregations().get("terms");
        assertThat(terms.getDocCountError(), equalTo(0L));
    }
}
