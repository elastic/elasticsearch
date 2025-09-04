/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.TermsLookup;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.InternalStats;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class KnnSearchSingleNodeTests extends ESSingleNodeTestCase {
    private static final int VECTOR_DIMENSION = 10;

    public void testKnnSearchRemovedVector() throws IOException {
        int numShards = 1 + randomInt(3);
        Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards).build();

        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("vector")
            .field("type", "dense_vector")
            .field("dims", VECTOR_DIMENSION)
            .field("index", true)
            .field("similarity", "l2_norm")
            .endObject()
            .startObject("text")
            .field("type", "text")
            .endObject()
            .endObject()
            .endObject();
        createIndex("index", indexSettings, builder);

        for (int doc = 0; doc < 10; doc++) {
            prepareIndex("index").setId(Integer.toString(doc)).setSource("vector", randomVector(), "text", "hello world").get();
            prepareIndex("index").setSource("text", "goodnight world").get();
        }

        indicesAdmin().prepareRefresh("index").get();
        client().prepareUpdate("index", "0").setDoc("vector", (Object) null).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        float[] queryVector = randomVector();
        KnnSearchBuilder knnSearch = new KnnSearchBuilder("vector", queryVector, 20, 50, null, null).boost(5.0f);
        assertResponse(
            client().prepareSearch("index")
                .setKnnSearch(List.of(knnSearch))
                .setQuery(QueryBuilders.matchQuery("text", "goodnight"))
                .setSize(10),
            response -> {
                // Originally indexed 20 documents, but deleted vector field with an update, so only 19 should be hit
                assertHitCount(response, 19);
                assertEquals(10, response.getHits().getHits().length);
            }
        );
        // Make sure we still have 20 docs
        assertHitCount(client().prepareSearch("index").setSize(0).setTrackTotalHits(true), 20);
    }

    public void testKnnWithQuery() throws IOException {
        int numShards = 1 + randomInt(3);
        Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards).build();

        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("vector")
            .field("type", "dense_vector")
            .field("dims", VECTOR_DIMENSION)
            .field("index", true)
            .field("similarity", "l2_norm")
            .endObject()
            .startObject("text")
            .field("type", "text")
            .endObject()
            .endObject()
            .endObject();
        createIndex("index", indexSettings, builder);

        for (int doc = 0; doc < 10; doc++) {
            prepareIndex("index").setSource("vector", randomVector(), "text", "hello world").get();
            prepareIndex("index").setSource("text", "goodnight world").get();
        }

        indicesAdmin().prepareRefresh("index").get();

        float[] queryVector = randomVector();
        KnnSearchBuilder knnSearch = new KnnSearchBuilder("vector", queryVector, 5, 50, null, null).boost(5.0f).queryName("knn");
        assertResponse(
            client().prepareSearch("index")
                .setKnnSearch(List.of(knnSearch))
                .setQuery(QueryBuilders.matchQuery("text", "goodnight").queryName("query"))
                .addFetchField("*")
                .setSize(10),
            response -> {

                // The total hits is k plus the number of text matches
                assertHitCount(response, 15);
                assertEquals(10, response.getHits().getHits().length);

                // Because of the boost, vector results should appear first
                assertNotNull(response.getHits().getAt(0).field("vector"));
                assertEquals(response.getHits().getAt(0).getMatchedQueries()[0], "knn");
                assertEquals(response.getHits().getAt(9).getMatchedQueries()[0], "query");
            }
        );
    }

    public void testKnnFilter() throws IOException {
        int numShards = 1 + randomInt(3);
        Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards).build();

        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("vector")
            .field("type", "dense_vector")
            .field("dims", VECTOR_DIMENSION)
            .field("index", true)
            .field("similarity", "l2_norm")
            .endObject()
            .startObject("field")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject();
        createIndex("index", indexSettings, builder);

        for (int doc = 0; doc < 10; doc++) {
            String value = doc % 2 == 0 ? "first" : "second";
            prepareIndex("index").setId(String.valueOf(doc)).setSource("vector", randomVector(), "field", value).get();
        }

        indicesAdmin().prepareRefresh("index").get();

        float[] queryVector = randomVector();
        KnnSearchBuilder knnSearch = new KnnSearchBuilder("vector", queryVector, 5, 50, null, null).addFilterQuery(
            QueryBuilders.termsQuery("field", "second")
        );
        assertResponse(client().prepareSearch("index").setKnnSearch(List.of(knnSearch)).addFetchField("*").setSize(10), response -> {
            assertHitCount(response, 5);
            assertEquals(5, response.getHits().getHits().length);
            for (SearchHit hit : response.getHits().getHits()) {
                assertEquals("second", hit.field("field").getValue());
            }
        });
    }

    public void testKnnFilterWithRewrite() throws IOException {
        int numShards = 1 + randomInt(3);
        Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards).build();

        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("vector")
            .field("type", "dense_vector")
            .field("dims", VECTOR_DIMENSION)
            .field("index", true)
            .field("similarity", "l2_norm")
            .endObject()
            .startObject("field")
            .field("type", "keyword")
            .endObject()
            .startObject("other-field")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject();
        createIndex("index", indexSettings, builder);

        for (int doc = 0; doc < 10; doc++) {
            prepareIndex("index").setId(String.valueOf(doc)).setSource("vector", randomVector(), "field", "value").get();
        }
        prepareIndex("index").setId("lookup-doc").setSource("other-field", "value").get();

        indicesAdmin().prepareRefresh("index").get();

        float[] queryVector = randomVector();
        KnnSearchBuilder knnSearch = new KnnSearchBuilder("vector", queryVector, 5, 50, null, null).addFilterQuery(
            QueryBuilders.termsLookupQuery("field", new TermsLookup("index", "lookup-doc", "other-field"))
        );
        assertResponse(client().prepareSearch("index").setKnnSearch(List.of(knnSearch)).setSize(10), response -> {
            assertHitCount(response, 5);
            assertEquals(5, response.getHits().getHits().length);
        });
    }

    public void testMultiKnnClauses() throws IOException {
        // This tests the recall from vectors being searched in different docs
        int numShards = 1 + randomInt(3);
        Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards).build();

        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("vector")
            .field("type", "dense_vector")
            .field("dims", VECTOR_DIMENSION)
            .field("index", true)
            .field("similarity", "l2_norm")
            .endObject()
            .startObject("vector_2")
            .field("type", "dense_vector")
            .field("dims", VECTOR_DIMENSION)
            .field("index", true)
            .field("similarity", "l2_norm")
            .endObject()
            .startObject("text")
            .field("type", "text")
            .endObject()
            .startObject("number")
            .field("type", "long")
            .endObject()
            .endObject()
            .endObject();
        createIndex("index", indexSettings, builder);

        for (int doc = 0; doc < 10; doc++) {
            prepareIndex("index").setSource("vector", randomVector(1.0f, 2.0f), "text", "hello world", "number", 1).get();
            prepareIndex("index").setSource("vector_2", randomVector(20f, 21f), "text", "hello world", "number", 2).get();
            prepareIndex("index").setSource("text", "goodnight world", "number", 3).get();
        }
        indicesAdmin().prepareRefresh("index").get();

        float[] queryVector = randomVector(20f, 21f);
        KnnSearchBuilder knnSearch = new KnnSearchBuilder("vector", queryVector, 5, 50, null, null).boost(5.0f);
        KnnSearchBuilder knnSearch2 = new KnnSearchBuilder("vector_2", queryVector, 5, 50, null, null).boost(10.0f);
        assertResponse(
            client().prepareSearch("index")
                .setKnnSearch(List.of(knnSearch, knnSearch2))
                .setQuery(QueryBuilders.matchQuery("text", "goodnight"))
                .addFetchField("*")
                .setSize(10)
                .addAggregation(AggregationBuilders.stats("stats").field("number")),
            response -> {

                // The total hits is k plus the number of text matches
                assertHitCount(response, 20);
                assertEquals(10, response.getHits().getHits().length);
                InternalStats agg = response.getAggregations().get("stats");
                assertThat(agg.getCount(), equalTo(20L));
                assertThat(agg.getMax(), equalTo(3.0));
                assertThat(agg.getMin(), equalTo(1.0));
                assertThat(agg.getAvg(), equalTo(2.25));
                assertThat(agg.getSum(), equalTo(45.0));

                // Because of the boost & vector distributions, vector_2 results should appear first
                assertNotNull(response.getHits().getAt(0).field("vector_2"));
            }
        );
    }

    public void testMultiKnnClausesSameDoc() throws IOException {
        int numShards = 1 + randomInt(3);
        Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards).build();

        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("vector")
            .field("type", "dense_vector")
            .field("dims", VECTOR_DIMENSION)
            .field("index", true)
            .field("similarity", "l2_norm")
            .endObject()
            .startObject("vector_2")
            .field("type", "dense_vector")
            .field("dims", VECTOR_DIMENSION)
            .field("index", true)
            .field("similarity", "l2_norm")
            .endObject()
            .startObject("number")
            .field("type", "long")
            .endObject()
            .endObject()
            .endObject();
        createIndex("index", indexSettings, builder);

        for (int doc = 0; doc < 10; doc++) {
            // Make them have hte same vector. This will allow us to test the recall is the same but scores take into account both fields
            float[] vector = randomVector();
            prepareIndex("index").setSource("vector", vector, "vector_2", vector, "number", doc).get();
        }
        indicesAdmin().prepareRefresh("index").get();

        float[] queryVector = randomVector();
        // Having the same query vector and same docs should mean our KNN scores are linearly combined if the same doc is matched
        KnnSearchBuilder knnSearch = new KnnSearchBuilder("vector", queryVector, 5, 50, null, null);
        KnnSearchBuilder knnSearch2 = new KnnSearchBuilder("vector_2", queryVector, 5, 50, null, null);
        assertResponse(
            client().prepareSearch("index")
                .setKnnSearch(List.of(knnSearch))
                .addFetchField("*")
                .setSize(10)
                .addAggregation(AggregationBuilders.stats("stats").field("number")),
            responseOneKnn -> assertResponse(
                client().prepareSearch("index")
                    .setKnnSearch(List.of(knnSearch, knnSearch2))
                    .addFetchField("*")
                    .setSize(10)
                    .addAggregation(AggregationBuilders.stats("stats").field("number")),
                responseBothKnn -> {

                    // The total hits is k matched docs
                    assertHitCount(responseOneKnn, 5);
                    assertHitCount(responseBothKnn, 5);
                    assertEquals(5, responseOneKnn.getHits().getHits().length);
                    assertEquals(5, responseBothKnn.getHits().getHits().length);

                    for (int i = 0; i < responseOneKnn.getHits().getHits().length; i++) {
                        SearchHit oneHit = responseOneKnn.getHits().getHits()[i];
                        SearchHit bothHit = responseBothKnn.getHits().getHits()[i];
                        assertThat(bothHit.getId(), equalTo(oneHit.getId()));
                        assertThat(bothHit.getScore(), greaterThan(oneHit.getScore()));
                    }
                    InternalStats oneAgg = responseOneKnn.getAggregations().get("stats");
                    InternalStats bothAgg = responseBothKnn.getAggregations().get("stats");
                    assertThat(bothAgg.getCount(), equalTo(oneAgg.getCount()));
                    assertThat(bothAgg.getAvg(), equalTo(oneAgg.getAvg()));
                    assertThat(bothAgg.getMax(), equalTo(oneAgg.getMax()));
                    assertThat(bothAgg.getSum(), equalTo(oneAgg.getSum()));
                    assertThat(bothAgg.getMin(), equalTo(oneAgg.getMin()));
                }
            )
        );
    }

    public void testKnnFilteredAlias() throws IOException {
        int numShards = 1 + randomInt(3);
        Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards).build();

        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("vector")
            .field("type", "dense_vector")
            .field("dims", VECTOR_DIMENSION)
            .field("index", true)
            .field("similarity", "l2_norm")
            .endObject()
            .startObject("field")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject();
        createIndex("index", indexSettings, builder);
        indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
            .addAlias("index", "test-alias", QueryBuilders.termQuery("field", "hit"))
            .get();

        int expectedHits = 0;
        for (int doc = 0; doc < 10; doc++) {
            if (randomBoolean()) {
                prepareIndex("index").setId(String.valueOf(doc)).setSource("vector", randomVector(), "field", "hit").get();
                ++expectedHits;
            } else {
                prepareIndex("index").setId(String.valueOf(doc)).setSource("vector", randomVector(), "field", "not hit").get();
            }
        }
        indicesAdmin().prepareRefresh("index").get();

        float[] queryVector = randomVector();
        KnnSearchBuilder knnSearch = new KnnSearchBuilder("vector", queryVector, 10, 50, null, null);
        final int expectedHitCount = expectedHits;
        assertResponse(client().prepareSearch("test-alias").setKnnSearch(List.of(knnSearch)).setSize(10), response -> {
            assertHitCount(response, expectedHitCount);
            assertEquals(expectedHitCount, response.getHits().getHits().length);
        });
    }

    public void testKnnSearchAction() throws IOException {
        Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).build();
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("vector")
            .field("type", "dense_vector")
            .field("dims", VECTOR_DIMENSION)
            .field("index", true)
            .field("similarity", "l2_norm")
            .endObject()
            .endObject()
            .endObject();
        createIndex("index1", indexSettings, builder);
        createIndex("index2", indexSettings, builder);

        for (int doc = 0; doc < 10; doc++) {
            prepareIndex("index1").setId(String.valueOf(doc)).setSource("vector", randomVector()).get();
            prepareIndex("index2").setId(String.valueOf(doc)).setSource("vector", randomVector()).get();
        }

        indicesAdmin().prepareForceMerge("index1", "index2").setMaxNumSegments(1).get();
        indicesAdmin().prepareRefresh("index1", "index2").get();

        // Since there's no kNN search action at the transport layer, we just emulate
        // how the action works (it builds a kNN query under the hood)
        float[] queryVector = randomVector();
        assertResponse(
            client().prepareSearch("index1", "index2")
                .setQuery(new KnnVectorQueryBuilder("vector", queryVector, 5, 5, null, null))
                .setSize(2),
            response -> {
                // The total hits is num_cands * num_shards, since the query gathers num_cands hits from each shard
                assertHitCount(response, 5 * 2);
                assertEquals(2, response.getHits().getHits().length);
            }
        );
    }

    public void testKnnVectorsWith4096Dims() throws IOException {
        int numShards = 1 + randomInt(3);
        Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards).build();

        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("vector")
            .field("type", "dense_vector")
            .field("dims", 4096)
            .field("index", true)
            .field("similarity", "l2_norm")
            .endObject()
            .endObject()
            .endObject();
        createIndex("index", indexSettings, builder);

        for (int doc = 0; doc < 10; doc++) {
            prepareIndex("index").setSource("vector", randomVector(4096)).get();
        }

        indicesAdmin().prepareRefresh("index").get();

        float[] queryVector = randomVector(4096);
        KnnSearchBuilder knnSearch = new KnnSearchBuilder("vector", queryVector, 3, 50, null, null).boost(5.0f);
        assertResponse(client().prepareSearch("index").setKnnSearch(List.of(knnSearch)).addFetchField("*").setSize(10), response -> {
            assertHitCount(response, 3);
            assertEquals(3, response.getHits().getHits().length);
            assertEquals(4096, response.getHits().getAt(0).field("vector").getValues().size());
        });
    }

    private float[] randomVector() {
        float[] vector = new float[VECTOR_DIMENSION];
        for (int i = 0; i < vector.length; i++) {
            vector[i] = randomFloat();
        }
        return vector;
    }

    private float[] randomVector(int dims) {
        float[] vector = new float[dims];
        for (int i = 0; i < vector.length; i++) {
            vector[i] = randomFloat();
        }
        return vector;
    }

    private float[] randomVector(float dimLower, float dimUpper) {
        float[] vector = new float[VECTOR_DIMENSION];
        for (int i = 0; i < vector.length; i++) {
            vector[i] = (float) randomDoubleBetween(dimLower, dimUpper, true);
        }
        return vector;
    }
}
