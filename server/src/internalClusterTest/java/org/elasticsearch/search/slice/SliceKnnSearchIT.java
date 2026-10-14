/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.slice;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.List;
import java.util.Locale;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.startsWith;

/**
 * Nearest neighbour search on a slice-enabled index whose vector format does not partition the vectors by slice. The nearest
 * vectors to the query all live in another slice than the one searched, so the search only returns hits when the neighbours
 * are selected among the documents of the requested slice.
 */
public class SliceKnnSearchIT extends ESIntegTestCase {

    private static final int DIMS = 8;
    private static final int NEAR_DOCS = 30;
    private static final int FAR_DOCS = 3;
    private static final String SLICES_REQUIRED = "to perform knn search on field [vector], the slices to search must be selected";

    public void testKnnSearchSelectsNeighboursWithinSlice() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        final String index = createSliceIndex();

        for (int k = 1; k <= FAR_DOCS; k++) {
            final int expectedHits = k;
            final SearchRequestBuilder search = prepareSearch(index).setKnnSearch(
                List.of(new KnnSearchBuilder("vector", queryVector(), k, 10, null, null, null))
            ).setSize(10);
            search.request().searchSlice("far");
            assertResponse(search, response -> {
                assertNoFailures(response);
                assertThat(response.getHits().getHits().length, equalTo(expectedHits));
                for (SearchHit hit : response.getHits().getHits()) {
                    assertThat(hit.getId(), startsWith("far-"));
                }
            });
        }
    }

    public void testKnnQuerySelectsNeighboursWithinSlice() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        final String index = createSliceIndex();

        final SearchRequestBuilder search = prepareSearch(index).setQuery(
            new KnnVectorQueryBuilder("vector", queryVector(), 2, 10, null, null, null)
        ).setSize(10);
        search.request().searchSlice("far");
        assertResponse(search, response -> {
            assertNoFailures(response);
            // a knn query returns up to num_candidates documents, so every document of the slice may be returned
            assertThat(response.getHits().getHits().length, greaterThanOrEqualTo(2));
            for (SearchHit hit : response.getHits().getHits()) {
                assertThat(hit.getId(), startsWith("far-"));
            }
        });
    }

    /**
     * The vector field refuses a nearest neighbour search that does not name the slices to search, as a knn search or as a
     * knn query at any depth of the query.
     */
    public void testKnnRequiresSelectedSlices() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        final String index = createSliceIndex();

        final KnnVectorQueryBuilder knnQuery = new KnnVectorQueryBuilder("vector", queryVector(), 2, 10, null, null, null);
        for (SearchRequestBuilder search : List.of(
            prepareSearch(index).setKnnSearch(List.of(new KnnSearchBuilder("vector", queryVector(), 2, 10, null, null, null))),
            prepareSearch(index).setQuery(knnQuery),
            prepareSearch(index).setQuery(QueryBuilders.boolQuery().filter(QueryBuilders.matchAllQuery()).should(knnQuery))
        )) {
            if (randomBoolean()) {
                // selecting every slice does not name the slices to search
                search.request().searchSlice(SliceIndexing.SLICE_ALL);
            }
            final Exception e = expectThrows(Exception.class, search);
            assertThat(ExceptionsHelper.status(e), equalTo(RestStatus.BAD_REQUEST));
            assertThat(ExceptionsHelper.stackTrace(e), containsString(SLICES_REQUIRED));
        }
    }

    /**
     * The decision is made by the field of each index. In a request that also targets an index without slices, only the
     * shards of the slice-enabled index fail.
     */
    public void testKnnRequiresSelectedSlicesOnlyOnSliceEnabledIndex() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        final String index = createSliceIndex();
        assertAcked(prepareCreate("plain").setSettings(Settings.builder().put("index.number_of_shards", 1)).setMapping("""
            { "properties": { "vector": { "type": "dense_vector", "dims": 8, "index": true, "similarity": "l2_norm" } } }"""));
        prepareIndex("plain").setId("plain-1").setSource("vector", queryVector()).setRouting("any").get();
        refresh("plain");

        final KnnVectorQueryBuilder knnQuery = new KnnVectorQueryBuilder("vector", queryVector(), 2, 10, null, null, null);
        assertResponse(prepareSearch(index, "plain").setQuery(knnQuery), response -> {
            assertThat(response.getFailedShards(), equalTo(1));
            assertThat(response.getShardFailures()[0].index(), equalTo(index));
            assertThat(response.getShardFailures()[0].reason(), containsString(SLICES_REQUIRED));
            assertThat(response.getHits().getHits().length, equalTo(1));
            assertThat(response.getHits().getHits()[0].getId(), equalTo("plain-1"));
        });
    }

    /**
     * An index without slices holds no document of the named slices, so its vectors, however near, take no place among the
     * nearest neighbours of a search that also targets a slice-enabled index.
     */
    public void testNamedSlicesOnIndexWithoutSlices() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        final String index = createSliceIndex();
        assertAcked(prepareCreate("plain").setSettings(Settings.builder().put("index.number_of_shards", 1)).setMapping("""
            { "properties": { "vector": { "type": "dense_vector", "dims": 8, "index": true, "similarity": "l2_norm" } } }"""));
        final BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < NEAR_DOCS; i++) {
            bulk.add(new IndexRequest("plain").id("plain-" + i).source("vector", vector(1f, i * 0.001f)).routing("far"));
        }
        assertNoFailures(bulk.get());

        final SearchRequestBuilder search = prepareSearch(index, "plain").setKnnSearch(
            List.of(new KnnSearchBuilder("vector", queryVector(), FAR_DOCS, 10, null, null, null))
        ).setSize(10);
        search.request().searchSlice("far");
        assertResponse(search, response -> {
            assertNoFailures(response);
            assertThat(response.getHits().getHits().length, equalTo(FAR_DOCS));
            for (SearchHit hit : response.getHits().getHits()) {
                assertThat(hit.getId(), startsWith("far-"));
            }
        });
    }

    /**
     * A request that names no slice reads every slice when none of its queries needs them by name.
     */
    public void testOtherQueriesNeedNoSlices() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        final String index = createSliceIndex();

        for (QueryBuilder query : List.of(QueryBuilders.matchAllQuery(), QueryBuilders.existsQuery("vector"))) {
            final SearchRequestBuilder search = prepareSearch(index).setQuery(query).setSize(0);
            if (randomBoolean()) {
                search.request().searchSlice(SliceIndexing.SLICE_ALL);
            }
            assertHitCount(search, NEAR_DOCS + FAR_DOCS);
        }
    }

    /**
     * An index without slices has no document in any slice: a search that names a slice returns nothing from it, even
     * when its documents are routed with the name of the slice.
     */
    public void testSliceMatchesNothingOnIndexWithoutSlices() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        final String sliced = createSliceIndex();
        assertAcked(prepareCreate("plain").setSettings(Settings.builder().put("index.number_of_shards", 1)));
        prepareIndex("plain").setId("plain-1").setSource("field", "value").setRouting("far").get();
        prepareIndex("plain").setId("plain-2").setSource("field", "value").get();
        refresh("plain");

        final SearchRequestBuilder search = prepareSearch(sliced, "plain").setSize(100);
        search.request().searchSlice("far");
        assertResponse(search, response -> {
            assertNoFailures(response);
            assertThat(response.getHits().getHits().length, equalTo(FAR_DOCS));
            for (SearchHit hit : response.getHits().getHits()) {
                assertThat(hit.getId(), startsWith("far-"));
            }
        });
    }

    private String createSliceIndex() {
        final String index = "slice-knn";
        final String indexOptions = randomFrom("hnsw", "int8_hnsw", "flat", "int8_flat");
        assertAcked(
            prepareCreate(index).setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put(IndexSettings.SLICE_ENABLED.getKey(), true)
            ).setMapping(String.format(Locale.ROOT, """
                {
                  "properties": {
                    "vector": {
                      "type": "dense_vector",
                      "dims": %d,
                      "index": true,
                      "similarity": "l2_norm",
                      "index_options": { "type": "%s" }
                    }
                  }
                }""", DIMS, indexOptions))
        );
        ensureGreen(index);

        final BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < NEAR_DOCS; i++) {
            bulk.add(
                new IndexRequest(index).id("near-" + i).source("vector", vector(1f, i * 0.001f)).routing("near").setRoutingFromSlice(true)
            );
        }
        for (int i = 0; i < FAR_DOCS; i++) {
            bulk.add(
                new IndexRequest(index).id("far-" + i).source("vector", vector(-1f, i * 0.001f)).routing("far").setRoutingFromSlice(true)
            );
        }
        assertNoFailures(bulk.get());
        return index;
    }

    private static float[] queryVector() {
        return vector(1f, 0f);
    }

    private static float[] vector(float first, float second) {
        final float[] vector = new float[DIMS];
        vector[0] = first;
        vector[1] = second;
        return vector;
    }
}
