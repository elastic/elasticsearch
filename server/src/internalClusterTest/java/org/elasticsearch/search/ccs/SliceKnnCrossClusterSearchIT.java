/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.ccs;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.junit.Before;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

/**
 * Knn searches that select slices across clusters. Each cluster applies the slices of the request to its own indices, and the
 * vector field of each slice-enabled index decides whether a knn search may run without named slices.
 */
public class SliceKnnCrossClusterSearchIT extends AbstractMultiClustersTestCase {

    private static final String REMOTE = "cluster_a";
    private static final String SLICED = "tenants";
    private static final Map<String, Integer> DOCS_PER_SLICE = Map.of("acme", 4, "globex", 3);
    private static final String SLICES_REQUIRED = "to perform knn search on field [vector], the slices to search must be selected";

    @Override
    protected List<String> remoteClusterAlias() {
        return List.of(REMOTE);
    }

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE, false);
    }

    @Before
    public void setupIndices() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        for (String cluster : List.of(LOCAL_CLUSTER, REMOTE)) {
            createSlicedIndex(cluster);
        }
    }

    public void testKnnOnSelectedSlicesOfEveryCluster() throws Exception {
        SearchRequest request = knnSearch(SLICED, REMOTE + ":" + SLICED).searchSlice("acme");
        assertResponse(client(LOCAL_CLUSTER).search(request), response -> {
            assertNoFailures(response);
            Set<String> clusters = new HashSet<>();
            for (SearchHit hit : response.getHits().getHits()) {
                assertThat(hit.getId(), startsWith("acme-"));
                clusters.add(hit.getClusterAlias() == null ? LOCAL_CLUSTER : hit.getClusterAlias());
            }
            assertThat(response.getHits().getHits().length, equalTo(2 * DOCS_PER_SLICE.get("acme")));
            assertThat(clusters, equalTo(Set.of(LOCAL_CLUSTER, REMOTE)));
        });
    }

    public void testKnnRequiresSelectedSlicesOnEveryCluster() {
        for (String[] indices : List.of(new String[] { REMOTE + ":" + SLICED }, new String[] { SLICED, REMOTE + ":" + SLICED })) {
            SearchRequest request = knnSearch(indices);
            if (randomBoolean()) {
                request.searchSlice(SliceIndexing.SLICE_ALL);
            }
            Exception e = expectThrows(Exception.class, () -> client(LOCAL_CLUSTER).search(request).actionGet().decRef());
            assertThat(List.of(indices).toString(), ExceptionsHelper.stackTrace(e), containsString(SLICES_REQUIRED));
        }
    }

    private SearchRequest knnSearch(String... indices) {
        SearchRequest request = new SearchRequest(indices).source(
            new SearchSourceBuilder().knnSearch(List.of(new KnnSearchBuilder("vector", new float[] { 1, 0, 0 }, 10, 20, null, null, null)))
                .size(20)
        );
        request.setCcsMinimizeRoundtrips(randomBoolean());
        request.allowPartialSearchResults(false);
        return request;
    }

    private void createSlicedIndex(String cluster) {
        Client client = client(cluster);
        Settings.Builder settings = Settings.builder()
            .put("index.number_of_shards", randomIntBetween(1, 3))
            .put(IndexSettings.SLICE_ENABLED.getKey(), true);
        assertAcked(client.admin().indices().prepareCreate(SLICED).setSettings(settings).setMapping("""
            { "properties": { "vector": { "type": "dense_vector", "dims": 3, "similarity": "l2_norm" } } }"""));
        BulkRequestBuilder bulk = client.prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        DOCS_PER_SLICE.forEach((slice, docs) -> {
            for (int i = 0; i < docs; i++) {
                bulk.add(
                    new IndexRequest(SLICED).id(slice + "-" + i)
                        .source("vector", new float[] { 1, i, 0 })
                        .routing(slice)
                        .setRoutingFromSlice(true)
                );
            }
        });
        assertNoFailures(bulk.get());
    }
}
