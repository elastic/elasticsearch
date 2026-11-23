/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.ccs;

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
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.junit.Before;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

/**
 * Searches that name a slice across clusters. Each cluster applies the slice to its own indices: an index without slices
 * holds no document of any slice, whichever cluster it is on and whichever clusters hold slice-enabled indices.
 */
public class SliceCrossClusterSearchIT extends AbstractMultiClustersTestCase {

    private static final String REMOTE = "cluster_a";
    private static final String SLICED = "tenants";
    private static final String PLAIN = "reference";
    private static final Map<String, Integer> DOCS_PER_SLICE = Map.of("acme", 4, "globex", 3);

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
            createIndex(cluster, SLICED, true);
            createIndex(cluster, PLAIN, false);
        }
    }

    public void testSliceOnEveryCluster() throws Exception {
        SearchRequest request = search(SLICED, REMOTE + ":" + SLICED).searchSlice("acme");
        assertResponse(client(LOCAL_CLUSTER).search(request), response -> {
            assertNoFailures(response);
            assertThat(response.getHits().getHits().length, equalTo(2 * DOCS_PER_SLICE.get("acme")));
            for (SearchHit hit : response.getHits().getHits()) {
                assertThat(hit.getId(), startsWith("acme-"));
            }
        });
    }

    public void testSliceOnClusterWithoutSlicedIndex() throws Exception {
        for (String[] indices : List.of(
            new String[] { SLICED, REMOTE + ":" + PLAIN },
            new String[] { PLAIN, REMOTE + ":" + SLICED },
            new String[] { PLAIN, REMOTE + ":" + PLAIN },
            new String[] { REMOTE + ":" + PLAIN }
        )) {
            SearchRequest request = search(indices).searchSlice("acme");
            int expected = List.of(indices).toString().contains(SLICED) ? DOCS_PER_SLICE.get("acme") : 0;
            assertResponse(client(LOCAL_CLUSTER).search(request), response -> {
                assertNoFailures(response);
                assertThat(List.of(indices).toString(), response.getHits().getHits().length, equalTo(expected));
                for (SearchHit hit : response.getHits().getHits()) {
                    assertThat(hit.getIndex(), equalTo(SLICED));
                    assertThat(hit.getId(), startsWith("acme-"));
                }
            });
        }
    }

    private SearchRequest search(String... indices) {
        SearchRequest request = new SearchRequest(indices).source(new SearchSourceBuilder().size(100));
        request.setCcsMinimizeRoundtrips(randomBoolean());
        request.allowPartialSearchResults(false);
        return request;
    }

    private void createIndex(String cluster, String index, boolean sliced) {
        Client client = client(cluster);
        Settings.Builder settings = Settings.builder().put("index.number_of_shards", randomIntBetween(1, 3));
        if (sliced) {
            settings.put(IndexSettings.SLICE_ENABLED.getKey(), true);
        }
        assertAcked(client.admin().indices().prepareCreate(index).setSettings(settings));
        BulkRequestBuilder bulk = client.prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        DOCS_PER_SLICE.forEach((slice, docs) -> {
            for (int i = 0; i < docs; i++) {
                IndexRequest doc = new IndexRequest(index).id(slice + "-" + i).source("position", i);
                // the documents of the plain index are routed with the slice name as a regular routing value
                bulk.add(sliced ? doc.routing(slice).setRoutingFromSlice(true) : doc.routing(slice));
            }
        });
        assertNoFailures(bulk.get());
    }
}
