/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.basic;

import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * This test basically verifies that search with a single shard active (cause we indexed to it) and other
 * shards possibly not active at all (cause they haven't allocated) will still work.
 */
public class SearchWhileCreatingIndexIT extends ESIntegTestCase {
    public void testIndexCausesIndexCreation() throws Exception {
        searchWhileCreatingIndex(false, 1); // 1 replica in our default...
    }

    public void testNoReplicas() throws Exception {
        searchWhileCreatingIndex(true, 0);
    }

    public void testOneReplica() throws Exception {
        searchWhileCreatingIndex(true, 1);
    }

    public void testTwoReplicas() throws Exception {
        searchWhileCreatingIndex(true, 2);
    }

    private void searchWhileCreatingIndex(boolean createIndex, int numberOfReplicas) throws Exception {

        // TODO: randomize the wait for active shards value on index creation and ensure the appropriate
        // number of data nodes are started for the randomized active shard count value
        String id = randomAlphaOfLength(5);
        // we will go the primary or the replica, but in a
        // randomized re-creatable manner
        int counter = 0;
        String preference = randomAlphaOfLength(5);

        logger.info("running iteration for id {}, preference {}", id, preference);

        if (createIndex) {
            createIndex("test");
        }
        client().prepareIndex("test").setId(id).setSource("field", "test").get();
        RefreshResponse refreshResponse = indicesAdmin().prepareRefresh("test").get();
        // at least one shard should be successful when refreshing
        assertThat(refreshResponse.getSuccessfulShards(), greaterThanOrEqualTo(1));

        logger.info("using preference {}", preference);
        // we want to make sure that while recovery happens, and a replica gets recovered, its properly refreshed
        ClusterHealthStatus status = clusterAdmin().prepareHealth("test").get().getStatus();
        while (status != ClusterHealthStatus.GREEN) {
            // first, verify that search normal search works
            SearchResponse searchResponse = client().prepareSearch("test").setQuery(QueryBuilders.termQuery("field", "test")).get();
            assertHitCount(searchResponse, 1);
            Client client = client();
            searchResponse = client.prepareSearch("test")
                .setPreference(preference + Integer.toString(counter++))
                .setQuery(QueryBuilders.termQuery("field", "test"))
                .get();
            if (searchResponse.getHits().getTotalHits().value != 1) {
                refresh();
                SearchResponse searchResponseAfterRefresh = client.prepareSearch("test")
                    .setPreference(preference)
                    .setQuery(QueryBuilders.termQuery("field", "test"))
                    .get();
                logger.info(
                    "hits count mismatch on any shard search failed, post explicit refresh hits are {}",
                    searchResponseAfterRefresh.getHits().getTotalHits().value
                );
                ensureGreen();
                SearchResponse searchResponseAfterGreen = client.prepareSearch("test")
                    .setPreference(preference)
                    .setQuery(QueryBuilders.termQuery("field", "test"))
                    .get();
                logger.info(
                    "hits count mismatch on any shard search failed, post explicit wait for green hits are {}",
                    searchResponseAfterGreen.getHits().getTotalHits().value
                );
                assertHitCount(searchResponse, 1);
            }
            assertHitCount(searchResponse, 1);
            status = clusterAdmin().prepareHealth("test").get().getStatus();
            internalCluster().ensureAtLeastNumDataNodes(numberOfReplicas + 1);
        }
        cluster().wipeIndices("test");
    }
}
