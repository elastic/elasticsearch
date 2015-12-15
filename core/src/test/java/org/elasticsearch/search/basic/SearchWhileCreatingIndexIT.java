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

package org.elasticsearch.search.basic;

import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
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

        // make sure we have enough nodes to guaranty default QUORUM consistency.
        // TODO: add a smarter choice based on actual consistency (when that is randomized)
        int shardsNo = numberOfReplicas + 1;
        int neededNodes = shardsNo <= 2 ? 1 : shardsNo / 2 + 1;
        internalCluster().ensureAtLeastNumDataNodes(randomIntBetween(neededNodes, shardsNo));
        for (int i = 0; i < 20; i++) {
            logger.info("running iteration {}", i);
            if (createIndex) {
                createIndex("test");
            }
            client().prepareIndex("test", "type1", randomAsciiOfLength(5)).setSource("field", "test").execute().actionGet();
            RefreshResponse refreshResponse = client().admin().indices().prepareRefresh("test").execute().actionGet();
            assertThat(refreshResponse.getSuccessfulShards(), greaterThanOrEqualTo(1)); // at least one shard should be successful when refreshing

            // we want to make sure that while recovery happens, and a replica gets recovered, its properly refreshed
            ClusterHealthStatus status = ClusterHealthStatus.RED;
            while (status != ClusterHealthStatus.GREEN) {
                // first, verify that search on the primary search works
                SearchResponse searchResponse = client().prepareSearch("test").setPreference("_primary").setQuery(QueryBuilders.termQuery("field", "test")).execute().actionGet();
                assertHitCount(searchResponse, 1);
                // now, let it go to primary or replica, though in a randomized re-creatable manner
                String preference = randomAsciiOfLength(5);
                Client client = client();
                searchResponse = client.prepareSearch("test").setPreference(preference).setQuery(QueryBuilders.termQuery("field", "test")).execute().actionGet();
                if (searchResponse.getHits().getTotalHits() != 1) {
                    refresh();
                    SearchResponse searchResponseAfterRefresh = client.prepareSearch("test").setPreference(preference).setQuery(QueryBuilders.termQuery("field", "test")).execute().actionGet();
                    logger.info("hits count mismatch on any shard search failed, post explicit refresh hits are {}", searchResponseAfterRefresh.getHits().getTotalHits());
                    ensureGreen();
                    SearchResponse searchResponseAfterGreen = client.prepareSearch("test").setPreference(preference).setQuery(QueryBuilders.termQuery("field", "test")).execute().actionGet();
                    logger.info("hits count mismatch on any shard search failed, post explicit wait for green hits are {}", searchResponseAfterGreen.getHits().getTotalHits());
                    assertHitCount(searchResponse, 1);
                }
                assertHitCount(searchResponse, 1);
                status = client().admin().cluster().prepareHealth("test").get().getStatus();
                internalCluster().ensureAtLeastNumDataNodes(numberOfReplicas + 1);
            }
            cluster().wipeIndices("test");
        }
    }
}