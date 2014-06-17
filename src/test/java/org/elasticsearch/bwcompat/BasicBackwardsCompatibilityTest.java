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
package org.elasticsearch.bwcompat;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.lucene.util.English;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ElasticsearchBackwardsCompatIntegrationTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 */
public class BasicBackwardsCompatibilityTest extends ElasticsearchBackwardsCompatIntegrationTest {

    /**
     * Basic test using Index & Realtime Get with external versioning. This test ensures routing works correctly across versions.
     */
    @Test
    public void testExternalVersion() throws Exception {
        createIndex("test");
        final boolean routing = randomBoolean();
        int numDocs = randomIntBetween(10, 20);
        for (int i = 0; i < numDocs; i++) {
            String id = Integer.toString(i);
            String routingKey = routing ? randomRealisticUnicodeOfLength(10) : null;
            client().prepareIndex("test", "type1", id).setRouting(routingKey).setVersion(1).setVersionType(VersionType.EXTERNAL).setSource("field1", English.intToEnglish(i)).get();
            GetResponse get = client().prepareGet("test", "type1", id).setRouting(routingKey).get();
            assertThat("Document with ID " +id + " should exist but doesn't", get.isExists(), is(true));
            assertThat(get.getVersion(), equalTo(1l));
            client().prepareIndex("test", "type1", id).setRouting(routingKey).setVersion(2).setVersionType(VersionType.EXTERNAL).setSource("field1", English.intToEnglish(i)).get();
            get = client().prepareGet("test", "type1", id).setRouting(routingKey).get();
            assertThat("Document with ID " +id + " should exist but doesn't", get.isExists(), is(true));
            assertThat(get.getVersion(), equalTo(2l));
        }
    }

    /**
     * Basic test using Index & Realtime Get with internal versioning. This test ensures routing works correctly across versions.
     */
    @Test
    public void testInternalVersion() throws Exception {
        createIndex("test");
        final boolean routing = randomBoolean();
        int numDocs = randomIntBetween(10, 20);
        for (int i = 0; i < numDocs; i++) {
            String routingKey = routing ? randomRealisticUnicodeOfLength(10) : null;
            String id = Integer.toString(i);
            assertThat(id, client().prepareIndex("test", "type1", id).setRouting(routingKey).setSource("field1", English.intToEnglish(i)).get().isCreated(), is(true));
            GetResponse get = client().prepareGet("test", "type1", id).setRouting(routingKey).get();
            assertThat("Document with ID " +id + " should exist but doesn't", get.isExists(), is(true));
            assertThat(get.getVersion(), equalTo(1l));
            client().prepareIndex("test", "type1", id).setRouting(routingKey).setSource("field1", English.intToEnglish(i)).execute().actionGet();
            get = client().prepareGet("test", "type1", id).setRouting(routingKey).get();
            assertThat("Document with ID " +id + " should exist but doesn't", get.isExists(), is(true));
            assertThat(get.getVersion(), equalTo(2l));
        }
    }

    /**
     * Very basic bw compat test with a mixed version cluster random indexing and lookup by ID via term query
     */
    @Test
    public void testIndexAndSearch() throws Exception {
        createIndex("test");
        int numDocs = randomIntBetween(10, 20);
        List<IndexRequestBuilder> builder = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            String id = Integer.toString(i);
            builder.add(client().prepareIndex("test", "type1", id).setSource("field1", English.intToEnglish(i), "the_id", id));
        }
        indexRandom(true, builder);
        for (int i = 0; i < numDocs; i++) {
            String id = Integer.toString(i);
            assertHitCount(client().prepareSearch().setQuery(QueryBuilders.termQuery("the_id", id)).get(), 1);
        }
    }

    @Test
    public void testRecoverFromPreviousVersion() throws ExecutionException, InterruptedException {
        assertAcked(prepareCreate("test").setSettings(ImmutableSettings.builder().put("index.routing.allocation.exclude._name", backwardsCluster().newNodePattern()).put(indexSettings())));
        ensureYellow();
        assertAllShardsOnNodes("test", backwardsCluster().backwardsNodePattern());
        int numDocs = randomIntBetween(100, 150);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex("test", "type1", randomRealisticUnicodeOfLength(10) + String.valueOf(i)).setSource("field1", English.intToEnglish(i));
        }
        indexRandom(true, docs);
        CountResponse countResponse = client().prepareCount().get();
        assertHitCount(countResponse, numDocs);
        backwardsCluster().allowOnlyNewNodes("test");
        ensureYellow("test");// move all shards to the new node
        final int numIters = randomIntBetween(10, 20);
        for (int i = 0; i < numIters; i++) {
            countResponse = client().prepareCount().get();
            assertHitCount(countResponse, numDocs);
        }
    }

    /**
     * Test that ensures that we will never recover from a newer to an older version (we are not forward compatible)
     */
    @Test
    public void testNoRecoveryFromNewNodes() throws ExecutionException, InterruptedException {
        assertAcked(prepareCreate("test").setSettings(ImmutableSettings.builder().put("index.routing.allocation.exclude._name", backwardsCluster().backwardsNodePattern()).put(indexSettings())));
        if (backwardsCluster().numNewDataNodes() == 0) {
            backwardsCluster().startNewNode();
        }
        ensureYellow();
        assertAllShardsOnNodes("test", backwardsCluster().newNodePattern());
        if (randomBoolean()) {
            backwardsCluster().allowOnAllNodes("test");
        }
        int numDocs = randomIntBetween(100, 150);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex("test", "type1", randomRealisticUnicodeOfLength(10) + String.valueOf(i)).setSource("field1", English.intToEnglish(i));
        }
        indexRandom(true, docs);
        backwardsCluster().allowOnAllNodes("test");
        while(ensureYellow() != ClusterHealthStatus.GREEN) {
            backwardsCluster().startNewNode();
        }
        assertAllShardsOnNodes("test", backwardsCluster().newNodePattern());
        CountResponse countResponse = client().prepareCount().get();
        assertHitCount(countResponse, numDocs);
        final int numIters = randomIntBetween(10, 20);
        for (int i = 0; i < numIters; i++) {
            countResponse = client().prepareCount().get();
            assertHitCount(countResponse, numDocs);
        }
    }

    public void assertAllShardsOnNodes(String index, String pattern) {
        ClusterState clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();
        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                for (ShardRouting shardRouting : indexShardRoutingTable) {
                    if (shardRouting.currentNodeId() != null &&  index.equals(shardRouting.getIndex())) {
                        String name = clusterState.nodes().get(shardRouting.currentNodeId()).name();
                        assertThat("Allocated on new node: " + name, Regex.simpleMatch(pattern, name), is(true));
                    }
                }
            }
        }
    }

    /**
     * Upgrades a single node to the current version
     */
    @Test
    public void testIndexUpgradeSingleNode() throws Exception {
        assertAcked(prepareCreate("test").setSettings(ImmutableSettings.builder().put("index.routing.allocation.exclude._name", backwardsCluster().newNodePattern()).put(indexSettings())));
        int numDocs = randomIntBetween(100, 150);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex("test", "type1", String.valueOf(i)).setSource("field1", English.intToEnglish(i));
        }

        indexRandom(true, docs);
        assertAllShardsOnNodes("test", backwardsCluster().backwardsNodePattern());
        client().admin().indices().prepareUpdateSettings("test").setSettings(ImmutableSettings.builder().put(EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE, "none")).get();
        backwardsCluster().allowOnAllNodes("test");
        CountResponse countResponse = client().prepareCount().get();
        assertHitCount(countResponse, numDocs);
        backwardsCluster().upgradeOneNode();
        ensureYellow("test");
        if (randomBoolean()) {
            for (int i = 0; i < numDocs; i++) {
                docs[i] = client().prepareIndex("test", "type1", String.valueOf(i)).setSource("field1", English.intToEnglish(i));
            }
            indexRandom(true, docs);
        }
        client().admin().indices().prepareUpdateSettings("test").setSettings(ImmutableSettings.builder().put(EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE, "all")).get();
        final int numIters = randomIntBetween(10, 20);
        for (int i = 0; i < numIters; i++) {
            countResponse = client().prepareCount().get();
            assertHitCount(countResponse, numDocs);
        }
    }

    /**
     * Test that allocates an index on one or more old nodes and then do a rolling upgrade
     * one node after another is shut down and restarted from a newer version and we verify
     * that all documents are still around after each nodes upgrade.
     */
    @Test
    public void testIndexRollingUpgrade() throws Exception {
        String[] indices = new String[randomIntBetween(1,3)];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = "test" + i;
            assertAcked(prepareCreate(indices[i]).setSettings(ImmutableSettings.builder().put("index.routing.allocation.exclude._name", backwardsCluster().newNodePattern()).put(indexSettings())));
        }


        int numDocs = randomIntBetween(100, 150);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        String[] indexForDoc = new String[docs.length];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex(indexForDoc[i] = RandomPicks.randomFrom(getRandom(), indices), "type1", String.valueOf(i)).setSource("field1", English.intToEnglish(i));
        }
        indexRandom(true, docs);
        for (int i = 0; i < indices.length; i++) {
            assertAllShardsOnNodes(indices[i], backwardsCluster().backwardsNodePattern());
        }
        client().admin().indices().prepareUpdateSettings(indices).setSettings(ImmutableSettings.builder().put(EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE, "none")).get();
        backwardsCluster().allowOnAllNodes(indices);
        logClusterState();
        boolean upgraded;
        do {
            logClusterState();
            CountResponse countResponse = client().prepareCount().get();
            assertHitCount(countResponse, numDocs);
            upgraded = backwardsCluster().upgradeOneNode();
            ensureYellow();
            countResponse = client().prepareCount().get();
            assertHitCount(countResponse, numDocs);
            for (int i = 0; i < numDocs; i++) {
                docs[i] = client().prepareIndex(indexForDoc[i], "type1", String.valueOf(i)).setSource("field1", English.intToEnglish(i));
            }
            indexRandom(true, docs);
        } while (upgraded);
        client().admin().indices().prepareUpdateSettings(indices).setSettings(ImmutableSettings.builder().put(EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE, "all")).get();
        CountResponse countResponse  = client().prepareCount().get();
        assertHitCount(countResponse, numDocs);
    }


}
