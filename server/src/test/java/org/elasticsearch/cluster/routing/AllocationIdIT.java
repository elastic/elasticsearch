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

package org.elasticsearch.cluster.routing;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.lucene.store.SimpleFSDirectory;
import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplanation;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationDecision;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.cluster.routing.allocation.command.AllocateStalePrimaryAllocationCommand;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.MockEngineFactoryPlugin;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.RemoveCorruptedShardDataCommandIT;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.engine.MockEngineSupport;
import org.elasticsearch.test.transport.MockTransportService;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isIn;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 0)
public class AllocationIdIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class, MockEngineFactoryPlugin.class, InternalSettingsPlugin.class);
    }

    public void testAllocationIdIsSetAfterRecoveryOnAllocateStalePrimary() throws Exception {
        /*
         * test case to spot the problem if allocation id is adjusted before recovery is done (historyUUID is adjusted):
         *
         * - Master node + 2 data nodes
         * - (1) index some docs
         * - stop primary (node1)
         * - (2) index more docs to a formal replica (node2)
         * - stop node2
         *
         * node1 would not be a new primary due to master node state - it is required to run AllocateStalePrimary
         * - put a corruption marker to node1 to interrupt recovery
         * - start node1 (shard would not start as it is stale)
         * - allocate stale primary - allocation id is adjusted (*) - but it fails due to the presence of corruption marker
         * - stop node1
         * - stop node0 (master node) to forget about recoverySource (it is stored in a routing table)
         * - drop a corruption marker
         * - start node0 (master) and node1
         *  -> node0 becomes a new primary with the same historyUUID if (*) has a real allocation id
         *  -> node0 has a RED index if (*) points to a fake shard -> node requires another AllocateStalePrimary
         * - index same amount of docs to node1 as it was added at (2)
         *
         * - node1 and node2 have the same number of docs ( but with different docs )
         * - bring node2 back
         * -> no recovery take place if a shard allocation id at (*) is persisted => nodes are fully in-sync but have diff docs
         * -> due to fake allocation id at (*) AllocateStalePrimary is forced for the 2nd time and a full recovery takes place
         */
        // initial set up
        final String indexName = "index42";
        String node0 = internalCluster().startMasterOnlyNode();
        String node1 = internalCluster().startNode();
        createIndex(indexName);
        final int numDocs = indexDocs(indexName);
        final IndexSettings indexSettings = getIndexSettings(indexName, node1);
        final String primaryNodeId = getNodeIdByName(node1);
        final Set<String> allocationIds = getAllocationIds(indexName);
        final ShardId shardId = new ShardId(resolveIndex(indexName), 0);
        final Path indexPath = getIndexPath(node1, shardId);
        assertThat(allocationIds, hasSize(1));
        final Set<String> historyUUIDs = historyUUIDs(node1, indexName);
        String node2 = internalCluster().startNode();
        ensureGreen(indexName);
        assertSameDocIdsOnShards();
        // initial set up is done

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(node1));

        // index more docs to node2 (formal replica)
        int numExtraDocs = randomIntBetween(10, 100);
        {
            IndexRequestBuilder[] builders = new IndexRequestBuilder[numExtraDocs];
            for (int i = 0; i < builders.length; i++) {
                builders[i] = client().prepareIndex(indexName, "type").setSource("foo", "bar2");
            }

            indexRandom(true, false, false, Arrays.asList(builders));
            flush(indexName);
            assertHitCount(client(node2).prepareSearch(indexName).setQuery(matchAllQuery()).get(), numDocs + numExtraDocs);
        }

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(node2));

        // create fake corrupted marker
        putFakeCorruptionMarker(indexSettings, shardId, indexPath);

        // thanks to master node1 is out of sync
        node1 = internalCluster().startNode();

        // there is only _stale_ primary
        checkNoValidShardCopy(indexName, shardId);

        // allocate stale primary
        client(node1).admin().cluster().prepareReroute()
            .add(new AllocateStalePrimaryAllocationCommand(indexName, 0, primaryNodeId, true))
            .get();

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(node1));
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(node0));

        try(Store store = new Store(shardId, indexSettings, new SimpleFSDirectory(indexPath), new DummyShardLock(shardId))) {
            store.removeCorruptionMarker();
        }

        node0 = internalCluster().startMasterOnlyNode();
        node1 = internalCluster().startNode();

        // that we can have w/o fake id:
        // ensureYellow(indexName);
        // assertThat(historyUUIDs(node12, indexName), equalTo(historyUUIDs));

        // index has to red: no any shard is allocated (allocation id is a fake id that does not match to anything)
        final ClusterHealthStatus indexHealthStatus = client().admin().cluster()
            .health(Requests.clusterHealthRequest(indexName)).actionGet().getStatus();
        assertThat(indexHealthStatus, is(ClusterHealthStatus.RED));

        // no any valid shard is there; have to invoke AllocateStalePrimary again
        client().admin().cluster().prepareReroute()
            .add(new AllocateStalePrimaryAllocationCommand(indexName, 0, primaryNodeId, true))
            .get();

        ensureYellow(indexName);

        {
            IndexRequestBuilder[] builders = new IndexRequestBuilder[numExtraDocs];
            for (int i = 0; i < builders.length; i++) {
                builders[i] = client().prepareIndex(indexName, "type").setSource("foo", "bar3");
            }

            indexRandom(true, false, false, Arrays.asList(builders));
            flush(indexName);
        }
        assertHitCount(client(node1).prepareSearch(indexName).setQuery(matchAllQuery()).get(), numDocs + numExtraDocs);

        // bring node2 back
        node2 = internalCluster().startNode();
        ensureGreen(indexName);

        assertThat(historyUUIDs(node1, indexName), everyItem(not(isIn(historyUUIDs))));
        assertThat(historyUUIDs(node1, indexName), equalTo(historyUUIDs(node2, indexName)));

        assertHitCount(client(node1).prepareSearch(indexName).setQuery(matchAllQuery()).get(), numDocs + numExtraDocs);
        assertHitCount(client(node2).prepareSearch(indexName).setQuery(matchAllQuery()).get(), numDocs + numExtraDocs);
        assertSameDocIdsOnShards();
    }

    private void createIndex(String indexName) {
        assertAcked(prepareCreate(indexName)
            .setSettings(Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "-1")
                .put(MockEngineSupport.DISABLE_FLUSH_ON_CLOSE.getKey(), true)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), "checksum")));
    }

    private int indexDocs(String indexName) throws InterruptedException, ExecutionException {
        // index some docs in several segments
        int numDocs = 0;
        for (int k = 0, attempts = randomIntBetween(5, 10); k < attempts; k++) {
            final int numExtraDocs = between(10, 100);
            IndexRequestBuilder[] builders = new IndexRequestBuilder[numExtraDocs];
            for (int i = 0; i < builders.length; i++) {
                builders[i] = client().prepareIndex(indexName, "type").setSource("foo", "bar");
            }

            numDocs += numExtraDocs;

            indexRandom(true, false, false, Arrays.asList(builders));
            flush(indexName);
        }

        return numDocs;
    }

    private String getNodeIdByName(String nodeName) {
        String primaryNodeId = null;
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final DiscoveryNodes nodes = state.nodes();
        for (ObjectObjectCursor<String, DiscoveryNode> cursor : nodes.getNodes()) {
            final String name = cursor.value.getName();
            if (name.equals(nodeName)) {
                primaryNodeId = cursor.key;
                break;
            }
        }
        assertThat(primaryNodeId, notNullValue());
        return primaryNodeId;
    }

    private Path getIndexPath(String nodeName, ShardId shardId) {
        final Set<Path> indexDirs = RemoveCorruptedShardDataCommandIT.getDirs(nodeName, shardId, ShardPath.INDEX_FOLDER_NAME);
        assertThat(indexDirs, hasSize(1));
        return indexDirs.iterator().next();
    }

    private Set<String> getAllocationIds(String indexName) {
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final Set<String> allocationIds = state.metaData().index(indexName).inSyncAllocationIds(0);
        return allocationIds;
    }

    private IndexSettings getIndexSettings(String indexName, String nodeName) {
        final IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeName);
        final IndexService indexService = indicesService.indexService(resolveIndex(indexName));
        return indexService.getIndexSettings();
    }

    private Set<String> historyUUIDs(String node, String indexName) {
        final ShardStats[] shards = client(node).admin().indices().prepareStats(indexName).clear().get().getShards();
        assertThat(shards.length, greaterThan(0));
        return Arrays.stream(shards).map(shard -> shard.getCommitStats().getUserData().get(Engine.HISTORY_UUID_KEY))
            .collect(Collectors.toSet());
    }

    private void putFakeCorruptionMarker(IndexSettings indexSettings, ShardId shardId, Path indexPath) throws IOException {
        try(Store store = new Store(shardId, indexSettings, new SimpleFSDirectory(indexPath), new DummyShardLock(shardId))) {
            store.markStoreCorrupted(new IOException("fake ioexception"));
        }
    }

    private void checkNoValidShardCopy(String indexName, ShardId shardId) throws Exception {
        assertBusy(() -> {
            final ClusterAllocationExplanation explanation =
                client().admin().cluster().prepareAllocationExplain()
                    .setIndex(indexName).setShard(shardId.id()).setPrimary(true)
                    .get().getExplanation();

            final ShardAllocationDecision shardAllocationDecision = explanation.getShardAllocationDecision();
            assertThat(shardAllocationDecision.isDecisionTaken(), equalTo(true));
            assertThat(shardAllocationDecision.getAllocateDecision().getAllocationDecision(),
                equalTo(AllocationDecision.NO_VALID_SHARD_COPY));
        });
    }

}
