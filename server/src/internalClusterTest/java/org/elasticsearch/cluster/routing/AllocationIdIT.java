/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplanation;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.internal.Requests;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.allocation.AllocationDecision;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.cluster.routing.allocation.command.AllocateStalePrimaryAllocationCommand;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
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
import org.elasticsearch.test.transport.MockTransportService;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 0)
public class AllocationIdIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class, MockEngineFactoryPlugin.class, InternalSettingsPlugin.class);
    }

    public void testFailedRecoveryOnAllocateStalePrimaryRequiresAnotherAllocateStalePrimary() throws Exception {
        /*
         * Allocation id is put on start of shard while historyUUID is adjusted after recovery is done.
         *
         * If during execution of AllocateStalePrimary a proper allocation id is stored in allocation id set and recovery is failed
         * shard restart skips the stage where historyUUID is changed.
         *
         * That leads to situation where allocated stale primary and its replica belongs to the same historyUUID and
         * replica will receive operations after local checkpoint while documents before checkpoints could be significant different.
         *
         * Therefore, on AllocateStalePrimary we put some fake allocation id (no real one could be generated like that)
         * and any failure during recovery requires extra AllocateStalePrimary command to be executed.
         */

        // initial set up
        final String indexName = "index42";
        final String master = internalCluster().startMasterOnlyNode();
        String node1 = internalCluster().startNode();
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), "checksum")
                .build()
        );
        final int numDocs = indexDocs(indexName, "foo", "bar");
        final IndexSettings indexSettings = getIndexSettings(indexName, node1);
        final Set<String> allocationIds = getAllocationIds(indexName);
        final ShardId shardId = new ShardId(resolveIndex(indexName), 0);
        final Path indexPath = getIndexPath(node1, shardId);
        assertThat(allocationIds, hasSize(1));
        final String historyUUID = historyUUID(node1, indexName);
        String node2 = internalCluster().startNode();
        ensureGreen(indexName);
        internalCluster().assertSameDocIdsOnShards();
        // initial set up is done

        Settings node1DataPathSettings = internalCluster().dataPathSettings(node1);
        Settings node2DataPathSettings = internalCluster().dataPathSettings(node2);
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(node1));

        // index more docs to node2 that marks node1 as stale
        int numExtraDocs = indexDocs(indexName, "foo", "bar2");
        assertHitCount(client(node2).prepareSearch(indexName).setQuery(matchAllQuery()).get(), numDocs + numExtraDocs);

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(node2));

        // create fake corrupted marker on node1
        putFakeCorruptionMarker(indexSettings, shardId, indexPath);

        // thanks to master node1 is out of sync
        node1 = internalCluster().startNode(node1DataPathSettings);

        // there is only _stale_ primary
        checkNoValidShardCopy(indexName, shardId);

        // allocate stale primary
        client(node1).admin().cluster().prepareReroute().add(new AllocateStalePrimaryAllocationCommand(indexName, 0, node1, true)).get();

        // allocation fails due to corruption marker
        assertBusy(() -> {
            final ClusterState state = client().admin().cluster().prepareState().get().getState();
            final ShardRouting shardRouting = state.routingTable().index(indexName).shard(shardId.id()).primaryShard();
            assertThat(shardRouting.state(), equalTo(ShardRoutingState.UNASSIGNED));
            assertThat(shardRouting.unassignedInfo().getReason(), equalTo(UnassignedInfo.Reason.ALLOCATION_FAILED));
        });

        internalCluster().stopNode(node1);
        try (Store store = new Store(shardId, indexSettings, newFSDirectory(indexPath), new DummyShardLock(shardId))) {
            store.removeCorruptionMarker();
        }
        node1 = internalCluster().startNode(node1DataPathSettings);

        // index is red: no any shard is allocated (allocation id is a fake id that does not match to anything)
        checkHealthStatus(indexName, ClusterHealthStatus.RED);
        checkNoValidShardCopy(indexName, shardId);

        // no any valid shard is there; have to invoke AllocateStalePrimary again
        client().admin().cluster().prepareReroute().add(new AllocateStalePrimaryAllocationCommand(indexName, 0, node1, true)).get();

        ensureYellow(indexName);

        // bring node2 back
        node2 = internalCluster().startNode(node2DataPathSettings);
        ensureGreen(indexName);

        assertThat(historyUUID(node1, indexName), not(equalTo(historyUUID)));
        assertThat(historyUUID(node1, indexName), equalTo(historyUUID(node2, indexName)));

        internalCluster().assertSameDocIdsOnShards();
    }

    public void checkHealthStatus(String indexName, ClusterHealthStatus healthStatus) {
        final ClusterHealthStatus indexHealthStatus = client().admin()
            .cluster()
            .health(Requests.clusterHealthRequest(indexName))
            .actionGet()
            .getStatus();
        assertThat(indexHealthStatus, is(healthStatus));
    }

    private int indexDocs(String indexName, Object... source) throws InterruptedException {
        // index some docs in several segments
        int numDocs = 0;
        for (int k = 0, attempts = randomIntBetween(5, 10); k < attempts; k++) {
            final int numExtraDocs = between(10, 100);
            IndexRequestBuilder[] builders = new IndexRequestBuilder[numExtraDocs];
            for (int i = 0; i < builders.length; i++) {
                builders[i] = client().prepareIndex(indexName).setSource(source);
            }

            indexRandom(true, false, true, Arrays.asList(builders));
            numDocs += numExtraDocs;
        }

        return numDocs;
    }

    private Path getIndexPath(String nodeName, ShardId shardId) {
        return RemoveCorruptedShardDataCommandIT.getPathToShardData(nodeName, shardId, ShardPath.INDEX_FOLDER_NAME);
    }

    private Set<String> getAllocationIds(String indexName) {
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final Set<String> allocationIds = state.metadata().index(indexName).inSyncAllocationIds(0);
        return allocationIds;
    }

    private IndexSettings getIndexSettings(String indexName, String nodeName) {
        final IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeName);
        final IndexService indexService = indicesService.indexService(resolveIndex(indexName));
        return indexService.getIndexSettings();
    }

    private String historyUUID(String node, String indexName) {
        final ShardStats[] shards = client(node).admin().indices().prepareStats(indexName).clear().get().getShards();
        final String nodeId = client(node).admin().cluster().prepareState().get().getState().nodes().resolveNode(node).getId();
        assertThat(shards.length, greaterThan(0));
        final Set<String> historyUUIDs = Arrays.stream(shards)
            .filter(shard -> shard.getShardRouting().currentNodeId().equals(nodeId))
            .map(shard -> shard.getCommitStats().getUserData().get(Engine.HISTORY_UUID_KEY))
            .collect(Collectors.toSet());
        assertThat(historyUUIDs, hasSize(1));
        return historyUUIDs.iterator().next();
    }

    private void putFakeCorruptionMarker(IndexSettings indexSettings, ShardId shardId, Path indexPath) throws IOException {
        try (Store store = new Store(shardId, indexSettings, newFSDirectory(indexPath), new DummyShardLock(shardId))) {
            store.markStoreCorrupted(new IOException("fake ioexception"));
        }
    }

    private void checkNoValidShardCopy(String indexName, ShardId shardId) throws Exception {
        assertBusy(() -> {
            final ClusterAllocationExplanation explanation = client().admin()
                .cluster()
                .prepareAllocationExplain()
                .setIndex(indexName)
                .setShard(shardId.id())
                .setPrimary(true)
                .get()
                .getExplanation();

            final ShardAllocationDecision shardAllocationDecision = explanation.getShardAllocationDecision();
            assertThat(shardAllocationDecision.isDecisionTaken(), equalTo(true));
            assertThat(
                shardAllocationDecision.getAllocateDecision().getAllocationDecision(),
                equalTo(AllocationDecision.NO_VALID_SHARD_COPY)
            );
        });
    }

}
