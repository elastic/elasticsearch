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
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplanation;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationDecision;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.cluster.routing.allocation.command.AllocateStalePrimaryAllocationCommand;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.MockEngineFactoryPlugin;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.RemoveCorruptedShardDataCommand;
import org.elasticsearch.index.shard.RemoveCorruptedShardDataCommandIT;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.CorruptionUtils;
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
        /**
         * test case to spot the problem if allocation id is adjusted before recovery is done (historyUUID is adjusted):
         *
         * - start node1 (primary), index some docs, start node2 (replica) all docs are replicated; same historyUUID
         * - stop node2 (replica)
         * - corrupt index on node1, drop corrupted parts it with a corruption tool but don't change historyUUID
         *   (if we start both node1 and node2 no any recovery would be done due to same historyUUID while node1 has less docs than node2)
         *   after corruption tool it is required AllocateStalePrimary to be called.
         *
         * we'd like to disrupt the recovery (local) process:
         * - put a fake corruption marker on node1 to disrupt the recovery
         * - start node - allocation id is adjusted (*) - recovery failed and historyUUID is not changed
         * - remove a fake corruption marker from node1 and start it again
         * - the interruption on recovery has the effect close to AllocateStalePrimary is ignored if a real allocation
         *   id in (*) is persisted before recovery is done (and historyUUID is changed)
         * - index is RED as shard allocation id does not match persisted at (*) allocation id (_forced_allocation)
         * - AllocateStalePrimary has to be called again
         *   it fails if a shard allocation id at (*) is persisted
         * - index turns to YELLOW
         * - start replica -> after (a full) recovery index turns GREEN and all shards have the same number of docs ( numDocs - corrupted )
         *   no recovery take place if a shard allocation id at (*) is persisted =>
         *   => nodes are fully in-sync but have diff number of docs
         */

        // initial set up
        final String indexName = "index42";
        final String node1 = internalCluster().startNode();
        final int numDocs = indexDocs(indexName);
        final IndexSettings indexSettings = getIndexSettings(indexName);
        final String primaryNodeId = getNodeIdByName(node1);
        final Set<String> allocationIds = getAllocationIds(indexName);
        final ShardId shardId = new ShardId(resolveIndex(indexName), 0);
        final Path indexPath = getIndexPath(node1, shardId);
        assertThat(allocationIds, hasSize(1));
        final Set<String> historyUUIDs = historyUUIDs(node1, indexName);
        final String node2 = internalCluster().startNode();
        ensureGreen(indexName);
        assertSameDocIdsOnShards();
        // initial set up is done

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(node2));

        final int expectedNumDocs = corruptIndexAndGetExpectedNumDocs(node1, indexName, numDocs, shardId, indexPath);

        // start nodes in the same order to pick up same data folders; same as node1
        final String node3 = internalCluster().startNode();

        // there is only _stale_ primary (due to new allocation id)
        checkNoValidShardCopy(indexName, shardId);

        // create fake corrupted marker
        putFakeCorruptionMarker(indexSettings, shardId, indexPath);

        // allocate stale primary
        client().admin().cluster().prepareReroute()
            .add(new AllocateStalePrimaryAllocationCommand(indexName, 0, primaryNodeId, true))
            .get();

        // no valid shard copy as shard failed due to corruption marker
        checkNoValidShardCopy(indexName, shardId);

        // restart node1 and remove fake corruption marker to be able to start shard
        internalCluster().restartNode(node3, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                try(Store store = new Store(shardId, indexSettings, new SimpleFSDirectory(indexPath), new DummyShardLock(shardId))) {
                    store.removeCorruptionMarker();
                }
                return super.onNodeStopped(nodeName);
            }
        });

        // check that allocation id is changed
        assertBusy(() -> {
            final ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
            final MetaData indexMetaData = clusterState.metaData();
            final IndexMetaData index = indexMetaData.index(indexName);
            assertThat(index, notNullValue());
            final Set<String> newAllocationIds = index.inSyncAllocationIds(0);
            assertThat(newAllocationIds, hasSize(1));
            assertThat(newAllocationIds, everyItem(not(isIn(allocationIds))));
        });

        // that we can have in case of put a real (not a fake) allocation id:
        //
        // ensureYellow(indexName);
        // Set<String> newHistoryUUIds = historyUUIDs(indexName);
        // assertThat(newHistoryUUIds, equalTo(historyUUIDs));

        // index has to red: no any shard is allocated (allocation id is a fake id that does not match to anything)
        final ClusterHealthStatus indexHealthStatus = client().admin().cluster()
            .health(Requests.clusterHealthRequest(indexName)).actionGet().getStatus();
        assertThat(indexHealthStatus, is(ClusterHealthStatus.RED));

        // no any valid shard is there; have to invoke AllocateStalePrimary again
        client().admin().cluster().prepareReroute()
            .add(new AllocateStalePrimaryAllocationCommand(indexName, 0, primaryNodeId, true))
            .get();

        ensureYellow(indexName);

        // node4 uses same data folder as node2
        final String node4 = internalCluster().startNode();

        // wait for the replica is fully recovered
        ensureGreen(indexName);

        // check that historyUUID is changed
        assertThat(historyUUIDs(node3, indexName), everyItem(not(isIn(historyUUIDs))));
        assertThat(historyUUIDs(node4, indexName), everyItem(not(isIn(historyUUIDs))));

        // both primary and replica have the same number of docs
        assertHitCount(client(node3).prepareSearch(indexName).setQuery(matchAllQuery()).get(), expectedNumDocs);
        // otherwise - replica has not been recovered and it has more docs than primary
        assertHitCount(client(node4).prepareSearch(indexName).setQuery(matchAllQuery()).get(), expectedNumDocs);
        assertSameDocIdsOnShards();
    }

    private int indexDocs(String indexName) throws InterruptedException, ExecutionException {
        assertAcked(prepareCreate(indexName)
            .setSettings(Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "-1")
                .put(MockEngineSupport.DISABLE_FLUSH_ON_CLOSE.getKey(), true)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), "checksum")));
        // index some docs in several segments
        int numDocs = 0;
        for (int k = 0, attempts = randomIntBetween(5, 10); k < attempts; k++) {
            final int numExtraDocs = between(10, 100);
            IndexRequestBuilder[] builders = new IndexRequestBuilder[numExtraDocs];
            for (int i = 0; i < builders.length; i++) {
                builders[i] = client().prepareIndex(indexName, "type").setSource("foo", "bar");
            }

            numDocs += numExtraDocs;

            indexRandom(false, false, false, Arrays.asList(builders));
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

    private int corruptIndexAndGetExpectedNumDocs(String nodeName, String indexName, int numDocs,
                                                  ShardId shardId, Path indexPath) throws Exception {
        // corrupt index on node restart
        internalCluster().restartNode(nodeName, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String node) throws Exception {
                CorruptionUtils.corruptIndex(random(), indexPath, false);
                return super.onNodeStopped(node);
            }
        });

        // all shards should be failed due to a corrupted index
        assertBusy(() -> {
            final ClusterAllocationExplanation explanation =
                client().admin().cluster().prepareAllocationExplain()
                    .setIndex(indexName).setShard(shardId.id()).setPrimary(true)
                    .get().getExplanation();

            final UnassignedInfo unassignedInfo = explanation.getUnassignedInfo();
            assertThat(unassignedInfo.getReason(), equalTo(UnassignedInfo.Reason.ALLOCATION_FAILED));
        });

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodeName));

        // drop corrupted documents from index, but keep the same historyUUID
        final RemoveCorruptedShardDataCommand command = new RemoveCorruptedShardDataCommand() {
            @Override
            protected void addNewHistoryCommit(Directory indexDirectory, Terminal terminal11, boolean updateLocalCheckpoint) {
                // do not create a new history commit
            }
        };

        final OptionParser parser = command.getParser();
        final Environment environment = TestEnvironment.newEnvironment(internalCluster().getDefaultSettings());
        final OptionSet options = parser.parse("-index", indexName, "-shard-id", Integer.toString(shardId.id()));
        final MockTerminal terminal = new MockTerminal();
        terminal.addTextInput("y");
        command.execute(terminal, options, environment);
        // grab number of corrupted docs from RemoveCorruptedShardDataCommand output
        return RemoveCorruptedShardDataCommandIT.getExpectedNumDocs(numDocs, terminal);
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

    private IndexSettings getIndexSettings(String indexName) {
        final IndicesService indicesService = internalCluster().getInstance(IndicesService.class);
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
