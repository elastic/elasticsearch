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

package org.elasticsearch.index.translog;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.NativeFSLockFactory;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.command.AllocateStalePrimaryAllocationCommand;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MockEngineFactoryPlugin;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.engine.MockEngineSupport;
import org.elasticsearch.test.transport.MockTransportService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.elasticsearch.common.util.CollectionUtils.iterableAsArrayList;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 0)
public class TruncateTranslogIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class, MockEngineFactoryPlugin.class);
    }

    public void testCorruptTranslogTruncation() throws Exception {
        internalCluster().startNodes(2, Settings.EMPTY);

        final String primaryNode = internalCluster().getNodeNames()[0];
        final String replicaNode = internalCluster().getNodeNames()[1];

        final String indexName = "test";
        assertAcked(prepareCreate(indexName).setSettings(Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 1)
            .put("index.refresh_interval", "-1")
            .put(MockEngineSupport.DISABLE_FLUSH_ON_CLOSE.getKey(), true) // never flush - always recover from translog
            .put("index.routing.allocation.exclude._name", replicaNode)
        ));
        ensureYellow();

        assertAcked(client().admin().indices().prepareUpdateSettings(indexName).setSettings(Settings.builder()
            .put("index.routing.allocation.exclude._name", (String)null)
        ));
        ensureGreen();

        // Index some documents
        int numDocsToKeep = randomIntBetween(10, 100);
        logger.info("--> indexing [{}] docs to be kept", numDocsToKeep);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocsToKeep];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex(indexName, "type").setSource("foo", "bar");
        }
        indexRandom(false, false, false, Arrays.asList(builders));
        flush(indexName);
        disableTranslogFlush(indexName);
        // having no extra docs is an interesting case for seq no based recoveries - test it more often
        int numDocsToTruncate = randomBoolean() ? 0 : randomIntBetween(0, 100);
        logger.info("--> indexing [{}] more doc to be truncated", numDocsToTruncate);
        builders = new IndexRequestBuilder[numDocsToTruncate];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex(indexName, "type").setSource("foo", "bar");
        }
        indexRandom(false, false, false, Arrays.asList(builders));
        Set<Path> translogDirs = getTranslogDirs(indexName);

        TruncateTranslogCommand ttc = new TruncateTranslogCommand();
        MockTerminal t = new MockTerminal();
        OptionParser parser = ttc.getParser();

        if (randomBoolean() && numDocsToTruncate > 0) {
            // flush the replica, so it will have more docs than what the primary will have
            Index index = resolveIndex(indexName);
            IndexShard replica = internalCluster().getInstance(IndicesService.class, replicaNode).getShardOrNull(new ShardId(index, 0));
            replica.flush(new FlushRequest());
            logger.info("--> performed extra flushing on replica");
        }

        // shut down the replica node to be tested later
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(replicaNode));

        // Corrupt the translog file(s)
        logger.info("--> corrupting translog");
        corruptRandomTranslogFiles(indexName);

        // Restart the single node
        logger.info("--> restarting node");
        internalCluster().restartRandomDataNode();
        ensureStableCluster(1);

        // all shards should be failed due to a corrupted translog
        final SearchPhaseExecutionException searchPhaseExecutionException = expectThrows(SearchPhaseExecutionException.class,
            () -> client().prepareSearch(indexName).setQuery(matchAllQuery()).get());

        // Good, all shards should be failed because there is only a
        // single shard and its translog is corrupt
        assertThat(searchPhaseExecutionException.getMessage(), equalTo("all shards failed"));

        logger.info("--> closing '{}' index", indexName);
        client().admin().indices().prepareClose(indexName).get();

        // have to shut down primary node - otherwise node lock is present
        final InternalTestCluster.RestartCallback callback =
            new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                // and we can actually truncate the translog
                for (Path translogDir : translogDirs) {
                    final Path idxLocation = translogDir.getParent().resolve(ShardPath.INDEX_FOLDER_NAME);
                    assertBusy(() -> {
                        logger.info("--> checking that lock has been released for {}", idxLocation);
                        try (Directory dir = FSDirectory.open(idxLocation, NativeFSLockFactory.INSTANCE);
                             Lock writeLock = dir.obtainLock(IndexWriter.WRITE_LOCK_NAME)) {
                            // Great, do nothing, we just wanted to obtain the lock
                        } catch (LockObtainFailedException lofe) {
                            logger.info("--> failed acquiring lock for {}", idxLocation);
                            fail("still waiting for lock release at [" + idxLocation + "]");
                        } catch (IOException ioe) {
                            fail("Got an IOException: " + ioe);
                        }
                    });

                    final Settings defaultSettings = internalCluster().getDefaultSettings();
                    final Environment environment = TestEnvironment.newEnvironment(defaultSettings);

                    OptionSet options = parser.parse("-d", translogDir.toAbsolutePath().toString(), "-b");
                    logger.info("--> running truncate translog command for [{}]", translogDir.toAbsolutePath());
                    ttc.execute(t, options, environment);
                    logger.info("--> output:\n{}", t.getOutput());
                }

                return super.onNodeStopped(nodeName);
            }
        };
        internalCluster().restartNode(primaryNode, callback);

        ensureStableCluster(1);
        logger.info("--> opening '{}' index", indexName);
        client().admin().indices().prepareOpen(indexName).get();

        String primaryNodeId = null;
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        final DiscoveryNodes nodes = state.nodes();
        for (ObjectObjectCursor<String, DiscoveryNode> cursor : nodes.getNodes()) {
            final String name = cursor.value.getName();
            if (name.equals(primaryNode)) {
                primaryNodeId = cursor.key;
                break;
            }
        }
        assertThat(primaryNodeId, notNullValue());

        assertThat(t.getOutput(), containsString("allocate_stale_primary"));
        assertThat(t.getOutput(), containsString("\"node\" : \"" + primaryNodeId + "\""));

        // has to fail as there is only _stale_ primary (due to new allocation id)
        expectThrows(SearchPhaseExecutionException.class,
            () -> client().prepareSearch(indexName).setQuery(matchAllQuery()).get());

        client().admin().cluster().prepareReroute()
            .add(new AllocateStalePrimaryAllocationCommand(indexName, 0, primaryNodeId
                , true))
            .get();
        ensureYellow(indexName);

        // Run a search and make sure it succeeds
        assertHitCount(client().prepareSearch(indexName).setQuery(matchAllQuery()).get(), numDocsToKeep);

        logger.info("--> starting the replica node to test recovery");
        internalCluster().startNode();
        ensureGreen(indexName);
        for (String node : internalCluster().nodesInclude(indexName)) {
            SearchRequestBuilder q = client().prepareSearch(indexName).setPreference("_only_nodes:" + node).setQuery(matchAllQuery());
            assertHitCount(q.get(), numDocsToKeep);
        }
        final RecoveryResponse recoveryResponse = client().admin().indices().prepareRecoveries(indexName).setActiveOnly(false).get();
        final RecoveryState replicaRecoveryState = recoveryResponse.shardRecoveryStates().get(indexName).stream()
            .filter(recoveryState -> recoveryState.getPrimary() == false).findFirst().get();
        assertThat(replicaRecoveryState.getIndex().toString(), replicaRecoveryState.getIndex().recoveredFileCount(), greaterThan(0));
        // Ensure that the global checkpoint and local checkpoint are restored from the max seqno of the last commit.
        final SeqNoStats seqNoStats = getSeqNoStats(indexName, 0);
        assertThat(seqNoStats.getGlobalCheckpoint(), equalTo(seqNoStats.getMaxSeqNo()));
        assertThat(seqNoStats.getLocalCheckpoint(), equalTo(seqNoStats.getMaxSeqNo()));
    }

    public void testCorruptTranslogTruncationOfReplica() throws Exception {
        internalCluster().startNodes(2, Settings.EMPTY);

        final String replicaNode = internalCluster().getNodeNames()[1];

        assertAcked(prepareCreate("test").setSettings(Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 1)
            .put("index.refresh_interval", "-1")
            .put(MockEngineSupport.DISABLE_FLUSH_ON_CLOSE.getKey(), true) // never flush - always recover from translog
            .put("index.routing.allocation.exclude._name", replicaNode)
        ));
        ensureYellow();

        assertAcked(client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder()
            .put("index.routing.allocation.exclude._name", (String)null)
        ));
        ensureGreen();

        // Index some documents
        int numDocsToKeep = randomIntBetween(0, 100);
        logger.info("--> indexing [{}] docs to be kept", numDocsToKeep);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocsToKeep];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex("test", "type").setSource("foo", "bar");
        }
        indexRandom(false, false, false, Arrays.asList(builders));
        flush("test");
        disableTranslogFlush("test");
        // having no extra docs is an interesting case for seq no based recoveries - test it more often
        int numDocsToTruncate = randomBoolean() ? 0 : randomIntBetween(0, 100);
        logger.info("--> indexing [{}] more docs to be truncated", numDocsToTruncate);
        builders = new IndexRequestBuilder[numDocsToTruncate];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex("test", "type").setSource("foo", "bar");
        }
        indexRandom(false, false, false, Arrays.asList(builders));
        final int totalDocs = numDocsToKeep + numDocsToTruncate;

        // sample the replica node translog dirs
        final ShardId shardId = new ShardId(resolveIndex("test"), 0);
        Set<Path> translogDirs = getTranslogDirs(replicaNode, shardId);

        // stop the cluster nodes. we don't use full restart so the node start up order will be the same
        // and shard roles will be maintained
        internalCluster().stopRandomDataNode();
        internalCluster().stopRandomDataNode();

        // Corrupt the translog file(s)
        logger.info("--> corrupting translog");
        TestTranslog.corruptTranslogFiles(logger, random(), translogDirs);

        // Restart the single node
        logger.info("--> starting node");
        internalCluster().startNode();

        ensureYellow();

        // Run a search and make sure it succeeds
        assertHitCount(client().prepareSearch("test").setQuery(matchAllQuery()).get(), totalDocs);

        TruncateTranslogCommand ttc = new TruncateTranslogCommand();
        MockTerminal t = new MockTerminal();
        OptionParser parser = ttc.getParser();

        final Environment environment = TestEnvironment.newEnvironment(internalCluster().getDefaultSettings());

        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                for (Path translogDir : translogDirs) {
                    final Path idxLocation = translogDir.getParent().resolve(ShardPath.INDEX_FOLDER_NAME);
                    assertBusy(() -> {
                        logger.info("--> checking that lock has been released for {}", idxLocation);
                        try (Directory dir = FSDirectory.open(idxLocation, NativeFSLockFactory.INSTANCE);
                             Lock writeLock = dir.obtainLock(IndexWriter.WRITE_LOCK_NAME)) {
                            // Great, do nothing, we just wanted to obtain the lock
                        }  catch (LockObtainFailedException lofe) {
                            logger.info("--> failed acquiring lock for {}", idxLocation);
                            fail("still waiting for lock release at [" + idxLocation + "]");
                        } catch (IOException ioe) {
                            fail("Got an IOException: " + ioe);
                        }
                    });

                    OptionSet options = parser.parse("-d", translogDir.toAbsolutePath().toString(), "-b");
                    logger.info("--> running truncate translog command for [{}]", translogDir.toAbsolutePath());
                    ttc.execute(t, options, environment);
                    logger.info("--> output:\n{}", t.getOutput());
                }

                return super.onNodeStopped(nodeName);
            }
        });

        logger.info("--> starting the replica node to test recovery");
        internalCluster().startNode();
        ensureGreen("test");
        for (String node : internalCluster().nodesInclude("test")) {
            assertHitCount(client().prepareSearch("test").setPreference("_only_nodes:" + node).setQuery(matchAllQuery()).get(), totalDocs);
        }

        final RecoveryResponse recoveryResponse = client().admin().indices().prepareRecoveries("test").setActiveOnly(false).get();
        final RecoveryState replicaRecoveryState = recoveryResponse.shardRecoveryStates().get("test").stream()
            .filter(recoveryState -> recoveryState.getPrimary() == false).findFirst().get();
        // the replica translog was disabled so it doesn't know what hte global checkpoint is and thus can't do ops based recovery
        assertThat(replicaRecoveryState.getIndex().toString(), replicaRecoveryState.getIndex().recoveredFileCount(), greaterThan(0));
        // Ensure that the global checkpoint and local checkpoint are restored from the max seqno of the last commit.
        final SeqNoStats seqNoStats = getSeqNoStats("test", 0);
        assertThat(seqNoStats.getGlobalCheckpoint(), equalTo(seqNoStats.getMaxSeqNo()));
        assertThat(seqNoStats.getLocalCheckpoint(), equalTo(seqNoStats.getMaxSeqNo()));
    }

    private Set<Path> getTranslogDirs(String indexName) throws IOException {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        GroupShardsIterator shardIterators = state.getRoutingTable().activePrimaryShardsGrouped(new String[]{indexName}, false);
        List<ShardIterator> iterators = iterableAsArrayList(shardIterators);
        ShardIterator shardIterator = RandomPicks.randomFrom(random(), iterators);
        ShardRouting shardRouting = shardIterator.nextOrNull();
        assertNotNull(shardRouting);
        assertTrue(shardRouting.primary());
        assertTrue(shardRouting.assignedToNode());
        String nodeId = shardRouting.currentNodeId();
        ShardId shardId = shardRouting.shardId();
        return getTranslogDirs(nodeId, shardId);
    }

    private Set<Path> getTranslogDirs(String nodeId, ShardId shardId) {
        final NodesStatsResponse nodeStatses = client().admin().cluster().prepareNodesStats(nodeId).setFs(true).get();
        final Set<Path> translogDirs = new TreeSet<>(); // treeset makes sure iteration order is deterministic
        final NodeStats nodeStats = nodeStatses.getNodes().get(0);
        for (FsInfo.Path fsPath : nodeStats.getFs()) {
            String path = fsPath.getPath();
            final String relativeDataLocationPath =  "indices/"+ shardId.getIndex().getUUID() +"/" + Integer.toString(shardId.getId())
                + "/" + ShardPath.TRANSLOG_FOLDER_NAME;
            Path translogPath = PathUtils.get(path).resolve(relativeDataLocationPath);
            if (Files.isDirectory(translogPath)) {
                translogDirs.add(translogPath);
            }
        }
        return translogDirs;
    }

    private void corruptRandomTranslogFiles(String indexName) throws IOException {
        Set<Path> translogDirs = getTranslogDirs(indexName);
        TestTranslog.corruptTranslogFiles(logger, random(), translogDirs);
    }

    /** Disables translog flushing for the specified index */
    private static void disableTranslogFlush(String index) {
        Settings settings = Settings.builder()
                .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(1, ByteSizeUnit.PB))
                .build();
        client().admin().indices().prepareUpdateSettings(index).setSettings(settings).get();
    }

    private SeqNoStats getSeqNoStats(String index, int shardId) {
        final ShardStats[] shardStats = client().admin().indices()
            .prepareStats(index).get()
            .getIndices().get(index).getShards();
        return shardStats[shardId].getSeqNoStats();
    }
}
