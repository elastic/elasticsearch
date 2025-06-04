/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.shard;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.NativeFSLockFactory;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationDecision;
import org.elasticsearch.cluster.routing.allocation.command.AllocateStalePrimaryAllocationCommand;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.gateway.GatewayMetaState;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.MockEngineFactoryPlugin;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.translog.TestTranslog;
import org.elasticsearch.index.translog.TranslogCorruptedException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.CorruptionUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.engine.MockEngineSupport;
import org.elasticsearch.test.transport.MockTransportService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplanationUtils.getClusterAllocationExplanation;
import static org.elasticsearch.common.util.CollectionUtils.iterableAsArrayList;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoveCorruptedShardDataCommandIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class, MockEngineFactoryPlugin.class, InternalSettingsPlugin.class);
    }

    public void testCorruptIndex() throws Exception {
        final String node = internalCluster().startNode();

        final String indexName = "index42";
        assertAcked(
            prepareCreate(indexName).setSettings(
                indexSettings(1, 0).put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
                    .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "-1")
                    .put(MockEngineSupport.DISABLE_FLUSH_ON_CLOSE.getKey(), true)
                    .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), "checksum")
            )
        );

        // index some docs in several segments
        int numDocs = 0;
        for (int k = 0, attempts = randomIntBetween(5, 10); k < attempts; k++) {
            final int numExtraDocs = between(10, 100);
            IndexRequestBuilder[] builders = new IndexRequestBuilder[numExtraDocs];
            for (int i = 0; i < builders.length; i++) {
                builders[i] = prepareIndex(indexName).setSource("foo", "bar");
            }

            numDocs += numExtraDocs;

            indexRandom(false, false, false, Arrays.asList(builders));
            flush(indexName);
        }

        logger.info("--> indexed {} docs", numDocs);

        final RemoveCorruptedShardDataCommand command = new RemoveCorruptedShardDataCommand();
        final MockTerminal terminal = MockTerminal.create();
        final OptionParser parser = command.getParser();
        final ProcessInfo processInfo = new ProcessInfo(Map.of(), Map.of(), createTempDir());

        final Settings dataPathSettings = internalCluster().dataPathSettings(node);

        final Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(dataPathSettings).build()
        );
        final OptionSet options = parser.parse("-index", indexName, "-shard-id", "0");

        // Try running it before the node is stopped (and shard is closed)
        try {
            command.execute(terminal, options, environment, processInfo);
            fail("expected the command to fail as node is locked");
        } catch (Exception e) {
            assertThat(
                e.getMessage(),
                allOf(containsString("failed to lock node's directory"), containsString("is Elasticsearch still running?"))
            );
        }

        final Path indexDir = getPathToShardData(indexName, ShardPath.INDEX_FOLDER_NAME);

        internalCluster().restartNode(node, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                // Try running it before the shard is corrupted, it should flip out because there is no corruption file marker
                try {
                    command.execute(terminal, options, environment, processInfo);
                    fail("expected the command to fail as there is no corruption file marker");
                } catch (Exception e) {
                    assertThat(e.getMessage(), startsWith("Shard does not seem to be corrupted at"));
                }

                CorruptionUtils.corruptIndex(random(), indexDir, false);
                return super.onNodeStopped(nodeName);
            }
        });

        // shard should be failed due to a corrupted index
        assertBusy(() -> {
            final var shardAllocationDecision = getClusterAllocationExplanation(client(), indexName, 0, true).getShardAllocationDecision();
            assertThat(shardAllocationDecision.isDecisionTaken(), equalTo(true));
            assertThat(
                shardAllocationDecision.getAllocateDecision().getAllocationDecision(),
                equalTo(AllocationDecision.NO_VALID_SHARD_COPY)
            );
        });

        internalCluster().restartNode(node, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                terminal.addTextInput("y");
                command.execute(terminal, options, environment, processInfo);

                return super.onNodeStopped(nodeName);
            }
        });

        waitNoPendingTasksOnAll();

        String nodeId = null;
        final ClusterState state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        final DiscoveryNodes nodes = state.nodes();
        for (Map.Entry<String, DiscoveryNode> cursor : nodes.getNodes().entrySet()) {
            final String name = cursor.getValue().getName();
            if (name.equals(node)) {
                nodeId = cursor.getKey();
                break;
            }
        }
        assertThat(nodeId, notNullValue());

        logger.info("--> output:\n{}", terminal.getOutput());

        assertThat(terminal.getOutput(), containsString("allocate_stale_primary"));
        assertThat(terminal.getOutput(), containsString("\"node\" : \"" + nodeId + "\""));

        // there is only _stale_ primary (due to new allocation id)
        assertBusy(() -> {
            final var shardAllocationDecision = getClusterAllocationExplanation(client(), indexName, 0, true).getShardAllocationDecision();
            assertThat(shardAllocationDecision.isDecisionTaken(), equalTo(true));
            assertThat(
                shardAllocationDecision.getAllocateDecision().getAllocationDecision(),
                equalTo(AllocationDecision.NO_VALID_SHARD_COPY)
            );
        });

        ClusterRerouteUtils.reroute(client(), new AllocateStalePrimaryAllocationCommand(indexName, 0, nodeId, true));

        assertBusy(() -> {
            final var explanation = getClusterAllocationExplanation(client(), indexName, 0, true);
            assertThat(explanation.getCurrentNode(), notNullValue());
            assertThat(explanation.getShardState(), equalTo(ShardRoutingState.STARTED));
        });

        final Pattern pattern = Pattern.compile("Corrupted Lucene index segments found -\\s+(?<docs>\\d+) documents will be lost.");
        final Matcher matcher = pattern.matcher(terminal.getOutput());
        assertThat(matcher.find(), equalTo(true));
        final int expectedNumDocs = numDocs - Integer.parseInt(matcher.group("docs"));

        ensureGreen(indexName);

        assertHitCount(prepareSearch(indexName).setQuery(matchAllQuery()), expectedNumDocs);
    }

    public void testCorruptTranslogTruncation() throws Exception {
        internalCluster().startNodes(2);

        final String node1 = internalCluster().getNodeNames()[0];
        final String node2 = internalCluster().getNodeNames()[1];

        final String indexName = "test";
        assertAcked(
            prepareCreate(indexName).setSettings(
                indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "-1")
                    .put(MockEngineSupport.DISABLE_FLUSH_ON_CLOSE.getKey(), true) // never flush - always recover from translog
                    .put("index.routing.allocation.exclude._name", node2)
            )
        );
        ensureYellow();

        updateIndexSettings(Settings.builder().putNull("index.routing.allocation.exclude._name"), indexName);
        ensureGreen();

        // Index some documents
        int numDocsToKeep = randomIntBetween(10, 100);
        logger.info("--> indexing [{}] docs to be kept", numDocsToKeep);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocsToKeep];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = prepareIndex(indexName).setSource("foo", "bar");
        }
        indexRandom(false, false, false, Arrays.asList(builders));
        flush(indexName);

        disableTranslogFlush(indexName);
        // having no extra docs is an interesting case for seq no based recoveries - test it more often
        int numDocsToTruncate = randomBoolean() ? 0 : randomIntBetween(0, 100);
        logger.info("--> indexing [{}] more doc to be truncated", numDocsToTruncate);
        builders = new IndexRequestBuilder[numDocsToTruncate];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = prepareIndex(indexName).setSource("foo", "bar");
        }
        indexRandom(false, false, false, Arrays.asList(builders));

        RemoveCorruptedShardDataCommand command = new RemoveCorruptedShardDataCommand();
        MockTerminal terminal = MockTerminal.create();
        OptionParser parser = command.getParser();

        if (randomBoolean() && numDocsToTruncate > 0) {
            // flush the replica, so it will have more docs than what the primary will have
            Index index = resolveIndex(indexName);
            IndexShard replica = internalCluster().getInstance(IndicesService.class, node2).getShardOrNull(new ShardId(index, 0));
            replica.flush(new FlushRequest());
            logger.info("--> performed extra flushing on replica");
        }

        final Settings node1PathSettings = internalCluster().dataPathSettings(node1);
        final Settings node2PathSettings = internalCluster().dataPathSettings(node2);

        // shut down the replica node to be tested later
        internalCluster().stopNode(node2);

        final Path translogDir = getPathToShardData(indexName, ShardPath.TRANSLOG_FOLDER_NAME);
        final Path indexDir = getPathToShardData(indexName, ShardPath.INDEX_FOLDER_NAME);

        // Restart the single node
        logger.info("--> restarting node");
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                logger.info("--> corrupting translog on node {}", nodeName);
                TestTranslog.corruptRandomTranslogFile(logger, random(), translogDir);
                return super.onNodeStopped(nodeName);
            }
        });

        // all shards should be failed due to a corrupted translog
        assertBusy(() -> {
            final UnassignedInfo unassignedInfo = getClusterAllocationExplanation(client(), indexName, 0, true).getUnassignedInfo();
            assertThat(unassignedInfo.reason(), equalTo(UnassignedInfo.Reason.ALLOCATION_FAILED));
            assertThat(ExceptionsHelper.unwrap(unassignedInfo.failure(), TranslogCorruptedException.class), not(nullValue()));
        });

        // have to shut down primary node - otherwise node lock is present
        internalCluster().restartNode(node1, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                assertBusy(() -> {
                    logger.info("--> checking that lock has been released for {}", indexDir);
                    // noinspection EmptyTryBlock since we're just trying to obtain the lock
                    try (
                        Directory dir = FSDirectory.open(indexDir, NativeFSLockFactory.INSTANCE);
                        Lock ignored = dir.obtainLock(IndexWriter.WRITE_LOCK_NAME)
                    ) {} catch (LockObtainFailedException lofe) {
                        logger.info("--> failed acquiring lock for {}", indexDir);
                        throw new AssertionError("still waiting for lock release at [" + indexDir + "]", lofe);
                    } catch (IOException ioe) {
                        throw new AssertionError("unexpected IOException [" + indexDir + "]", ioe);
                    }
                });

                final Environment environment = TestEnvironment.newEnvironment(
                    Settings.builder().put(internalCluster().getDefaultSettings()).put(node1PathSettings).build()
                );

                terminal.addTextInput("y");
                OptionSet options = parser.parse("-d", translogDir.toAbsolutePath().toString());
                final ProcessInfo processInfo = new ProcessInfo(Map.of(), Map.of(), createTempDir());
                logger.info("--> running command for [{}]", translogDir.toAbsolutePath());
                command.execute(terminal, options, environment, processInfo);
                logger.info("--> output:\n{}", terminal.getOutput());

                return super.onNodeStopped(nodeName);
            }
        });

        String primaryNodeId = null;
        final ClusterState state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        final DiscoveryNodes nodes = state.nodes();
        for (Map.Entry<String, DiscoveryNode> cursor : nodes.getNodes().entrySet()) {
            final String name = cursor.getValue().getName();
            if (name.equals(node1)) {
                primaryNodeId = cursor.getKey();
                break;
            }
        }
        assertThat(primaryNodeId, notNullValue());

        assertThat(terminal.getOutput(), containsString("allocate_stale_primary"));
        assertThat(terminal.getOutput(), containsString("\"node\" : \"" + primaryNodeId + "\""));

        // there is only _stale_ primary (due to new allocation id)
        assertBusy(() -> {
            final var shardAllocationDecision = getClusterAllocationExplanation(client(), indexName, 0, true).getShardAllocationDecision();
            assertThat(shardAllocationDecision.isDecisionTaken(), equalTo(true));
            assertThat(
                shardAllocationDecision.getAllocateDecision().getAllocationDecision(),
                equalTo(AllocationDecision.NO_VALID_SHARD_COPY)
            );
        });

        ClusterRerouteUtils.reroute(client(), new AllocateStalePrimaryAllocationCommand(indexName, 0, primaryNodeId, true));

        assertBusy(() -> {
            final var explanation = getClusterAllocationExplanation(client(), indexName, 0, true);
            assertThat(explanation.getCurrentNode(), notNullValue());
            assertThat(explanation.getShardState(), equalTo(ShardRoutingState.STARTED));
        });

        ensureYellow(indexName);

        // Run a search and make sure it succeeds
        assertHitCount(prepareSearch(indexName).setQuery(matchAllQuery()), numDocsToKeep);

        logger.info("--> starting the replica node to test recovery");
        internalCluster().startNode(node2PathSettings);
        ensureGreen(indexName);
        for (String node : internalCluster().nodesInclude(indexName)) {
            SearchRequestBuilder q = prepareSearch(indexName).setPreference("_only_nodes:" + node).setQuery(matchAllQuery());
            assertHitCount(q, numDocsToKeep);
        }
        final RecoveryResponse recoveryResponse = indicesAdmin().prepareRecoveries(indexName).setActiveOnly(false).get();
        final RecoveryState replicaRecoveryState = recoveryResponse.shardRecoveryStates()
            .get(indexName)
            .stream()
            .filter(recoveryState -> recoveryState.getPrimary() == false)
            .findFirst()
            .get();
        assertThat(replicaRecoveryState.getIndex().toString(), replicaRecoveryState.getIndex().recoveredFileCount(), greaterThan(0));
        // Ensure that the global checkpoint and local checkpoint are restored from the max seqno of the last commit.
        final SeqNoStats seqNoStats = getSeqNoStats(indexName, 0);
        assertThat(seqNoStats.getGlobalCheckpoint(), equalTo(seqNoStats.getMaxSeqNo()));
        assertThat(seqNoStats.getLocalCheckpoint(), equalTo(seqNoStats.getMaxSeqNo()));
    }

    public void testCorruptTranslogTruncationOfReplica() throws Exception {
        internalCluster().startMasterOnlyNode();

        final String node1 = internalCluster().startDataOnlyNode();
        final String node2 = internalCluster().startDataOnlyNode();
        logger.info("--> nodes name: {}, {}", node1, node2);

        final String indexName = "test";
        assertAcked(
            prepareCreate(indexName).setSettings(
                indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "-1")
                    .put(MockEngineSupport.DISABLE_FLUSH_ON_CLOSE.getKey(), true) // never flush - always recover from translog
                    .put("index.routing.allocation.exclude._name", node2)
            )
        );
        ensureYellow();

        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", (String) null), indexName);
        ensureGreen();

        // Index some documents
        int numDocsToKeep = randomIntBetween(0, 100);
        logger.info("--> indexing [{}] docs to be kept", numDocsToKeep);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocsToKeep];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = prepareIndex(indexName).setSource("foo", "bar");
        }
        indexRandom(false, false, false, Arrays.asList(builders));
        flush(indexName);
        disableTranslogFlush(indexName);
        // having no extra docs is an interesting case for seq no based recoveries - test it more often
        int numDocsToTruncate = randomBoolean() ? 0 : randomIntBetween(0, 100);
        logger.info("--> indexing [{}] more docs to be truncated", numDocsToTruncate);
        builders = new IndexRequestBuilder[numDocsToTruncate];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = prepareIndex(indexName).setSource("foo", "bar");
        }
        indexRandom(false, false, false, Arrays.asList(builders));
        final int totalDocs = numDocsToKeep + numDocsToTruncate;

        // sample the replica node translog dirs
        final ShardId shardId = new ShardId(resolveIndex(indexName), 0);
        final Path translogDir = getPathToShardData(node2, shardId, ShardPath.TRANSLOG_FOLDER_NAME);

        final Settings node1PathSettings = internalCluster().dataPathSettings(node1);
        final Settings node2PathSettings = internalCluster().dataPathSettings(node2);

        assertBusy(
            () -> internalCluster().getInstances(GatewayMetaState.class).forEach(gw -> assertTrue(gw.allPendingAsyncStatesWritten()))
        );

        // stop data nodes
        internalCluster().stopRandomDataNode();
        internalCluster().stopRandomDataNode();

        // Corrupt the translog file(s) on the replica
        logger.info("--> corrupting translog");
        TestTranslog.corruptRandomTranslogFile(logger, random(), translogDir);

        // Start the node with the non-corrupted data path
        logger.info("--> starting node");
        internalCluster().startNode(node1PathSettings);

        ensureYellow();

        // Run a search and make sure it succeeds
        assertHitCount(prepareSearch(indexName).setQuery(matchAllQuery()), totalDocs);

        // check replica corruption
        final RemoveCorruptedShardDataCommand command = new RemoveCorruptedShardDataCommand();
        final MockTerminal terminal = MockTerminal.create();
        final OptionParser parser = command.getParser();

        final Environment environment = TestEnvironment.newEnvironment(
            Settings.builder().put(internalCluster().getDefaultSettings()).put(node2PathSettings).build()
        );
        terminal.addTextInput("y");
        OptionSet options = parser.parse("-d", translogDir.toAbsolutePath().toString());
        final ProcessInfo processInfo = new ProcessInfo(Map.of(), Map.of(), createTempDir());
        logger.info("--> running command for [{}]", translogDir.toAbsolutePath());
        command.execute(terminal, options, environment, processInfo);
        logger.info("--> output:\n{}", terminal.getOutput());

        logger.info("--> starting the replica node to test recovery");
        internalCluster().startNode(node2PathSettings);
        ensureGreen(indexName);
        for (String node : internalCluster().nodesInclude(indexName)) {
            assertHitCount(prepareSearch(indexName).setPreference("_only_nodes:" + node).setQuery(matchAllQuery()), totalDocs);
        }

        final RecoveryResponse recoveryResponse = indicesAdmin().prepareRecoveries(indexName).setActiveOnly(false).get();
        final RecoveryState replicaRecoveryState = recoveryResponse.shardRecoveryStates()
            .get(indexName)
            .stream()
            .filter(recoveryState -> recoveryState.getPrimary() == false)
            .findFirst()
            .get();
        // the replica translog was disabled so it doesn't know what hte global checkpoint is and thus can't do ops based recovery
        assertThat(replicaRecoveryState.getIndex().toString(), replicaRecoveryState.getIndex().recoveredFileCount(), greaterThan(0));
        // Ensure that the global checkpoint and local checkpoint are restored from the max seqno of the last commit.
        final SeqNoStats seqNoStats = getSeqNoStats(indexName, 0);
        assertThat(seqNoStats.getGlobalCheckpoint(), equalTo(seqNoStats.getMaxSeqNo()));
        assertThat(seqNoStats.getLocalCheckpoint(), equalTo(seqNoStats.getMaxSeqNo()));
    }

    public void testResolvePath() throws Exception {
        final int numOfNodes = randomIntBetween(1, 5);
        final List<String> nodeNames = internalCluster().startNodes(numOfNodes, Settings.EMPTY);

        final String indexName = "test" + randomInt(100);
        assertAcked(prepareCreate(indexName).setSettings(indexSettings(1, numOfNodes - 1)));
        flush(indexName);

        ensureGreen(indexName);

        final Map<String, String> nodeNameToNodeId = new HashMap<>();
        final ClusterState state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        final DiscoveryNodes nodes = state.nodes();
        for (Map.Entry<String, DiscoveryNode> cursor : nodes.getNodes().entrySet()) {
            nodeNameToNodeId.put(cursor.getValue().getName(), cursor.getKey());
        }

        final List<ShardIterator> shardIterators = state.getRoutingTable().activePrimaryShardsGrouped(new String[] { indexName }, false);
        final List<ShardIterator> iterators = iterableAsArrayList(shardIterators);
        final ShardRouting shardRouting = iterators.iterator().next().nextOrNull();
        assertThat(shardRouting, notNullValue());
        final ShardId shardId = shardRouting.shardId();

        final RemoveCorruptedShardDataCommand command = new RemoveCorruptedShardDataCommand();
        final OptionParser parser = command.getParser();

        final Map<String, Path> indexPathByNodeName = new HashMap<>();
        final Map<String, Environment> environmentByNodeName = new HashMap<>();
        for (String nodeName : nodeNames) {
            final String nodeId = nodeNameToNodeId.get(nodeName);
            indexPathByNodeName.put(nodeName, getPathToShardData(nodeId, shardId, ShardPath.INDEX_FOLDER_NAME));

            final Environment environment = TestEnvironment.newEnvironment(
                Settings.builder().put(internalCluster().getDefaultSettings()).put(internalCluster().dataPathSettings(nodeName)).build()
            );
            environmentByNodeName.put(nodeName, environment);

            internalCluster().stopNode(nodeName);
            logger.info(" -- stopped {}", nodeName);
        }

        for (String nodeName : nodeNames) {
            final Path indexPath = indexPathByNodeName.get(nodeName);
            final OptionSet options = parser.parse("--dir", indexPath.toAbsolutePath().toString());
            command.findAndProcessShardPath(
                options,
                environmentByNodeName.get(nodeName),
                environmentByNodeName.get(nodeName).dataDirs(),
                state,
                shardPath -> assertThat(shardPath.resolveIndex(), equalTo(indexPath))
            );
        }
    }

    private Path getPathToShardData(String indexName, String dirSuffix) {
        ClusterState state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        List<ShardIterator> shardIterators = state.getRoutingTable().activePrimaryShardsGrouped(new String[] { indexName }, false);
        List<ShardIterator> iterators = iterableAsArrayList(shardIterators);
        ShardIterator shardIterator = RandomPicks.randomFrom(random(), iterators);
        ShardRouting shardRouting = shardIterator.nextOrNull();
        assertNotNull(shardRouting);
        assertTrue(shardRouting.primary());
        assertTrue(shardRouting.assignedToNode());
        String nodeId = shardRouting.currentNodeId();
        ShardId shardId = shardRouting.shardId();
        return getPathToShardData(nodeId, shardId, dirSuffix);
    }

    public static Path getPathToShardData(String nodeId, ShardId shardId, String shardPathSubdirectory) {
        final NodesStatsResponse nodeStatsResponse = clusterAdmin().prepareNodesStats(nodeId).setFs(true).get();
        final Set<Path> paths = StreamSupport.stream(nodeStatsResponse.getNodes().get(0).getFs().spliterator(), false)
            .map(
                dataPath -> PathUtils.get(dataPath.getPath())
                    .resolve(NodeEnvironment.INDICES_FOLDER)
                    .resolve(shardId.getIndex().getUUID())
                    .resolve(Integer.toString(shardId.getId()))
                    .resolve(shardPathSubdirectory)
            )
            .filter(Files::isDirectory)
            .collect(Collectors.toSet());
        assertThat(paths, hasSize(1));
        return paths.iterator().next();
    }

    /** Disables translog flushing for the specified index */
    private static void disableTranslogFlush(String index) {
        updateIndexSettings(
            Settings.builder()
                .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), ByteSizeValue.of(1, ByteSizeUnit.PB)),
            index
        );
    }

    private SeqNoStats getSeqNoStats(String index, int shardId) {
        final ShardStats[] shardStats = indicesAdmin().prepareStats(index).get().getIndices().get(index).getShards();
        return shardStats[shardId].getSeqNoStats();
    }
}
