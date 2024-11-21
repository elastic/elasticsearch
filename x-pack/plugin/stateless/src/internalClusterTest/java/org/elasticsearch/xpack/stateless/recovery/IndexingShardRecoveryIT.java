/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.recovery;

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.cluster.coordination.StatelessClusterConsistencyService;
import co.elastic.elasticsearch.stateless.commits.BatchedCompoundCommit;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.commits.VirtualBatchedCompoundCommit;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.reroute.TransportClusterRerouteAction;
import org.elasticsearch.action.admin.indices.refresh.TransportUnpromotableShardRefreshAction;
import org.elasticsearch.action.admin.indices.refresh.UnpromotableShardRefreshRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.coordination.ApplyCommitRequest;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.RecoveryClusterStateDelayListeners;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TestTransportChannel;
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.xpack.shutdown.ShutdownPlugin;
import org.hamcrest.Matchers;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;

import static co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit.blobNameFromGeneration;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.WAIT_UNTIL;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_INTERVAL_SETTING;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_INTERVAL_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_RETRY_COUNT_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.routing.UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING;
import static org.elasticsearch.discovery.PeerFinder.DISCOVERY_FIND_PEERS_INTERVAL_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class IndexingShardRecoveryIT extends AbstractStatelessIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.concatLists(List.of(InternalSettingsPlugin.class, ShutdownPlugin.class), super.nodePlugins());
    }

    /**
     * Overrides default settings to prevent uncontrolled commit uploads
     */
    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(StatelessCommitService.STATELESS_UPLOAD_MAX_SIZE.getKey(), ByteSizeValue.ofGb(1))
            .put(StatelessCommitService.STATELESS_UPLOAD_VBCC_MAX_AGE.getKey(), TimeValue.timeValueDays(1))
            .put(StatelessCommitService.STATELESS_UPLOAD_MONITOR_INTERVAL.getKey(), TimeValue.timeValueDays(1));
    }

    /**
     * Overrides default settings to prevent uncontrolled flushes
     */
    private String createIndex(int shards, int replicas) {
        var indexName = randomIdentifier();
        createIndex(
            indexName,
            indexSettings(shards, replicas).put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), ByteSizeValue.ofGb(1L))
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
                .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
                .build()
        );
        return indexName;
    }

    public void testEmptyStoreRecovery() throws Exception {
        startMasterAndIndexNode(disableIndexingDiskAndMemoryControllersNodeSettings());
        var indexName = createIndex(randomIntBetween(1, 3), 0);

        var initialTermAndGen = new PrimaryTermAndGeneration(1L, 3L);
        assertBusyCommitsMatchExpectedResults(indexName, expectedResults(initialTermAndGen, 0));

        var generated = generateCommits(indexName);
        assertDocsCount(indexName, generated.docs);

        var expected = expectedResults(initialTermAndGen, generated.commits);
        assertBusyCommitsMatchExpectedResults(indexName, expected);
    }

    public void testExistingStoreRecovery() throws Exception {
        startMasterOnlyNode();
        var indexNode = startIndexNode(disableIndexingDiskAndMemoryControllersNodeSettings());
        var indexName = createIndex(randomIntBetween(1, 3), 0);

        var recoveredBcc = new PrimaryTermAndGeneration(1L, 3L);
        assertBusyCommitsMatchExpectedResults(indexName, expectedResults(recoveredBcc, 0));

        long totalDocs = 0L;
        assertDocsCount(indexName, totalDocs);

        int iters = randomIntBetween(1, 5);
        for (int i = 0; i < iters; i++) {

            long expectedGenerationAfterRecovery;
            if (randomBoolean()) {
                var generated = generateCommits(indexName);
                logger.info("--> iteration {}/{}: {} docs indexed in {} commits", i, iters, generated.docs, generated.commits);
                assertDocsCount(indexName, totalDocs + generated.docs);

                var expected = expectedResults(recoveredBcc, generated.commits);
                assertBusyCommitsMatchExpectedResults(indexName, expected);
                // we don't flush shards when the node closes, so non-uploaded commits in the virtual bcc will be lost and the shard will
                // recover from the latest one in the object store, will replay translog operations and then flush.
                expectedGenerationAfterRecovery = expected.lastUploadedCc.generation() + 1L;
                totalDocs += generated.docs;
            } else {
                expectedGenerationAfterRecovery = recoveredBcc.generation() + 1L;
            }

            if (randomBoolean()) {
                internalCluster().restartNode(indexNode);
            } else {
                internalCluster().stopNode(indexNode);
                indexNode = startIndexNode(disableIndexingDiskAndMemoryControllersNodeSettings());
            }
            ensureGreen(indexName);

            // after recovery, term is incremented twice (+1 when it becomes unassigned and +1 when it is reassigned)
            var expected = new PrimaryTermAndGeneration(recoveredBcc.primaryTerm() + 2L, expectedGenerationAfterRecovery);
            assertBusyCommitsMatchExpectedResults(indexName, expectedResults(expected, 0));
            assertDocsCount(indexName, totalDocs);
            recoveredBcc = expected;
        }
    }

    public void testSnapshotRecovery() throws Exception {
        startMasterOnlyNode();
        startIndexNode(disableIndexingDiskAndMemoryControllersNodeSettings());

        var indexName = createIndex(randomIntBetween(1, 3), 0);
        var lastUploaded = new PrimaryTermAndGeneration(1L, 3L);
        assertBusyCommitsMatchExpectedResults(indexName, expectedResults(lastUploaded, 0));

        int totalCommits = 0;
        long totalDocs = 0L;
        assertDocsCount(indexName, totalDocs);

        int iters = randomIntBetween(0, 5);
        for (int i = 0; i < iters; i++) {
            var generated = generateCommits(indexName);
            logger.info("--> iteration {}/{}: {} docs added in {} commits", i, iters, generated.docs, generated.commits);
            assertDocsCount(indexName, totalDocs + generated.docs);
            totalCommits += generated.commits;
            totalDocs += generated.docs;
        }

        var beforeSnapshot = expectedResults(lastUploaded, totalCommits);
        assertBusyCommitsMatchExpectedResults(indexName, beforeSnapshot);

        createRepository(logger, "snapshots", "fs");
        createSnapshot("snapshots", "snapshot", List.of(indexName), List.of());

        var afterSnapshot = beforeSnapshot;
        if (beforeSnapshot.lastVirtualBcc != null) {
            // shard snapshots trigger a flush before snapshotting, so any virtual BCC/CC has been flushed and becomes the latest uploaded
            afterSnapshot = new ExpectedCommits(beforeSnapshot.lastVirtualBcc, beforeSnapshot.lastVirtualCc, null, null);
        }
        assertBusyCommitsMatchExpectedResults(indexName, afterSnapshot);
        assertDocsCount(indexName, totalDocs);

        logger.info("--> deleting index {}", indexName);
        assertAcked(client().admin().indices().prepareDelete(indexName));

        logger.info("--> restoring snapshot of {}", indexName);
        var restore = clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, "snapshots", "snapshot").setWaitForCompletion(true).get();
        assertThat(restore.getRestoreInfo().successfulShards(), equalTo(getNumShards(indexName).numPrimaries));
        assertThat(restore.getRestoreInfo().failedShards(), equalTo(0));
        ensureGreen(indexName);

        var afterRestore = new PrimaryTermAndGeneration(
            // term is incremented by 1
            afterSnapshot.lastUploadedBcc.primaryTerm() + 1L,
            // Lucene index is committed twice on snapshot recovery: one for bootstrapNewHistory and one for associateIndexWithNewTranslog
            afterSnapshot.lastUploadedCc.generation() + 2L
        );
        assertBusyCommitsMatchExpectedResults(indexName, expectedResults(afterRestore, 0));
        assertDocsCount(indexName, totalDocs);
    }

    public void testPeerRecovery() throws Exception {
        startMasterOnlyNode();
        var indexNode = startIndexNode(disableIndexingDiskAndMemoryControllersNodeSettings());
        var indexName = createIndex(randomIntBetween(1, 3), 0);

        var currentGeneration = new PrimaryTermAndGeneration(1L, 3L);
        assertBusyCommitsMatchExpectedResults(indexName, expectedResults(currentGeneration, 0));

        long totalDocs = 0L;
        assertDocsCount(indexName, totalDocs);

        int iters = randomIntBetween(1, 5);
        for (int i = 0; i < iters; i++) {
            long expectedGenerationAfterRelocation;
            if (randomBoolean()) {
                var generated = generateCommits(indexName);
                logger.info("--> iteration {}/{}: {} docs indexed in {} commits", i, iters, generated.docs, generated.commits);
                assertDocsCount(indexName, totalDocs + generated.docs);

                var expected = expectedResults(currentGeneration, generated.commits);
                assertBusyCommitsMatchExpectedResults(indexName, expected);

                if (expected.lastVirtualBcc != null) {
                    // there is a flush before relocating the shard so last CC of VBCC is uploaded, in addition to
                    // generation increments due to a flush on the target shard after peer-recovery
                    expectedGenerationAfterRelocation = expected.lastVirtualCc.generation() + 1L;
                } else {
                    // generation increments due to a flush on the target shard after peer-recovery
                    expectedGenerationAfterRelocation = expected.lastUploadedCc.generation() + 1L;
                }
                totalDocs += generated.docs;
            } else {
                // generation increments due to a flush on the target shard after peer-recovery
                expectedGenerationAfterRelocation = currentGeneration.generation() + 1L;
            }

            var newIndexNode = startIndexNode(disableIndexingDiskAndMemoryControllersNodeSettings());
            logger.info("--> iteration {}/{}: node {} started", i, iters, newIndexNode);

            var excludedNode = indexNode;
            updateIndexSettings(Settings.builder().put(INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "_name", excludedNode), indexName);
            assertBusy(() -> assertThat(internalCluster().nodesInclude(indexName), not(hasItem(excludedNode))));
            assertNodeHasNoCurrentRecoveries(newIndexNode);
            internalCluster().stopNode(excludedNode);
            indexNode = newIndexNode;
            ensureGreen(indexName);

            var expected = new PrimaryTermAndGeneration(currentGeneration.primaryTerm(), expectedGenerationAfterRelocation);
            assertBusyCommitsMatchExpectedResults(indexName, expectedResults(expected, 0));
            assertDocsCount(indexName, totalDocs);
            currentGeneration = expected;
        }
    }

    public void testRecoverIndexingShardWithStaleCompoundCommit() throws Exception {
        final var masterNode = startMasterOnlyNode();
        final var extraSettings = Settings.builder()
            .put(StatelessClusterConsistencyService.DELAYED_CLUSTER_CONSISTENCY_INTERVAL_SETTING.getKey(), "100ms")
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            .build();
        final var indexNode = startIndexNode(extraSettings);
        ensureStableCluster(2, masterNode);

        final String indexName = randomIdentifier();
        createIndex(
            indexName,
            indexSettings(1, 0)
                // make sure nothing triggers flushes under the hood
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
                .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), ByteSizeValue.ofGb(1L))
                .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
                // one node will be isolated in this test
                .put(INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), TimeValue.ZERO)
                .build()
        );
        ensureGreen(indexName);

        logger.debug("--> index docs then flush multiple times to create multiple commits in the object store");
        final int initialFlushes = randomIntBetween(2, 10);
        for (int i = 0; i < initialFlushes; i++) {
            indexDocs(indexName, 10);
            flush(indexName);
        }

        var index = resolveIndex(indexName);
        var indexShard = findIndexShard(index, 0);
        var indexEngine = (IndexEngine) indexShard.getEngineOrNull();
        assertThat(indexEngine, notNullValue());
        final var primaryTermBeforeFailOver = indexShard.getOperationPrimaryTerm();
        final var generationBeforeFailOver = indexEngine.getLastCommittedSegmentInfos().getGeneration();

        logger.debug("--> start a new indexing node");
        final var newIndexNode = startIndexNode(extraSettings);
        ensureStableCluster(3, masterNode);

        logger.debug("--> index more docs, without flushing");
        indexDocs(indexName, scaledRandomIntBetween(10, 500));

        // set up a cluster state listener to wait for the index node to be removed from the cluster
        var masterClusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        final var nodeRemovedFuture = new PlainActionFuture<Void>();
        final var nodeRemovedClusterStateListener = new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                if (event.nodesRemoved()
                    && event.nodesDelta().removedNodes().stream().anyMatch(discoveryNode -> discoveryNode.getName().equals(indexNode))) {
                    logger.debug("--> index node {} is now removed", indexNode);
                    nodeRemovedFuture.onResponse(null);
                }
            }

            @Override
            public String toString() {
                return "ClusterStateListener for " + indexNode + " node removal";
            }
        };
        masterClusterService.addListener(nodeRemovedClusterStateListener);

        logger.debug("--> disrupting cluster to isolate index node {}", indexNode);
        final NetworkDisruption networkDisruption = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(Set.of(indexNode), Set.of(newIndexNode, masterNode)),
            NetworkDisruption.DISCONNECT
        );
        internalCluster().setDisruptionScheme(networkDisruption);
        networkDisruption.startDisrupting();

        logger.debug("--> waiting for index node {} to be removed from the cluster", indexNode);
        nodeRemovedFuture.actionGet();
        masterClusterService.removeListener(nodeRemovedClusterStateListener);

        logger.debug("--> waiting for index to recover on new index node {}", newIndexNode);
        ClusterHealthRequest healthRequest = new ClusterHealthRequest(TEST_REQUEST_TIMEOUT, indexName).timeout(TEST_REQUEST_TIMEOUT)
            .waitForStatus(ClusterHealthStatus.GREEN)
            .waitForEvents(Priority.LANGUID)
            .waitForNoRelocatingShards(true)
            .waitForNoInitializingShards(true)
            .waitForNodes(Integer.toString(2));
        client(newIndexNode).admin().cluster().health(healthRequest).actionGet();

        // once index node is removed and the new index shard started, flush the stale shard to create a commit with the same generation
        logger.debug("--> now flushing stale index shard {}", indexNode);
        client(indexNode).admin().indices().prepareFlush(indexName).setForce(true).get();

        var staleCommitGeneration = indexEngine.getLastCommittedSegmentInfos().getGeneration();
        logger.debug(
            "--> stale index shard created commit [primary term={}, generation {}]",
            primaryTermBeforeFailOver,
            staleCommitGeneration
        );
        assertThat(getPrimaryTerms(client(masterNode), indexName)[0], greaterThan(primaryTermBeforeFailOver));
        assertThat(staleCommitGeneration, equalTo(generationBeforeFailOver + 1L));

        var newIndexShard = findIndexShard(index, 0, newIndexNode);
        assertThat(newIndexShard, not(sameInstance(indexShard)));
        var primaryTermAfterFailOver = newIndexShard.getOperationPrimaryTerm();
        assertThat(primaryTermAfterFailOver, greaterThan(primaryTermBeforeFailOver));

        // index shards are flushed at the end of the recovery: it creates a commit with a generation equal to the commit generation of the
        // stale index shard when it was explicitly flushed when isolated
        assertThat(newIndexShard.getEngineOrNull(), notNullValue());
        var newIndexEngine = (IndexEngine) newIndexShard.getEngineOrNull();
        final var generation = newIndexEngine.getLastCommittedSegmentInfos().getGeneration();
        assertThat(generation, equalTo(staleCommitGeneration));

        logger.debug("--> and also flush new index shard {} so that it is one generation ahead", indexNode);
        client(newIndexNode).admin().indices().prepareFlush(indexName).setForce(true).get();

        logger.debug("--> stop isolated node {}", indexNode);
        internalCluster().stopNode(indexNode);
        ensureStableCluster(2, masterNode);

        logger.debug("--> stop disrupting cluster");
        networkDisruption.stopDisrupting();

        // list the blobs that exist in the object store and map the staless_commit_N files with their primary term prefixes
        var objectStoreService = getCurrentMasterObjectStoreService();
        var blobContainer = objectStoreService.getBlobContainer(newIndexShard.shardId());
        var blobNamesAndPrimaryTerms = new HashMap<String, Set<Long>>();
        for (var child : blobContainer.children(operationPurpose).entrySet()) {
            var blobNames = child.getValue().listBlobs(operationPurpose).keySet();
            blobNames.forEach(
                blobName -> blobNamesAndPrimaryTerms.computeIfAbsent(blobName, s -> new HashSet<>()).add(Long.parseLong(child.getKey()))
            );
        }
        // There should be at least the number of commits initially made + 1 stale commit / post recovery commit with same generation + 1
        // forced commit on the new indexing shard.
        // Is it possible there are more commits from the initial shard creation that may or may not have been deleted
        assertThat(blobNamesAndPrimaryTerms.size(), Matchers.greaterThanOrEqualTo(initialFlushes + 1 + 1));
        assertThat(
            "All commits uploaded under 1 primary term, except the stale generation",
            blobNamesAndPrimaryTerms.entrySet()
                .stream()
                .filter(commit -> commit.getKey().equals(StatelessCompoundCommit.blobNameFromGeneration(generation)) == false)
                .allMatch(commit -> commit.getValue().size() == 1),
            equalTo(true)
        );
        assertThat(
            "Only 1 commit uploaded under more than 1 primary term",
            blobNamesAndPrimaryTerms.entrySet().stream().filter(commit -> commit.getValue().size() != 1).count(),
            equalTo(1L)
        );
        assertThat(
            "The commit uploaded under more than 1 primary term correspond to the stale commit generation",
            blobNamesAndPrimaryTerms.entrySet().stream().filter(commit -> commit.getValue().size() != 1).map(Map.Entry::getKey).toList(),
            hasItem(equalTo(StatelessCompoundCommit.blobNameFromGeneration(staleCommitGeneration)))
        );
        assertThat(
            "The duplicate commits have been uploaded under the expected primary terms",
            blobNamesAndPrimaryTerms.entrySet()
                .stream()
                .filter(commit -> commit.getKey().equals(StatelessCompoundCommit.blobNameFromGeneration(staleCommitGeneration)))
                .map(Map.Entry::getValue)
                .findFirst()
                .get(),
            containsInAnyOrder(equalTo(primaryTermBeforeFailOver), equalTo(primaryTermAfterFailOver))
        );

        logger.debug("--> now we have duplicate commits with different primary terms, trigger a new recovery");
        startIndexNode(extraSettings);
        ensureStableCluster(3, masterNode);

        internalCluster().stopNode(newIndexNode);

        // before ES-6755 bugfix the shard would never reach the STARTED state here
        ensureGreen(indexName);
    }

    public void testCanCreateAnEmptyStoreWithMissingClusterStateUpdates() {
        startMasterOnlyNode();
        var indexNode = startIndexNode();
        var searchNode = startSearchNode();
        ensureStableCluster(3);

        final long initialClusterStateVersion = clusterService().state().version();

        var indexName = randomIdentifier();

        try (var recoveryClusterStateDelayListeners = new RecoveryClusterStateDelayListeners(initialClusterStateVersion)) {
            MockTransportService indexTransportService = MockTransportService.getInstance(indexNode);
            indexTransportService.addRequestHandlingBehavior(Coordinator.COMMIT_STATE_ACTION_NAME, (handler, request, channel, task) -> {
                assertThat(request, instanceOf(ApplyCommitRequest.class));
                recoveryClusterStateDelayListeners.getClusterStateDelayListener(((ApplyCommitRequest) request).getVersion())
                    .addListener(ActionListener.wrap(ignored -> handler.messageReceived(request, channel, task), channel::sendResponse));
            });
            recoveryClusterStateDelayListeners.addCleanup(indexTransportService::clearInboundRules);

            final var searchNodeClusterService = internalCluster().getInstance(ClusterService.class, searchNode);
            final var indexCreated = new AtomicBoolean();
            final ClusterStateListener clusterStateListener = event -> {
                final var indexNodeProceedListener = recoveryClusterStateDelayListeners.getClusterStateDelayListener(
                    event.state().version()
                );
                final var indexRoutingTable = event.state().routingTable().index(indexName);
                assertNotNull(indexRoutingTable);
                final var indexShardRoutingTable = indexRoutingTable.shard(0);

                if (indexShardRoutingTable.primaryShard().assignedToNode() == false && indexCreated.compareAndSet(false, true)) {
                    // this is the cluster state update which creates the index, so fail the application in order to miss the index
                    // in the cluster state when the shard is recovered from the empty store
                    indexNodeProceedListener.onFailure(new RuntimeException("Unable to process cluster state update"));
                } else {
                    // this is some other cluster state update, so we must let it proceed now
                    indexNodeProceedListener.onResponse(null);
                }
            };
            searchNodeClusterService.addListener(clusterStateListener);
            recoveryClusterStateDelayListeners.addCleanup(() -> searchNodeClusterService.removeListener(clusterStateListener));

            final var indexNodeClusterService = internalCluster().getInstance(ClusterService.class, indexNode);
            // Delay cluster state applications so when the recovery is running the index node doesn't know about the new index yet
            ClusterStateApplier delayingApplier = event -> safeSleep(1000);
            indexNodeClusterService.addLowPriorityApplier(delayingApplier);

            prepareCreate(indexName).setSettings(indexSettings(1, 0)).get();
            ensureGreen(indexName);
            indexNodeClusterService.removeApplier(delayingApplier);
        }
    }

    public void testRecoverIndexingShardWithStaleIndexingShard() throws Exception {
        var nodeSettings = Settings.builder()
            .put(FOLLOWER_CHECK_INTERVAL_SETTING.getKey(), "100ms")
            .put(FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey(), "1")
            .put(DISCOVERY_FIND_PEERS_INTERVAL_SETTING.getKey(), "100ms")
            .put(LEADER_CHECK_INTERVAL_SETTING.getKey(), "100ms")
            .put(LEADER_CHECK_RETRY_COUNT_SETTING.getKey(), "1")
            .put(Coordinator.PUBLISH_TIMEOUT_SETTING.getKey(), "1s")
            .put(TransportSettings.CONNECT_TIMEOUT.getKey(), "5s")
            .build();

        startMasterOnlyNode(nodeSettings);
        String indexNodeA = startIndexNode(nodeSettings);
        String searchNode = startSearchNode(nodeSettings);
        String masterName = internalCluster().getMasterName();

        final String indexName = "index-name";
        createIndex(indexName, indexSettings(1, 1).put("index.unassigned.node_left.delayed_timeout", "0ms").build());
        ensureGreen(indexName);

        final AtomicInteger docIdGenerator = new AtomicInteger();
        final AtomicInteger docsAcknowledged = new AtomicInteger();
        final AtomicInteger docsFailed = new AtomicInteger();

        var bulkRequest = client(indexNodeA).prepareBulk();
        for (int i = 0; i < 10; i++) {
            bulkRequest.add(
                new IndexRequest(indexName).id("doc-" + docIdGenerator.incrementAndGet())
                    .source("field", randomUnicodeOfCodepointLengthBetween(1, 25))
            );
        }

        // Index some operations
        for (BulkItemResponse itemResponse : bulkRequest.get().getItems()) {
            if (itemResponse.isFailed()) {
                docsFailed.incrementAndGet();
            } else {
                docsAcknowledged.incrementAndGet();
            }
        }

        final MockTransportService nodeATransportService = MockTransportService.getInstance(indexNodeA);
        final MockTransportService masterTransportService = MockTransportService.getInstance(masterName);

        String indexNodeB = startIndexNode(nodeSettings);

        ensureStableCluster(4);

        long initialPrimaryTerm = getPrimaryTerms(client(), indexName)[0];

        final PlainActionFuture<Void> removedNode = new PlainActionFuture<>();
        final PlainActionFuture<Void> staleRequestDone = new PlainActionFuture<>();
        try {
            final ClusterService masterClusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
            masterClusterService.addListener(clusterChangedEvent -> {
                if (removedNode.isDone() == false
                    && clusterChangedEvent.nodesDelta().removedNodes().stream().anyMatch(d -> d.getName().equals(indexNodeA))) {
                    removedNode.onResponse(null);
                }
            });
            masterTransportService.addUnresponsiveRule(nodeATransportService);

            removedNode.actionGet();

            logger.info("waiting for [{}] to be removed from cluster", indexNodeA);
            ensureStableCluster(3, masterName);

            assertBusy(() -> assertThat(getPrimaryTerms(client(masterName), indexName)[0], greaterThan(initialPrimaryTerm)));

            ClusterHealthRequest healthRequest = new ClusterHealthRequest(TEST_REQUEST_TIMEOUT, indexName).timeout(TEST_REQUEST_TIMEOUT)
                .waitForStatus(ClusterHealthStatus.GREEN)
                .waitForEvents(Priority.LANGUID)
                .waitForNoRelocatingShards(true)
                .waitForNoInitializingShards(true)
                .waitForNodes(Integer.toString(3));

            client(randomFrom(indexNodeB, searchNode)).admin().cluster().health(healthRequest).actionGet();

            var staleBulkRequest = client(indexNodeA).prepareBulk();
            for (int i = 0; i < 10; i++) {
                staleBulkRequest.add(
                    new IndexRequest(indexName).id("stale-doc-" + docIdGenerator.incrementAndGet())
                        .source("field", randomUnicodeOfCodepointLengthBetween(1, 25))
                );
            }
            staleBulkRequest.execute(new ActionListener<>() {
                @Override
                public void onResponse(BulkResponse bulkItemResponses) {
                    for (BulkItemResponse itemResponse : bulkItemResponses.getItems()) {
                        if (itemResponse.isFailed()) {
                            docsFailed.incrementAndGet();
                        } else {
                            docsAcknowledged.incrementAndGet();
                        }
                    }
                    staleRequestDone.onResponse(null);
                }

                @Override
                public void onFailure(Exception e) {
                    staleRequestDone.onFailure(e);
                }
            });

            // Slight delay to allow the stale node to potentially process requests before healing
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(200));
        } finally {
            masterTransportService.clearAllRules();
        }

        logger.info("--> [{}] documents acknowledged, [{}] documents failed", docsAcknowledged, docsFailed);
        ensureGreen(indexName);

        staleRequestDone.actionGet();

        refresh(indexName);
        final long totalHits = SearchResponseUtils.getTotalHitsValue(prepareSearch(indexName));
        assertThat(totalHits, equalTo((long) docsAcknowledged.get()));
    }

    public void testPostWriteRefreshTimeoutDoesNotMakeOnGoingRecoveryFail() throws Exception {
        var indexNode1 = startMasterAndIndexNode();
        var searchNode = startSearchNode();
        ensureStableCluster(2);

        var indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);
        var requestTimeout = TimeValue.timeValueSeconds(1);

        var indexDocs = new AtomicBoolean(true);
        var indexerThread = new Thread(() -> {
            while (indexDocs.get()) {
                try {
                    var bulkRequest = client().prepareBulk();
                    bulkRequest.setTimeout(requestTimeout);
                    var numDocs = randomIntBetween(50, 100);
                    for (int i = 0; i < numDocs; i++) {
                        bulkRequest.add(new IndexRequest(indexName).source("field", randomUnicodeOfCodepointLengthBetween(1, 25)));
                    }
                    bulkRequest.setRefreshPolicy(randomFrom(IMMEDIATE, WAIT_UNTIL));
                    // The refresh timeouts and that's considered a bulk failure (hence we cannot assert that there are no failures)
                    bulkRequest.get();
                } catch (Exception e) {
                    logger.info("Error", e);
                }
            }
        }, "indexer-thread");
        indexerThread.start();

        var indexNode2 = startMasterAndIndexNode();
        ensureStableCluster(3);

        Queue<CheckedRunnable<Exception>> pendingRefreshRequests = new LinkedBlockingQueue<>();
        var shardRefreshRequestSent = new CountDownLatch(1);
        var delayRefreshResponses = new AtomicBoolean(true);
        MockTransportService.getInstance(searchNode)
            .addRequestHandlingBehavior(TransportUnpromotableShardRefreshAction.NAME + "[u]", (handler, request, channel, task) -> {
                UnpromotableShardRefreshRequest req = (UnpromotableShardRefreshRequest) request;
                if (req.shardId().getIndexName().equals(indexName) && delayRefreshResponses.get()) {
                    shardRefreshRequestSent.countDown();
                    pendingRefreshRequests.add(() -> handler.messageReceived(request, channel, task));
                } else {
                    handler.messageReceived(request, channel, task);
                }
            });

        safeAwait(shardRefreshRequestSent);
        logger.info("--> relocating shard 0 from {} to {}", indexNode1, indexNode2);
        var relocationFuture = client().execute(
            TransportClusterRerouteAction.TYPE,
            new ClusterRerouteRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).setRetryFailed(false)
                .add(new MoveAllocationCommand(indexName, 0, indexNode1, indexNode2))
        );
        // Ensure that ongoing refreshes time-out
        safeSleep(requestTimeout.millis() + 100);

        // Process the refresh requests, even though the search shard should have failed at that point
        CheckedRunnable<Exception> pendingRefresh;
        while ((pendingRefresh = pendingRefreshRequests.poll()) != null) {
            pendingRefresh.run();
        }

        delayRefreshResponses.set(false);
        safeGet(relocationFuture);
        ensureGreen(indexName);

        indexDocs.set(false);
        indexerThread.join(requestTimeout.millis());
    }

    public void testRecoverMultipleIndexingShardsWithCoordinatingRetries() throws Exception {
        startMasterOnlyNode();
        String firstIndexingShard = startIndexNode();
        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).build());

        ensureGreen(indexName);

        MockTransportService.getInstance(firstIndexingShard)
            .addRequestHandlingBehavior(
                TransportShardBulkAction.ACTION_NAME,
                (handler, request, channel, task) -> handler.messageReceived(request, new TestTransportChannel(ActionListener.noop()), task)
            );

        String coordinatingNode = startIndexNode();
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", coordinatingNode), indexName);

        ActionFuture<BulkResponse> bulkRequest = client(coordinatingNode).prepareBulk(indexName)
            .add(new IndexRequest(indexName).source(Map.of("custom", "value")))
            .execute();

        assertBusy(() -> {
            IndicesStatsResponse statsResponse = client(firstIndexingShard).admin().indices().prepareStats(indexName).get();
            SeqNoStats seqNoStats = statsResponse.getIndex(indexName).getShards()[0].getSeqNoStats();
            assertThat(seqNoStats.getMaxSeqNo(), equalTo(0L));
        });
        flush(indexName);

        internalCluster().stopNode(firstIndexingShard);

        String secondIndexingShard = startIndexNode();
        ensureGreen(indexName);

        BulkResponse response = bulkRequest.actionGet();
        assertFalse(response.hasFailures());

        internalCluster().stopNode(secondIndexingShard);

        startIndexNodes(1);
        ensureGreen(indexName);

        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1));
        startSearchNode();
        ensureGreen(indexName);

        assertHitCount(prepareSearch(indexName).setQuery(QueryBuilders.termQuery("custom", "value")), 1);
    }

    private static void assertBusyCommitsMatchExpectedResults(String indexName, ExpectedCommits expected) throws Exception {
        assertBusyCommits(indexName, (shardId, uploaded, virtual) -> {
            assertThat(uploaded, notNullValue());

            assertThat(uploaded.primaryTermAndGeneration(), equalTo(expected.lastUploadedBcc));
            assertThat(uploaded.lastCompoundCommit().primaryTermAndGeneration(), equalTo(expected.lastUploadedCc));
            assertBlobExists(shardId, expected.lastUploadedBcc);

            if (expected.lastVirtualBcc == null) {
                assertThat(virtual, nullValue());
            } else {
                assertThat(virtual, notNullValue());
                assertThat(virtual.primaryTermAndGeneration(), equalTo(expected.lastVirtualBcc));
                assertThat(virtual.getLastPendingCompoundCommit(), notNullValue());
                assertThat(virtual.lastCompoundCommit().primaryTermAndGeneration(), equalTo(expected.lastVirtualCc));
            }
        });
    }

    private static void assertBusyCommits(
        String indexName,
        TriConsumer<ShardId, BatchedCompoundCommit, VirtualBatchedCompoundCommit> consumer
    ) throws Exception {
        assertBusy(
            () -> forAllCommitServices(
                indexName,
                (shardId, commitService) -> consumer.apply(
                    shardId,
                    commitService.getLatestUploadedBcc(shardId),
                    commitService.getCurrentVirtualBcc(shardId)
                )
            )
        );
    }

    private static void forAllCommitServices(String indexName, BiConsumer<ShardId, StatelessCommitService> consumer) {
        var clusterState = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT)
            .setMetadata(true)
            .setNodes(true)
            .setIndices(indexName)
            .get()
            .getState();
        assertThat("Index not found: " + indexName, clusterState.metadata().getProject().hasIndex(indexName), equalTo(true));

        var indexMetadata = clusterState.metadata().getProject().index(indexName);
        int shards = Integer.parseInt(indexMetadata.getSettings().get(SETTING_NUMBER_OF_SHARDS));
        for (int shard = 0; shard < shards; shard++) {
            var indexShard = findIndexShard(indexMetadata.getIndex(), shard);
            var routingEntry = indexShard.routingEntry();
            assertThat("Not active: " + routingEntry, indexShard.routingEntry().active(), equalTo(true));

            var nodeId = indexShard.routingEntry().currentNodeId();
            assertThat("Node does not exist: " + routingEntry, clusterState.nodes().nodeExists(nodeId), equalTo(true));

            var node = clusterState.getNodes().get(indexShard.routingEntry().currentNodeId());
            var commitService = internalCluster().getInstance(StatelessCommitService.class, node.getName());
            assertThat("Commit service does not exist: " + shard, commitService, notNullValue());
            assertThat(commitService.hasPendingBccUploads(indexShard.shardId()), equalTo(false));
            consumer.accept(indexShard.shardId(), commitService);
        }
    }

    private record DocsAndCommits(long docs, int commits) {}

    private DocsAndCommits generateCommits(String indexName) {
        int uploadMaxCommits = getUploadMaxCommits();
        int numCommits = randomIntBetween(0, Math.min(25, uploadMaxCommits * 3));

        long totalDocs = 0L;
        logger.info("--> generating {} commit(s) for index [{}] with {} upload max. commits", numCommits, indexName, uploadMaxCommits);
        for (int i = 0; i < numCommits; i++) {
            int numDocs = randomIntBetween(50, 100);
            indexDocs(indexName, numDocs, bulkRequest -> bulkRequest.setRefreshPolicy(RefreshPolicy.IMMEDIATE));
            totalDocs += numDocs;
        }
        return new DocsAndCommits(totalDocs, numCommits);
    }

    private record ExpectedCommits(
        PrimaryTermAndGeneration lastUploadedBcc,
        PrimaryTermAndGeneration lastUploadedCc,
        PrimaryTermAndGeneration lastVirtualBcc,
        PrimaryTermAndGeneration lastVirtualCc
    ) {}

    private ExpectedCommits expectedResults(PrimaryTermAndGeneration initial, int numCommits) {
        if (numCommits == 0) {
            return new ExpectedCommits(initial, initial, null, null);
        }
        int uploadMaxCommits = getUploadMaxCommits();
        int uploads = numCommits / uploadMaxCommits;
        if (uploads == 0) {
            return new ExpectedCommits(
                initial,
                initial,
                new PrimaryTermAndGeneration(initial.primaryTerm(), initial.generation() + 1L),
                new PrimaryTermAndGeneration(initial.primaryTerm(), initial.generation() + numCommits)
            );
        }
        int numUploaded = uploads * uploadMaxCommits;
        int numPending = numCommits % uploadMaxCommits;
        return new ExpectedCommits(
            // term/gen of latest uploaded BCC
            new PrimaryTermAndGeneration(initial.primaryTerm(), initial.generation() + (numUploaded - uploadMaxCommits + 1L)),
            // term/gen of last CC in latest uploaded BCC
            new PrimaryTermAndGeneration(initial.primaryTerm(), initial.generation() + numUploaded),
            // term/gen of virtual BCC
            numPending > 0 ? new PrimaryTermAndGeneration(initial.primaryTerm(), initial.generation() + numUploaded + 1L) : null,
            // term/gen of last CC in virtual BCC
            numPending > 0 ? new PrimaryTermAndGeneration(initial.primaryTerm(), initial.generation() + numUploaded + numPending) : null
        );
    }

    private static void assertBlobExists(ShardId shardId, PrimaryTermAndGeneration primaryTermAndGeneration) {
        try {
            var objectStoreService = getCurrentMasterObjectStoreService();
            var blobContainer = objectStoreService.getBlobContainer(shardId, primaryTermAndGeneration.primaryTerm());
            var blobName = blobNameFromGeneration(primaryTermAndGeneration.generation());
            assertTrue("Blob not found: " + blobContainer.path() + blobName, blobContainer.blobExists(OperationPurpose.INDICES, blobName));
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private static void assertDocsCount(String indexName, long expectedDocsCount) {
        assertThat(
            client().admin().indices().prepareStats(indexName).setDocs(true).get().getTotal().getDocs().getCount(),
            equalTo(expectedDocsCount)
        );
    }
}
