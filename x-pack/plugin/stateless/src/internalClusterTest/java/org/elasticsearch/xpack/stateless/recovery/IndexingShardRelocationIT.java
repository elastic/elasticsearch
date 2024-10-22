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
import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.action.GetVirtualBatchedCompoundCommitChunkRequest;
import co.elastic.elasticsearch.stateless.action.NewCommitNotificationRequest;
import co.elastic.elasticsearch.stateless.action.TransportGetVirtualBatchedCompoundCommitChunkAction;
import co.elastic.elasticsearch.stateless.action.TransportNewCommitNotificationAction;
import co.elastic.elasticsearch.stateless.cache.SharedBlobCacheWarmingService;
import co.elastic.elasticsearch.stateless.cache.StatelessSharedBlobCacheService;
import co.elastic.elasticsearch.stateless.commits.BatchedCompoundCommit;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.commits.VirtualBatchedCompoundCommit;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.lucene.BlobStoreCacheDirectory;
import co.elastic.elasticsearch.stateless.lucene.IndexBlobStoreCacheDirectory;
import co.elastic.elasticsearch.stateless.lucene.IndexDirectory;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreTestUtils;

import org.apache.logging.log4j.Level;
import org.apache.lucene.index.IndexFileNames;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.CountDownActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.coordination.ApplyCommitRequest;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.store.LuceneFilesExtensions;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.indices.recovery.RecoveryClusterStateDelayListeners;
import org.elasticsearch.indices.recovery.StatelessPrimaryRelocationAction;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.test.transport.StubbableTransport;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.NodeNotConnectedException;
import org.elasticsearch.transport.TestTransportChannel;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;

import static co.elastic.elasticsearch.stateless.commits.InternalFilesReplicatedRanges.REPLICATED_CONTENT_MAX_SINGLE_FILE_SIZE;
import static co.elastic.elasticsearch.stateless.objectstore.ObjectStoreTestUtils.getObjectStoreMockRepository;
import static co.elastic.elasticsearch.stateless.recovery.TransportStatelessPrimaryRelocationAction.PRIMARY_CONTEXT_HANDOFF_ACTION_NAME;
import static co.elastic.elasticsearch.stateless.recovery.TransportStatelessPrimaryRelocationAction.START_RELOCATION_ACTION_NAME;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.oneOf;

public class IndexingShardRelocationIT extends AbstractStatelessIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(Stateless.class);
        plugins.add(DisableWarmingPlugin.class);
        plugins.add(MockRepository.Plugin.class);
        plugins.add(InternalSettingsPlugin.class);
        return List.copyOf(plugins);
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK);
    }

    public void testRelocatingIndexShards() throws Exception {
        startMasterOnlyNode();
        final var numShards = randomIntBetween(1, 3);
        final var indexNodes = startIndexNodes(Math.max(2, numShards));

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(numShards, 0).build());
        ensureGreen(indexName);

        final ClusterStateListener verifyGreenListener = event -> {
            // ensure that the master remains unchanged, and the index remains green, throughout the test
            assertTrue(event.localNodeMaster());
            final var indexRoutingTable = event.state().routingTable().index(indexName);
            assertEquals(numShards, indexRoutingTable.size());
            for (int i = 0; i < numShards; i++) {
                final var indexShardRoutingTable = indexRoutingTable.shard(i);
                assertEquals(1, indexShardRoutingTable.size());
                assertThat(
                    indexRoutingTable.prettyPrint(),
                    indexShardRoutingTable.primaryShard().state(),
                    oneOf(ShardRoutingState.STARTED, ShardRoutingState.RELOCATING)
                );
            }
        };

        final var masterNodeClusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        masterNodeClusterService.addListener(verifyGreenListener);

        try {
            final AtomicInteger docIdGenerator = new AtomicInteger();
            final IntConsumer docIndexer = numDocs -> {
                var bulkRequest = client().prepareBulk();
                for (int i = 0; i < numDocs; i++) {
                    bulkRequest.add(
                        new IndexRequest(indexName).id("doc-" + docIdGenerator.incrementAndGet())
                            .source("field", randomUnicodeOfCodepointLengthBetween(1, 25))
                    );
                }
                assertNoFailures(bulkRequest.get(TimeValue.timeValueSeconds(10)));
            };

            docIndexer.accept(between(1, 100));
            if (randomBoolean()) {
                flush(indexName);
            }

            final var initialPrimaryTerms = getPrimaryTerms(client(), indexName);

            final int iters = randomIntBetween(5, 10);
            for (int i = 0; i < iters; i++) {

                final var nodeToRemove = indexNodes.get(i % indexNodes.size());

                final AtomicBoolean running = new AtomicBoolean(true);

                final Thread[] threads = new Thread[scaledRandomIntBetween(1, 3)];
                for (int j = 0; j < threads.length; j++) {
                    threads[j] = new Thread(() -> {
                        while (running.get()) {
                            docIndexer.accept(between(1, 20));
                        }
                    });
                    threads[j].start();
                }

                try {
                    logger.info("--> excluding [{}]", nodeToRemove);
                    updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", nodeToRemove), indexName);
                    assertBusy(() -> assertThat(internalCluster().nodesInclude(indexName), not(hasItem(nodeToRemove))));
                } finally {
                    running.set(false);
                    for (Thread thread : threads) {
                        thread.join();
                    }
                }

                assertArrayEquals(initialPrimaryTerms, getPrimaryTerms(client(), indexName));

                for (String indexNode : indexNodes) {
                    assertNodeHasNoCurrentRecoveries(indexNode);
                }

                if (randomBoolean()) {
                    docIndexer.accept(between(1, 10));
                }

                // verify all docs are present without needing input from a search node
                var bulkRequest = client().prepareBulk();
                for (int docId = 1; docId < docIdGenerator.get(); docId++) {
                    bulkRequest.add(new IndexRequest(indexName).id("doc-" + docId).create(true).source(Map.of()));
                }
                var bulkResponse = bulkRequest.get(TimeValue.timeValueSeconds(10));
                for (BulkItemResponse bulkResponseItem : bulkResponse.getItems()) {
                    assertEquals(RestStatus.CONFLICT, bulkResponseItem.status());
                }
            }
        } finally {
            masterNodeClusterService.removeListener(verifyGreenListener);
        }
    }

    public void testFailedRelocatingIndexShardHasNoCurrentRecoveries() {
        startMasterOnlyNode();
        final var indexNodeA = startIndexNode();

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).build());
        indexDocs(indexName, between(1, 100));
        refresh(indexName);

        final var indexNodeB = startIndexNode();
        ensureStableCluster(3); // with master node

        ObjectStoreService objectStoreService = getObjectStoreService(indexNodeB);
        MockRepository repository = ObjectStoreTestUtils.getObjectStoreMockRepository(objectStoreService);
        repository.setRandomControlIOExceptionRate(1.0);
        repository.setRandomDataFileIOExceptionRate(1.0);

        logger.info("--> excluding [{}]", indexNodeA);
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNodeA), indexName);
        // The shard fails to be relocated to indexNodeB and finally stays green on indexNodeA
        ensureGreen(indexName);
        assertNodeHasNoCurrentRecoveries(indexNodeA);
        assertNodeHasNoCurrentRecoveries(indexNodeB);
    }

    public void testRelocateNonexistentIndexShard() throws Exception {
        startMasterOnlyNode();
        final var numShards = 1;
        final var indexNodes = startIndexNodes(2);

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(numShards, 0).put("index.routing.allocation.require._name", indexNodes.get(0)).build());
        ensureGreen(indexName);

        indexDocs(indexName, between(1, 100));
        refresh(indexName);

        final var transportService = MockTransportService.getInstance(indexNodes.get(0));
        final var delayedRequestFuture = new PlainActionFuture<Runnable>();
        final var delayedRequestFutureOnce = ActionListener.assertOnce(delayedRequestFuture);
        transportService.addRequestHandlingBehavior(
            START_RELOCATION_ACTION_NAME,
            (handler, request, channel, task) -> delayedRequestFutureOnce.onResponse(
                () -> ActionListener.run(new ChannelActionListener<>(channel), l -> handler.messageReceived(request, channel, task))
            )
        );

        updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", indexNodes.get(1)), indexName);

        assertNotNull(delayedRequestFuture.get(10, TimeUnit.SECONDS));
        transportService.clearInboundRules();

        final var masterClusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        final var masterServiceBarrier = new CyclicBarrier(2);
        masterClusterService.submitUnbatchedStateUpdateTask("blocking", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                safeAwait(masterServiceBarrier);
                safeAwait(masterServiceBarrier);
                return currentState;
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        });
        safeAwait(masterServiceBarrier); // wait for master service to be blocked, so the shard cannot be reallocated after failure

        final var index = masterClusterService.state().metadata().index(indexName).getIndex();
        final var indicesService = internalCluster().getInstance(IndicesService.class, indexNodes.get(0));
        final var indexShard = indicesService.indexService(index).getShard(0);
        indexShard.failShard("test", new ElasticsearchException("test"));
        assertBusy(() -> assertNull(indicesService.getShardOrNull(indexShard.shardId())));

        delayedRequestFuture.get().run(); // release relocation request which will fail because the shard is no longer there
        safeAwait(masterServiceBarrier); // release master service to restart allocation process

        ensureGreen(indexName);
    }

    public void testRetryIndexShardRelocation() throws Exception {
        startMasterOnlyNode();
        final var numShards = 1;
        final var indexNodes = startIndexNodes(2);

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(numShards, 0).put("index.routing.allocation.require._name", indexNodes.get(0)).build());
        ensureGreen(indexName);

        indexDocs(indexName, between(1, 100));
        refresh(indexName);

        final var transportService = MockTransportService.getInstance(indexNodes.get(0));
        final var allAttemptsFuture = new PlainActionFuture<Void>();
        final var attemptListener = new CountDownActionListener(
            MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.get(Settings.EMPTY),
            allAttemptsFuture
        );
        transportService.addRequestHandlingBehavior(
            START_RELOCATION_ACTION_NAME,
            (handler, request, channel, task) -> ActionListener.completeWith(attemptListener, () -> {
                channel.sendResponse(new ElasticsearchException("simulated"));
                return null;
            })
        );

        updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", indexNodes.get(1)), indexName);
        allAttemptsFuture.get(10, TimeUnit.SECONDS);

        ensureGreen(indexName);
        assertEquals(Set.of(indexNodes.get(0)), internalCluster().nodesInclude(indexName));

        internalCluster().stopNode(indexNodes.get(0));
        ensureGreen(indexName);
        assertEquals(Set.of(indexNodes.get(1)), internalCluster().nodesInclude(indexName));
    }

    public void testFailureAfterPrimaryContextHandoff() throws Exception {
        startMasterOnlyNode();
        final var numShards = 1;
        final var indexNodes = startIndexNodes(2);

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(numShards, 0).put("index.routing.allocation.require._name", indexNodes.get(0)).build());
        ensureGreen(indexName);

        indexDocs(indexName, between(1, 100));
        refresh(indexName);

        final var transportService = MockTransportService.getInstance(indexNodes.get(0));
        final var allAttemptsFuture = new PlainActionFuture<Void>();
        final var attemptListener = new CountDownActionListener(1, allAttemptsFuture); // to assert that there's only one attempt
        transportService.addRequestHandlingBehavior(
            START_RELOCATION_ACTION_NAME,
            (handler, request, channel, task) -> ActionListener.run(
                new ChannelActionListener<>(channel).<TransportResponse>delegateFailure((l, r) -> {
                    attemptListener.onResponse(null);
                    l.onFailure(new ElasticsearchException("simulated"));
                }),
                l -> handler.messageReceived(request, new TestTransportChannel(l), task)
            )
        );

        updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", indexNodes.get(1)), indexName);
        allAttemptsFuture.get(10, TimeUnit.SECONDS);

        // the failure happens after the primary context handoff, so it causes both copies to fail, and then the primary initializes from
        // scratch on the correct node
        ensureGreen(indexName);
        assertEquals(Set.of(indexNodes.get(1)), internalCluster().nodesInclude(indexName));
    }

    public void testCommitGenerationOnRelocatingShardNeverGoesBackward() throws Exception {
        startMasterOnlyNode();
        var indexNodeA = startIndexNode();
        var indexNodeB = startIndexNode();

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).put("index.routing.allocation.require._name", indexNodeA).build());
        ensureGreen(indexName);

        indexDocs(indexName, between(1, 100));
        flush(indexName);

        IndexShard indexShard;

        indexShard = findIndexShard(indexName);
        var generationBeforeRelocation = indexShard.getEngineOrNull().getLastCommittedSegmentInfos().getGeneration();
        var metadataFilesBeforeRelocation = BlobStoreCacheDirectory.unwrapDirectory(indexShard.store().directory()).listAll();

        var relocationStarted = new CountDownLatch(1);
        var proceedWithRelocation = new CountDownLatch(1);

        var transportServiceSourceNode = MockTransportService.getInstance(indexNodeA);
        transportServiceSourceNode.addRequestHandlingBehavior(START_RELOCATION_ACTION_NAME, (handler, request, channel, task) -> {
            relocationStarted.countDown();
            safeAwait(proceedWithRelocation);
            handler.messageReceived(request, channel, task);
        });

        var mockRepositoryTargetNode = getObjectStoreMockRepository(getObjectStoreService(indexNodeB));
        // delay early cache prewarming to have it running (concurrently) with relocation
        // pre-warming reads the latest BCC blob and that we're blocking that read
        mockRepositoryTargetNode.setBlockOnAnyFiles();

        updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", indexNodeB), indexName);

        // early cache prewarming on target node scheduled or started
        safeAwait(relocationStarted);

        // add more commit on source shard before it gets block for primary context relocation
        var newCommits = randomIntBetween(1, 10);
        for (int i = 0; i < newCommits; i++) {
            indexDocs(indexName, between(1, 100));
            assertNoFailures(indicesAdmin().prepareRefresh(indexName).get());
            if (randomBoolean()) {
                assertNoFailures(indicesAdmin().prepareFlush(indexName).get());
            }
            if (rarely()) {
                assertNoFailures(indicesAdmin().prepareForceMerge().setMaxNumSegments(1).get());
            }
        }

        mockRepositoryTargetNode.unblock();
        proceedWithRelocation.countDown();

        ensureGreen(indexName);
        assertEquals(Set.of(indexNodeB), internalCluster().nodesInclude(indexName));

        indexShard = findIndexShard(indexName);
        var generationAfterRelocation = indexShard.getEngineOrNull().getLastCommittedSegmentInfos().getGeneration();
        var metadataFilesAfterRelocation = BlobStoreCacheDirectory.unwrapDirectory(indexShard.store().directory()).listAll();

        // assert that shard was not bootstrapped with old generation
        assertThat(generationAfterRelocation, greaterThan(generationBeforeRelocation));
        // assert that internal cache metadata files were not replaced by obsolete metadata files
        assertThat(metadataFilesAfterRelocation, not(equalTo(metadataFilesBeforeRelocation)));
    }

    public void testWaitForClusterStateToBeAppliedOnSourceNodeInPrimaryRelocation() throws Exception {
        startMasterOnlyNode();
        final var sourceNode = startIndexNode();
        String indexName = "test-index";
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);
        final List<IndexRequestBuilder> indexRequests = IntStream.range(0, between(10, 500))
            .mapToObj(n -> client().prepareIndex(indexName).setSource("foo", "bar"))
            .toList();
        indexRandom(randomBoolean(), true, true, indexRequests);
        assertThat(indicesAdmin().prepareFlush(indexName).get().getFailedShards(), equalTo(0));

        final var targetNode = startIndexNode();

        final long initialClusterStateVersion = clusterService().state().version();

        try (var recoveryClusterStateDelayListeners = new RecoveryClusterStateDelayListeners(initialClusterStateVersion)) {
            final var sourceNodeTransportService = MockTransportService.getInstance(sourceNode);
            sourceNodeTransportService.addRequestHandlingBehavior(
                Coordinator.COMMIT_STATE_ACTION_NAME,
                (handler, request, channel, task) -> {
                    assertThat(request, instanceOf(ApplyCommitRequest.class));
                    recoveryClusterStateDelayListeners.getClusterStateDelayListener(((ApplyCommitRequest) request).getVersion())
                        .addListener(ActionListener.wrap(ignored -> {
                            handler.messageReceived(request, channel, task);
                        }, e -> fail(e, "unexpected")));
                }
            );
            sourceNodeTransportService.addRequestHandlingBehavior(START_RELOCATION_ACTION_NAME, (handler, request, channel, task) -> {
                assertThat(request, instanceOf(StatelessPrimaryRelocationAction.Request.class));
                assertThat(
                    ((StatelessPrimaryRelocationAction.Request) request).clusterStateVersion(),
                    greaterThan(initialClusterStateVersion)
                );
                handler.messageReceived(
                    request,
                    new TestTransportChannel(
                        new ChannelActionListener<>(channel).delegateResponse((l, e) -> fail(e, "recovery should succeed on first attempt"))
                    ),
                    task
                );
                recoveryClusterStateDelayListeners.onStartRecovery();
            });
            recoveryClusterStateDelayListeners.addCleanup(sourceNodeTransportService::clearInboundRules);

            final var targetClusterService = internalCluster().getInstance(ClusterService.class, targetNode);
            final var startedRelocation = new AtomicBoolean();
            final ClusterStateListener clusterStateListener = event -> {
                final var sourceProceedListener = recoveryClusterStateDelayListeners.getClusterStateDelayListener(event.state().version());
                final var indexRoutingTable = event.state().routingTable().index(indexName);
                assertNotNull(indexRoutingTable);
                final var indexShardRoutingTable = indexRoutingTable.shard(0);
                if (indexShardRoutingTable.primaryShard().relocating() && startedRelocation.compareAndSet(false, true)) {
                    // this is the cluster state update which starts the recovery, so delay the primary node application until recovery
                    // has started
                    recoveryClusterStateDelayListeners.delayUntilRecoveryStart(sourceProceedListener);
                } else {
                    // this is some other cluster state update, so we must let it proceed now
                    sourceProceedListener.onResponse(null);
                }
            };
            targetClusterService.addListener(clusterStateListener);
            recoveryClusterStateDelayListeners.addCleanup(() -> targetClusterService.removeListener(clusterStateListener));

            updateIndexSettings(Settings.builder().put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", sourceNode), indexName);
            ensureGreen(indexName);
        }
    }

    public void testRelocateIndexingShardWithActionFailures() throws Exception {
        startMasterOnlyNode();
        final String indexNodeA = startIndexNode();
        ensureStableCluster(2);
        final String indexName = "test";
        createIndex(indexName, indexSettings(1, 0).build());
        int numDocs = scaledRandomIntBetween(1, 10);
        indexDocs(indexName, numDocs);

        final String indexNodeB = startIndexNode();
        ensureStableCluster(3);

        String actionToBreak = randomBoolean() ? START_RELOCATION_ACTION_NAME : PRIMARY_CONTEXT_HANDOFF_ACTION_NAME;
        final CountDownLatch requestFailed = startBreakingActions(indexNodeA, indexNodeB, actionToBreak);

        logger.info("--> move primary shard from: {} to: {}", indexNodeA, indexNodeB);
        ClusterRerouteUtils.reroute(client(), new MoveAllocationCommand(indexName, 0, indexNodeA, indexNodeB));

        assertTrue(requestFailed.await(30, TimeUnit.SECONDS));
        stopBreakingActions(indexNodeA, indexNodeB);

        ensureGreen();
        assertNodeHasNoCurrentRecoveries(indexNodeB);
        assertThat(findIndexShard(resolveIndex(indexName), 0).docStats().getCount(), equalTo((long) numDocs));
    }

    private CountDownLatch startBreakingActions(String nodeA, String nodeB, String recoveryActionToBlock) {
        logger.info("--> will break requests between node [{}] & node [{}] for actions [{}]", nodeA, nodeB, recoveryActionToBlock);

        final MockTransportService nodeAMockTransportService = MockTransportService.getInstance(nodeA);
        final MockTransportService nodeBMockTransportService = MockTransportService.getInstance(nodeB);
        final CountDownLatch requestFailed = new CountDownLatch(1);

        if (randomBoolean()) {
            final StubbableTransport.SendRequestBehavior sendRequestBehavior = (connection, requestId, action, request, options) -> {
                if (recoveryActionToBlock.equals(action)) {
                    requestFailed.countDown();
                    logger.info("--> preventing {} request by throwing ConnectTransportException", action);
                    throw new ConnectTransportException(connection.getNode(), "DISCONNECT: prevented " + action + " request");
                }
                connection.sendRequest(requestId, action, request, options);
            };
            // Fail on the sending side
            nodeAMockTransportService.addSendBehavior(nodeBMockTransportService, sendRequestBehavior);
            nodeBMockTransportService.addSendBehavior(nodeAMockTransportService, sendRequestBehavior);
        } else {
            // Fail on the receiving side.
            nodeAMockTransportService.addRequestHandlingBehavior(recoveryActionToBlock, (handler, request, channel, task) -> {
                logger.info("--> preventing {} response by closing response channel", recoveryActionToBlock);
                requestFailed.countDown();
                nodeBMockTransportService.disconnectFromNode(nodeAMockTransportService.getLocalNode());
                handler.messageReceived(request, channel, task);
            });
            nodeBMockTransportService.addRequestHandlingBehavior(recoveryActionToBlock, (handler, request, channel, task) -> {
                logger.info("--> preventing {} response by closing response channel", recoveryActionToBlock);
                requestFailed.countDown();
                nodeAMockTransportService.disconnectFromNode(nodeBMockTransportService.getLocalNode());
                handler.messageReceived(request, channel, task);
            });
        }

        return requestFailed;
    }

    private void stopBreakingActions(String... nodes) {
        for (String node : nodes) {
            MockTransportService.getInstance(node).clearAllRules();
        }
        logger.info("--> stopped breaking requests on nodes [{}]", Strings.collectionToCommaDelimitedString(Arrays.stream(nodes).toList()));
    }

    public void testOngoingIndexShardRelocationAndMasterFailOver() throws Exception {
        startMasterOnlyNode();  // first master eligible node
        String indexName = "test";
        startMasterOnlyNode(); // second master eligible node
        final String indexNodeA = startIndexNode();
        ensureStableCluster(3);
        createIndex(indexName, indexSettings(1, 0).build());
        int numDocs = scaledRandomIntBetween(1, 10);
        indexDocs(indexName, numDocs);
        final String indexNodeB = startIndexNode();
        ensureStableCluster(4);
        final boolean blockSourceNode = randomBoolean(); // else block target node

        final String nodeToBlock = blockSourceNode ? indexNodeA : indexNodeB;
        final MockTransportService transport = MockTransportService.getInstance(nodeToBlock);
        final SubscribableListener<Void> blockedListeners = new SubscribableListener<>();
        CountDownLatch relocationStartReadyBlocked = new CountDownLatch(1);
        transport.addSendBehavior((connection, requestId, action, request, options) -> {
            final String actionToBlock = blockSourceNode ? PRIMARY_CONTEXT_HANDOFF_ACTION_NAME : START_RELOCATION_ACTION_NAME;
            if (actionToBlock.equals(action)) {
                logger.info("--> Blocking the action [{}]", action);
                blockedListeners.addListener(new ActionListener<>() {
                    @Override
                    public void onResponse(Void unused) {
                        try {
                            logger.info("--> unblocking request [{}][{}][{}]", requestId, action, request);
                            connection.sendRequest(requestId, action, request, options);
                        } catch (NodeNotConnectedException e) {
                            logger.info("Ignoring network connectivity exception", e);
                        } catch (Exception e) {
                            throw new AssertionError("unexpected", e);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        throw new AssertionError("unexpected", e);
                    }
                });
                relocationStartReadyBlocked.countDown();
            } else {
                connection.sendRequest(requestId, action, request, options);
            }
        });
        try {
            logger.info("--> move primary shard from: {} to: {}", indexNodeA, indexNodeB);
            ClusterRerouteUtils.reroute(client(), new MoveAllocationCommand(indexName, 0, indexNodeA, indexNodeB));

            safeAwait(relocationStartReadyBlocked);
            restartMasterNodeGracefully();
        } finally {
            logger.info("--> Unblocking actions");
            blockedListeners.onResponse(null);
        }

        // Assert number of documents
        startSearchNode();
        setReplicaCount(1, indexName);
        assertFalse(clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT, indexName).setWaitForActiveShards(2).get().isTimedOut());
        ensureGreen(indexName);
        assertHitCount(prepareSearch(indexName), numDocs);
    }

    @TestLogging(reason = "testing WARN logging", value = "org.elasticsearch.indices.cluster.IndicesClusterStateService:WARN")
    public void testPrimaryRelocationCancelledLogging() {
        startMasterOnlyNode();
        final var indexNodeA = startIndexNode();
        startSearchNode();
        final var indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).put(MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.getKey(), 0).build());
        ensureGreen(indexName);
        indexDocs(indexName, randomIntBetween(10, 50));

        final var indexNodeB = startIndexNode();
        final var indexNodeATransportService = MockTransportService.getInstance(indexNodeA);

        final var countDownLatch = new CountDownLatch(1);

        indexNodeATransportService.addRequestHandlingBehavior(
            START_RELOCATION_ACTION_NAME,
            (handler, request, channel, task) -> internalCluster().getInstance(ShardStateAction.class, indexNodeB)
                .localShardFailed(
                    internalCluster().getInstance(IndicesService.class, indexNodeB)
                        .indexService(asInstanceOf(StatelessPrimaryRelocationAction.Request.class, request).shardId().getIndex())
                        .getShard(0)
                        .routingEntry(),
                    "simulated message",
                    new ElasticsearchException("simulated exception"),
                    new ChannelActionListener<>(channel).delegateFailureAndWrap(
                        (l, ignored) -> internalCluster().getInstance(ThreadPool.class, indexNodeB)
                            .generic()
                            .execute(ActionRunnable.wrap(l, l2 -> handler.messageReceived(request, new TestTransportChannel(l2) {
                                @Override
                                public void sendResponse(Exception exception) {
                                    assertThat(exception, instanceOf(IllegalIndexShardStateException.class));
                                    super.sendResponse(exception);
                                    countDownLatch.countDown();
                                }
                            }, task)))
                    )
                )
        );

        try (var mockLog = MockLog.capture(IndicesClusterStateService.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation("warnings", IndicesClusterStateService.class.getCanonicalName(), Level.WARN, "*")
            );

            assertAcked(
                admin().indices()
                    .prepareUpdateSettings(indexName)
                    .setSettings(Settings.builder().put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", indexNodeA))
            );

            safeAwait(countDownLatch);
            ensureGreen(indexName);
            mockLog.assertAllExpectationsMatched();
        } finally {
            indexNodeATransportService.clearAllRules();
        }
    }

    @TestLogging(reason = "testing WARN logging", value = "org.elasticsearch.indices.cluster.IndicesClusterStateService:WARN")
    public void testPrimaryRelocationWhileLocallyFailedLogging() {
        startMasterOnlyNode();
        final var indexNodeA = startIndexNode();
        startSearchNode();
        final var indexName = randomIdentifier();
        createIndex(indexName, 1, 0);
        ensureGreen(indexName);
        indexDocs(indexName, randomIntBetween(10, 50));

        startIndexNode();
        final var indexNodeATransportService = MockTransportService.getInstance(indexNodeA);

        final var countDownLatch = new CountDownLatch(1);

        indexNodeATransportService.addRequestHandlingBehavior(START_RELOCATION_ACTION_NAME, (handler, request, channel, task) -> {
            for (final var indexService : internalCluster().getInstance(IndicesService.class, indexNodeA)) {
                if (indexService.index().getName().equals(indexName)) {
                    for (final var indexShard : indexService) {
                        indexShard.failShard("simulated", null);
                    }
                }
            }
            handler.messageReceived(
                request,
                new TestTransportChannel(ActionListener.runAfter(new ChannelActionListener<>(channel), countDownLatch::countDown)),
                task
            );
        });

        try (var mockLog = MockLog.capture(IndicesClusterStateService.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "warnings",
                    IndicesClusterStateService.class.getCanonicalName(),
                    Level.WARN,
                    "marking and sending shard failed due to [failed recovery]"
                )
            );

            assertAcked(
                admin().indices()
                    .prepareUpdateSettings(indexName)
                    .setSettings(Settings.builder().put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", indexNodeA))
            );

            safeAwait(countDownLatch);

            assertAcked(
                admin().indices()
                    .prepareUpdateSettings(indexName)
                    .setSettings(Settings.builder().putNull(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name"))
            );

            ensureGreen(indexName);
            mockLog.assertAllExpectationsMatched();
        } finally {
            indexNodeATransportService.clearAllRules();
        }
    }

    // test for ES-8431
    public void testRelocationIsNotBlockedByRefreshes() throws Exception {
        var maxNonUploadedCommits = randomIntBetween(4, 5);
        var nodeSettings = Settings.builder()
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), maxNonUploadedCommits)
            .build();
        startMasterOnlyNode(nodeSettings);
        startIndexNode(nodeSettings);
        startSearchNode(nodeSettings);
        ensureStableCluster(3);
        final String[] nodeNames = internalCluster().getNodeNames();

        var indexName = randomIdentifier();
        createIndex(indexName, 1, 1);
        ensureGreen(indexName);

        var running = new AtomicBoolean(true);

        CountDownLatch indexingStarted = new CountDownLatch(1);
        CountDownLatch searchingStarted = new CountDownLatch(1);

        var indexingThread = new Thread(() -> {
            while (running.get()) {
                try {
                    // the refresh is important to provoke the original deadlock issue.
                    indexDocsAndRefresh(client(randomFrom(nodeNames)), indexName, randomIntBetween(10, 50));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                indexingStarted.countDown();
            }
        }, "indexing-thread");

        var searchingThread = new Thread(() -> {
            while (running.get()) {
                assertResponse(client(randomFrom(nodeNames)).prepareSearch(indexName).setQuery(matchAllQuery()), response -> {});
                searchingStarted.countDown();
            }
        }, "search-thread");

        indexingThread.start();
        searchingThread.start();

        try {
            // not sure this is necessary, but original test had a sleep and this ensures we wait 10s or until we have activity.
            indexingStarted.await(10, TimeUnit.SECONDS);
            safeAwait(searchingStarted);

            var newIndexNode = startIndexNode(nodeSettings);

            logger.info("--> relocating index shard into {}", newIndexNode);
            updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", newIndexNode), indexName);

            // also waits for no relocating shards.
            ensureGreen(indexName);
        } finally {
            running.set(false);

            indexingThread.join(30000);
            searchingThread.join(10000);
        }
        assertThat(indexingThread.isAlive(), is(false));
        assertThat(searchingThread.isAlive(), is(false));
    }

    public void testPreferredNodeIdsAreUsedDuringRelocation() throws Exception {
        startMasterOnlyNode();

        int maxNonUploadedCommits = randomIntBetween(1, 4);
        var nodeSettings = Settings.builder()
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), maxNonUploadedCommits)
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            .build();

        final var indexNodeSource = startIndexNode(nodeSettings);
        final var searchNode = startSearchNode(nodeSettings);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 1)
                // make sure nothing triggers flushes
                .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), ByteSizeValue.ofGb(1L))
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
                .put(MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.getKey(), 0)
                .build()
        );
        ensureGreen(indexName);

        int nbUploadedBatchedCommits = between(1, 3);
        for (int i = 0; i < nbUploadedBatchedCommits; i++) {
            for (int j = 0; j < maxNonUploadedCommits; j++) {
                indexDocs(indexName, scaledRandomIntBetween(100, 1_000));
                flush(indexName);
            }
        }

        // block the start of the relocation
        final var pauseRelocation = new CountDownLatch(1);
        final var resumeRelocation = new CountDownLatch(1);
        MockTransportService.getInstance(indexNodeSource)
            .addRequestHandlingBehavior(START_RELOCATION_ACTION_NAME, (handler, request, channel, task) -> {
                pauseRelocation.countDown();
                logger.info("--> relocation is paused");
                safeAwait(resumeRelocation);
                logger.info("--> relocation is resumed");
                handler.messageReceived(request, channel, task);
            });

        var index = resolveIndex(indexName);
        var indexShardSource = findIndexShard(index, 0, indexNodeSource);
        final var primaryTerm = indexShardSource.getOperationPrimaryTerm();

        // start another indexing node
        var indexNodeTarget = startIndexNode(nodeSettings);

        // last generation on source
        final var generation = indexShardSource.getEngineOrNull().getLastCommittedSegmentInfos().getGeneration();
        // expected generation on source when refreshing the index (before relocation completes)
        final var beforeGeneration = generation + 1L;
        // expected generation for flush on target (after relocation completes)
        final var afterGeneration = beforeGeneration + 1L;

        logger.info("--> move index shard from: {} to: {}", indexNodeSource, indexNodeTarget);
        ClusterRerouteUtils.reroute(client(), new MoveAllocationCommand(indexName, 0, indexNodeSource, indexNodeTarget));

        logger.info("--> wait for relocation to start on source");
        safeAwait(pauseRelocation);

        logger.info("--> add more docs so that the refresh produces a new commit");
        indexDocs(indexName, scaledRandomIntBetween(100, 1_000));

        final Queue<CheckedRunnable<Exception>> delayedActions = ConcurrentCollections.newQueue();
        // check that the source indexing shard sent a new commit notification with the correct generation and node id
        final var sourceNotificationReceived = new CountDownLatch(1);
        MockTransportService.getInstance(searchNode)
            .addRequestHandlingBehavior(TransportNewCommitNotificationAction.NAME + "[u]", (handler, request, channel, task) -> {
                var notification = asInstanceOf(NewCommitNotificationRequest.class, request);
                assertThat(notification.getTerm(), equalTo(primaryTerm));

                if (notification.getGeneration() == beforeGeneration) {
                    assertThat(notification.getNodeId(), equalTo(getNodeId(indexNodeSource)));
                    // Delayed the uploaded notification to ensure fetching from the indexing node
                    if (notification.isUploaded()) {
                        delayedActions.add(() -> handler.messageReceived(request, channel, task));
                    } else {
                        sourceNotificationReceived.countDown();
                        handler.messageReceived(request, channel, task);
                    }
                } else {
                    handler.messageReceived(request, channel, task);
                }
            });

        // check that the source indexing shard receives at least one GetVirtualBatchedCompoundCommitChunkRequest
        final var sourceGetChunkRequestReceived = new CountDownLatch(1);
        MockTransportService.getInstance(indexNodeSource)
            .addRequestHandlingBehavior(
                TransportGetVirtualBatchedCompoundCommitChunkAction.NAME + "[p]",
                (handler, request, channel, task) -> {
                    var chunkRequest = asInstanceOf(GetVirtualBatchedCompoundCommitChunkRequest.class, request);
                    assertThat(chunkRequest.getPrimaryTerm(), equalTo(primaryTerm));

                    if (chunkRequest.getVirtualBatchedCompoundCommitGeneration() == beforeGeneration) {
                        assertThat(chunkRequest.getPreferredNodeId(), equalTo(getNodeId(indexNodeSource)));
                        sourceGetChunkRequestReceived.countDown();
                    }
                    handler.messageReceived(request, channel, task);
                }
            );

        var refreshFuture = admin().indices().prepareRefresh(indexName).execute();
        safeAwait(sourceNotificationReceived);
        safeAwait(sourceGetChunkRequestReceived);

        // check that the target indexing shard sent a new commit notification with the correct generation and node id
        final var targetNotificationReceived = new CountDownLatch(1);
        MockTransportService.getInstance(searchNode)
            .addRequestHandlingBehavior(TransportNewCommitNotificationAction.NAME + "[u]", (handler, request, channel, task) -> {
                var notification = asInstanceOf(NewCommitNotificationRequest.class, request);
                assertThat(notification.getTerm(), equalTo(primaryTerm));

                if (notification.getGeneration() == afterGeneration) {
                    assertThat(
                        "Commit notification " + notification + " has the wrong preferred node id",
                        notification.getNodeId(),
                        equalTo(getNodeId(indexNodeTarget))
                    );
                    // Delayed the uploaded notification to ensure fetching from the indexing node
                    if (notification.isUploaded()) {
                        delayedActions.add(() -> handler.messageReceived(request, channel, task));
                    } else {
                        targetNotificationReceived.countDown();
                        handler.messageReceived(request, channel, task);
                    }
                } else {
                    handler.messageReceived(request, channel, task);
                }
            });

        // check that the target indexing shard receives at least one GetVirtualBatchedCompoundCommitChunkRequest
        final var targetGetChunkRequestReceived = new CountDownLatch(1);
        MockTransportService.getInstance(indexNodeTarget)
            .addRequestHandlingBehavior(
                TransportGetVirtualBatchedCompoundCommitChunkAction.NAME + "[p]",
                (handler, request, channel, task) -> {
                    var chunkRequest = asInstanceOf(GetVirtualBatchedCompoundCommitChunkRequest.class, request);
                    assertThat(chunkRequest.getPrimaryTerm(), equalTo(primaryTerm));

                    if (chunkRequest.getVirtualBatchedCompoundCommitGeneration() == afterGeneration) {
                        assertThat(
                            "Chunk request " + chunkRequest + " has the wrong preferred node id",
                            chunkRequest.getPreferredNodeId(),
                            equalTo(getNodeId(indexNodeTarget))
                        );
                        targetGetChunkRequestReceived.countDown();
                    }
                    handler.messageReceived(request, channel, task);
                }
            );

        logger.info("--> resume relocation");
        resumeRelocation.countDown();

        safeAwait(targetNotificationReceived);
        safeAwait(targetGetChunkRequestReceived);
        assertThat(refreshFuture.actionGet().getFailedShards(), equalTo(0));

        for (CheckedRunnable<Exception> delayedAction : delayedActions) {
            delayedAction.run();
        }
        // also waits for no relocating shards.
        ensureGreen(indexName);
    }

    public static class DisableWarmingPlugin extends Stateless {

        static final Setting<Boolean> ENABLED_WARMING = Setting.boolSetting(
            "test.stateless.warming_enabled",
            true,
            Setting.Property.NodeScope
        );

        public DisableWarmingPlugin(Settings settings) {
            super(settings);
        }

        @Override
        public List<Setting<?>> getSettings() {
            return CollectionUtils.concatLists(super.getSettings(), List.of(ENABLED_WARMING));
        }

        @Override
        protected SharedBlobCacheWarmingService createSharedBlobCacheWarmingService(
            StatelessSharedBlobCacheService cacheService,
            ThreadPool threadPool,
            TelemetryProvider telemetryProvider,
            Settings settings
        ) {
            if (ENABLED_WARMING.get(settings)) {
                return super.createSharedBlobCacheWarmingService(cacheService, threadPool, telemetryProvider, settings);
            }
            return new SharedBlobCacheWarmingService(cacheService, threadPool, telemetryProvider, settings) {
                @Override
                protected void warmCache(
                    Type type,
                    IndexShard indexShard,
                    StatelessCompoundCommit commit,
                    BlobStoreCacheDirectory directory,
                    ActionListener<Void> listener
                ) {
                    ActionListener.completeWith(listener, () -> null);
                }

                @Override
                public void warmCacheBeforeUpload(VirtualBatchedCompoundCommit vbcc, ActionListener<Void> listener) {
                    ActionListener.completeWith(listener, () -> null);
                }
            };
        }
    }

    public void testRelocatingIndexShardFetchesFirstCommitsRegionsOnly() throws Exception {
        startMasterOnlyNode();

        var cacheSize = ByteSizeValue.ofMb(1L);
        var regionSize = ByteSizeValue.ofBytes((long) randomIntBetween(1, 3) * SharedBytes.PAGE_SIZE);

        final var indexNodesSettings = Settings.builder()
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), cacheSize)
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), regionSize)
            .put(SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), regionSize)
            .put(SharedBlobCacheWarmingService.PREWARMING_RANGE_MINIMIZATION_STEP.getKey(), regionSize)
            .put(StatelessCommitService.STATELESS_COMMIT_USE_INTERNAL_FILES_REPLICATED_CONTENT.getKey(), true)
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_SIZE.getKey(), ByteSizeValue.ofGb(1))
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 100)
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            // Disable warming because it will populate the cache for more than the test expects. Once ES-9363 is implemented and warming
            // is skipped for commit with replicated ranges we can adjust DisableWarmingPlugin to disable warm-on-upload only.
            .put(DisableWarmingPlugin.ENABLED_WARMING.getKey(), false)
            .build();

        final var indexNode = startIndexNode(indexNodesSettings);
        final String indexName = randomIdentifier();
        assertAcked(
            prepareCreate(indexName).setSettings(
                indexSettings(1, 0).put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), ByteSizeValue.ofGb(1L))
                    .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
                    .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), "false")
                    .put(EngineConfig.USE_COMPOUND_FILE, randomBoolean())
                    .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
                    // Causes extra reads when randomly enabled by the test framework, so it is disabled in this test
                    .put(IndexSettings.BLOOM_FILTER_ID_FIELD_ENABLED_SETTING.getKey(), false)
                    .build()
            ).setMapping("""
                    {
                        "properties":{
                            "junk":{"type":"binary","doc_values":false,"store":true}
                        }
                    }
                """)
        );

        var indexShard = findIndexShard(resolveIndex(indexName), 0);
        var indexDirectory = IndexDirectory.unwrapDirectory(indexShard.store().directory());
        var commitService = internalCluster().getInstance(StatelessCommitService.class, indexNode);

        var nbBlobs = randomIntBetween(1, 3);
        long totalWriteCount = 0L;

        for (int blob = 0; blob < nbBlobs; blob++) {
            int nbCommits = randomIntBetween(1, 5);
            for (int commit = 0; commit < nbCommits; commit++) {
                long initialSizeInBytes = indexDirectory.estimateSizeInBytes();
                while (indexDirectory.estimateSizeInBytes() - initialSizeInBytes < regionSize.getBytes() + 1L) {
                    var bulkRequest = client().prepareBulk();
                    for (int doc = 0; doc < 100; doc++) {
                        bulkRequest.add(
                            prepareIndex(indexName).setSource(
                                "junk",
                                Base64.getEncoder().encodeToString(randomAlphaOfLength(100).getBytes(StandardCharsets.UTF_8))
                            )
                        );
                    }
                    assertNoFailures(bulkRequest.get(TimeValue.timeValueSeconds(10)));
                    indexShard.writeIndexingBuffer();
                }
                refresh(indexName);
            }
            flush(indexName);

            var batchedCompoundCommit = commitService.getLatestUploadedBcc(indexShard.shardId());
            assertThat(batchedCompoundCommit, notNullValue());

            var blobLength = getBlobLength(indexDirectory, batchedCompoundCommit.primaryTermAndGeneration());
            var blobRegionsTotal = computedFetchedRegions(0L, blobLength, regionSize.getBytes(), new HashSet<>()).size();
            // flag to indicate that this is the BCC the new indexing node will recover from
            boolean isRecoveredBcc = blob == nbBlobs - 1;

            int blobRegionsForRecovery = computeFetchedRegionsForRecovery(
                batchedCompoundCommit,
                blobLength,
                regionSize.getBytes(),
                isRecoveredBcc
            );
            logger.debug(
                "--> BCC blob {} of size [{}] bytes requires to read [{}/{}] regions of [{}] bytes during recovery",
                batchedCompoundCommit.primaryTermAndGeneration(),
                blobLength,
                blobRegionsForRecovery,
                blobRegionsTotal,
                regionSize.getBytes()
            );
            assertThat(blobRegionsForRecovery, lessThan(blobRegionsTotal));
            totalWriteCount += blobRegionsForRecovery;
        }

        final var indexNode2 = startIndexNode(indexNodesSettings);
        ensureStableCluster(3);

        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNode), indexName);
        assertBusy(() -> assertThat(internalCluster().nodesInclude(indexName), not(hasItem(indexNode))));
        ensureGreen(indexName);

        var cacheService = internalCluster().getInstance(Stateless.SharedBlobCacheServiceSupplier.class, indexNode2).get();
        assertThat(cacheService.getStats().writeCount(), equalTo(totalWriteCount));
        assertThat(cacheService.getStats().numberOfRegions(), greaterThan(1));
    }

    /**
     * Returns the length of the blob stored in the object store.
     */
    private static long getBlobLength(IndexDirectory indexDirectory, PrimaryTermAndGeneration primaryTermAndGeneration) throws IOException {
        var blobName = StatelessCompoundCommit.blobNameFromGeneration(primaryTermAndGeneration.generation());
        var blobs = IndexBlobStoreCacheDirectory.unwrapDirectory(indexDirectory)
            .getBlobContainer(primaryTermAndGeneration.primaryTerm())
            .listBlobsByPrefix(OperationPurpose.INDICES, blobName);
        assertThat(blobs, notNullValue());
        assertThat(blobs.size(), equalTo(1));
        return blobs.get(blobName).length();
    }

    /**
     * Computes the expected number of distinct regions to fetch in cache in order to recover the BCC.
     */
    public static int computeFetchedRegionsForRecovery(
        BatchedCompoundCommit latestBcc,
        long latestBccSizeInBytes,
        long regionSizeInBytes,
        boolean isRecoveredBcc
    ) {
        Set<Integer> regions = new HashSet<>();
        if (latestBcc != null) {
            long offset = 0L;
            for (var compoundCommit : latestBcc.compoundCommits()) {
                assert offset == BlobCacheUtils.toPageAlignedSize(offset);

                // compute the number of fetched regions in cache required to read the header
                computedFetchedRegions(offset, compoundCommit.headerSizeInBytes(), regionSizeInBytes, regions);
                offset += compoundCommit.headerSizeInBytes();

                // compute the number of fetched regions in cache required to read the replicated ranges
                computedFetchedRegions(
                    offset,
                    compoundCommit.internalFilesReplicatedRanges().dataSizeInBytes(),
                    regionSizeInBytes,
                    regions
                );
                offset += compoundCommit.internalFilesReplicatedRanges().dataSizeInBytes();

                // compute the number of fetched regions in cache to read the Lucene metadata files
                var internalFiles = compoundCommit.commitFiles()
                    .entrySet()
                    .stream()
                    .filter(entry -> compoundCommit.internalFiles().contains(entry.getKey()))
                    .sorted(Comparator.comparingLong(f -> f.getValue().offset()))
                    .toList();
                for (var internalFile : internalFiles) {
                    var blobLocation = internalFile.getValue();
                    // Lucene fully reads metadata file and the most recent segments_N when opening the index. If those files are larger
                    // than what the replicated ranges stores in the first region, then we need to account for the region(s) fetched to
                    // read the whole file.
                    if (REPLICATED_CONTENT_MAX_SINGLE_FILE_SIZE < blobLocation.fileLength()) {
                        var fileName = internalFile.getKey();
                        var fileExt = LuceneFilesExtensions.fromFile(fileName);
                        if (fileExt != null && fileExt.isMetadata()
                            || compoundCommit == latestBcc.lastCompoundCommit()
                                && isRecoveredBcc
                                && fileName.startsWith(IndexFileNames.SEGMENTS)) {
                            computedFetchedRegions(offset, blobLocation.fileLength(), regionSizeInBytes, regions);
                        }
                    }
                    offset += blobLocation.fileLength();
                }

                if (offset < latestBccSizeInBytes) {
                    long compoundCommitSizePageAligned = BlobCacheUtils.toPageAlignedSize(compoundCommit.sizeInBytes());
                    int paddingLength = Math.toIntExact(compoundCommitSizePageAligned - compoundCommit.sizeInBytes());

                    // When assertions are enabled, extra reads are executed to assert that padding bytes at the end of the compound commit
                    // are effectively zeros (see BatchedCompoundCommit#assertPaddingComposedOfZeros). Those reads trigger more cache misses
                    // and populates the cache for regions that do not include headers, so we must account for them in this test.
                    if (Assertions.ENABLED && 0 < paddingLength) {
                        computedFetchedRegions(offset, paddingLength, regionSizeInBytes, regions);
                    }
                    offset += paddingLength;
                }
            }
        }
        return regions.size();
    }

    /**
     * Computes the number of regions that needs to be fetched to read {@code length} bytes from position {@code offset}.
     */
    private static Set<Integer> computedFetchedRegions(long offset, long length, long regionSizeInBytes, Set<Integer> regions) {
        int regionStart = (int) (offset / regionSizeInBytes);
        long endOffset = offset + length;
        int regionEnd;
        if (endOffset % regionSizeInBytes == 0) {
            regionEnd = (int) ((endOffset - 1L) / regionSizeInBytes);
        } else {
            regionEnd = (int) (endOffset / regionSizeInBytes);
        }
        IntStream.rangeClosed(regionStart, regionEnd).forEach(regions::add);
        return regions;
    }
}
