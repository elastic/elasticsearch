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

package co.elastic.elasticsearch.stateless;

import co.elastic.elasticsearch.stateless.action.GetVirtualBatchedCompoundCommitChunkRequest;
import co.elastic.elasticsearch.stateless.action.NewCommitNotificationRequest;
import co.elastic.elasticsearch.stateless.action.NewCommitNotificationResponse;
import co.elastic.elasticsearch.stateless.action.TransportGetVirtualBatchedCompoundCommitChunkAction;
import co.elastic.elasticsearch.stateless.action.TransportNewCommitNotificationAction;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.commits.VirtualBatchedCompoundCommit;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.engine.SearchEngine;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreTestUtils;
import co.elastic.elasticsearch.stateless.recovery.RegisterCommitRequest;
import co.elastic.elasticsearch.stateless.recovery.TransportRegisterCommitForRecoveryAction;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.TransportUnpromotableShardRefreshAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TestTransportChannel;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.xpack.shutdown.PutShutdownNodeAction;
import org.elasticsearch.xpack.shutdown.ShutdownPlugin;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;

import static co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit.blobNameFromGeneration;
import static co.elastic.elasticsearch.stateless.recovery.TransportStatelessPrimaryRelocationAction.START_RELOCATION_ACTION_NAME;
import static java.util.stream.Collectors.toList;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_INTERVAL_SETTING;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_INTERVAL_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_RETRY_COUNT_SETTING;
import static org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService.HEARTBEAT_FREQUENCY;
import static org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.Type.SIGTERM;
import static org.elasticsearch.discovery.PeerFinder.DISCOVERY_FIND_PEERS_INTERVAL_SETTING;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.oneOf;

public class StatelessRecoveryIT extends AbstractStatelessIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.concatLists(List.of(MockRepository.Plugin.class, ShutdownPlugin.class), super.nodePlugins());
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            .put(HEARTBEAT_FREQUENCY.getKey(), TimeValue.timeValueSeconds(5))
            .put(FOLLOWER_CHECK_INTERVAL_SETTING.getKey(), "100ms")
            .put(FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey(), "1")
            .put(DISCOVERY_FIND_PEERS_INTERVAL_SETTING.getKey(), "100ms")
            .put(LEADER_CHECK_INTERVAL_SETTING.getKey(), "100ms")
            .put(LEADER_CHECK_RETRY_COUNT_SETTING.getKey(), "1")
            .put(Coordinator.PUBLISH_TIMEOUT_SETTING.getKey(), "1s")
            .put(TransportSettings.CONNECT_TIMEOUT.getKey(), "5s");
    }

    @Before
    public void init() {
        startMasterOnlyNode();
    }

    private void testTranslogRecovery(boolean heavyIndexing) throws Exception {
        startIndexNodes(2);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), new TimeValue(1, TimeUnit.MINUTES)).build()
        );
        ensureGreen(indexName);

        if (heavyIndexing) {
            indexDocuments(indexName, false); // produces several commits
            indexDocs(indexName, randomIntBetween(50, 100));
        } else {
            indexDocs(indexName, randomIntBetween(1, 5));
        }

        // The following custom documents will exist in translog and not committed before the node restarts.
        // After the node restarts, we can search for them to check that they exist.
        int customDocs = randomIntBetween(1, 5);
        int baseId = randomIntBetween(200, 300);
        for (int i = 0; i < customDocs; i++) {
            index(indexName, String.valueOf(baseId + i), Map.of("custom", "value"));
        }

        // Assert that the seqno before and after restarting the indexing node is the same
        SeqNoStats beforeSeqNoStats = client().admin().indices().prepareStats(indexName).get().getShards()[0].getSeqNoStats();
        Index index = resolveIndices().keySet().stream().filter(i -> i.getName().equals(indexName)).findFirst().get();
        DiscoveryNode node = findIndexNode(index, 0);
        internalCluster().restartNode(node.getName());
        ensureGreen(indexName);
        SeqNoStats afterSeqNoStats = client().admin().indices().prepareStats(indexName).get().getShards()[0].getSeqNoStats();
        assertEquals(beforeSeqNoStats, afterSeqNoStats);

        // Assert that the custom documents added above are returned when searched
        startSearchNodes(1);
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1), indexName);
        ensureGreen(indexName);
        assertHitCount(prepareSearch(indexName).setQuery(QueryBuilders.termQuery("custom", "value")), customDocs);
    }

    public void testTranslogRecoveryWithHeavyIndexing() throws Exception {
        testTranslogRecovery(true);
    }

    public void testTranslogRecoveryWithLightIndexing() throws Exception {
        testTranslogRecovery(false);
    }

    /**
     * Verify that if we index after a relocation, we remember the indexed ops even if the new node crashes.
     * This ensures that there is a flush with a new translog registration after relocation.
     */
    public void testIndexAfterRelocation() throws IOException {
        final var numShards = randomIntBetween(1, 3);
        final var indexNode = startIndexNode();

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(numShards, 0).build());
        ensureGreen(indexName);
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

        docIndexer.accept(between(1, 10));

        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", indexNode), indexName);

        final var indexNode2 = startIndexNode();

        // wait for relocation
        ensureGreen();

        docIndexer.accept(between(1, 10));

        // we ought to crash, but do not flush on close in stateless
        internalCluster().stopNode(indexNode2);
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", (String) null), indexName);
        ensureGreen();

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

    public void testRecoverSearchShard() throws IOException {

        startIndexNode();

        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        int numDocs = randomIntBetween(1, 100);
        indexDocs(indexName, numDocs);
        refresh(indexName);

        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1));

        var searchNode1 = startSearchNode();
        ensureGreen(indexName);
        assertHitCount(prepareSearch(indexName), numDocs);
        internalCluster().stopNode(searchNode1);

        var searchNode2 = startSearchNode();
        ensureGreen(indexName);
        assertHitCount(prepareSearch(indexName), numDocs);
        internalCluster().stopNode(searchNode2);
    }

    public void testRecoverSearchShardFromVirtualBcc() {
        assumeTrue("Test only works when uploads are delayed", STATELESS_UPLOAD_DELAYED);
        startIndexNode(Settings.builder().put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 10).build());

        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        long totalDocs = 0L;
        int initialCommits = randomIntBetween(0, 3);
        for (int i = 0; i < initialCommits; i++) {
            int numDocs = randomIntBetween(1, 100);
            indexDocs(indexName, numDocs);
            flush(indexName);
            totalDocs += numDocs;
        }

        var indexEngine = asInstanceOf(IndexEngine.class, findIndexShard(resolveIndex(indexName), 0).getEngineOrNull());
        final long initialGeneration = indexEngine.getCurrentGeneration();

        final int iters = randomIntBetween(1, 5);
        for (int i = 0; i < iters; i++) {
            int numDocs = randomIntBetween(1, 100);
            indexDocs(indexName, numDocs);
            refresh(indexName);
            totalDocs += numDocs;
        }

        var commitService = indexEngine.getStatelessCommitService();
        VirtualBatchedCompoundCommit virtualBcc = commitService.getCurrentVirtualBcc(indexEngine.config().getShardId());
        assertThat(virtualBcc, notNullValue());
        assertThat(virtualBcc.getPrimaryTermAndGeneration().generation(), equalTo(initialGeneration + 1));
        assertThat(virtualBcc.lastCompoundCommit().generation(), equalTo(initialGeneration + iters));

        startSearchNode();
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1));
        ensureGreen(indexName);

        var searchEngine = asInstanceOf(SearchEngine.class, findSearchShard(resolveIndex(indexName), 0).getEngineOrNull());
        assertThat(searchEngine.getLastCommittedSegmentInfos().getGeneration(), equalTo(virtualBcc.lastCompoundCommit().generation()));
        assertThat(searchEngine.getAcquiredPrimaryTermAndGenerations(), hasItem(virtualBcc.getPrimaryTermAndGeneration()));
        assertHitCount(prepareSearch(indexName), totalDocs);

        flush(indexName); // TODO Fix this, the flush should not be mandatory to delete the index (see ES-8335)
    }

    public void testRecoverMultipleIndexingShardsWithCoordinatingRetries() throws Exception {
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

    public void testStartingTranslogFileWrittenInCommit() throws Exception {
        List<String> indexNodes = startIndexNodes(1);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), new TimeValue(1, TimeUnit.HOURS)).build()
        );
        ensureGreen(indexName);

        final int iters = randomIntBetween(1, 10);
        for (int i = 0; i < iters; i++) {
            indexDocs(indexName, randomIntBetween(1, 100));
        }

        var objectStoreService = internalCluster().getInstance(ObjectStoreService.class, indexNodes.get(0));
        Map<String, BlobMetadata> translogFiles = objectStoreService.getTranslogBlobContainer().listBlobs(operationPurpose);

        final String newIndex = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            newIndex,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), new TimeValue(1, TimeUnit.HOURS)).build()
        );
        ensureGreen(newIndex);

        Index index = resolveIndex(newIndex);
        IndexShard indexShard = findShard(index, 0, DiscoveryNodeRole.INDEX_ROLE, ShardRouting.Role.INDEX_ONLY);
        var blobContainerForCommit = objectStoreService.getBlobContainer(indexShard.shardId(), indexShard.getOperationPrimaryTerm());
        String commitFile = blobNameFromGeneration(Lucene.readSegmentInfos(indexShard.store().directory()).getGeneration());
        assertThat(commitFile, blobContainerForCommit.blobExists(operationPurpose, commitFile), is(true));
        StatelessCompoundCommit commit = StatelessCompoundCommit.readFromStore(
            new InputStreamStreamInput(blobContainerForCommit.readBlob(operationPurpose, commitFile))
        );

        long initialRecoveryCommitStartingFile = commit.translogRecoveryStartFile();

        // Greater than or equal to because translog files start at 0
        assertThat(initialRecoveryCommitStartingFile, greaterThanOrEqualTo((long) translogFiles.size()));

        indexDocs(newIndex, randomIntBetween(1, 5));

        flush(newIndex);

        commitFile = blobNameFromGeneration(Lucene.readSegmentInfos(indexShard.store().directory()).getGeneration());
        assertThat(commitFile, blobContainerForCommit.blobExists(operationPurpose, commitFile), is(true));
        commit = StatelessCompoundCommit.readFromStore(
            new InputStreamStreamInput(blobContainerForCommit.readBlob(operationPurpose, commitFile))
        );

        // Recovery file has advanced because of flush
        assertThat(commit.translogRecoveryStartFile(), greaterThan(initialRecoveryCommitStartingFile));
    }

    public void testRecoveryMarksNewNodeInCommit() throws Exception {
        String initialNode = startIndexNodes(1).get(0);
        startSearchNode();
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), new TimeValue(1, TimeUnit.MINUTES)).build()
        );
        ensureGreen(indexName);

        int numDocsRound1 = randomIntBetween(1, 100);
        indexDocs(indexName, numDocsRound1);
        refresh(indexName);

        assertHitCount(prepareSearch(indexName), numDocsRound1);

        internalCluster().stopNode(initialNode);
        // second replacement node. we are checking here that green state == flush occurred so that the third node recovers from the correct
        // commit which will reference the buffered translog operations written on the second node
        String secondNode = startIndexNode();

        ensureGreen(indexName);

        int numDocsRound2 = randomIntBetween(1, 100);
        indexDocs(indexName, numDocsRound2);

        internalCluster().stopNode(secondNode);
        startIndexNode(); // third replacement node
        ensureGreen(indexName);

        assertHitCount(prepareSearch(indexName), numDocsRound1 + numDocsRound2);
    }

    public void testRelocateIndexingShardDoesNotReadFromTranslog() throws Exception {
        final String indexNodeA = startIndexNode();
        ensureStableCluster(2);
        final String indexName = "test";
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), new TimeValue(1, TimeUnit.MINUTES)).build()
        );

        final String indexNodeB = startIndexNode();
        ensureStableCluster(3);

        int numDocs = scaledRandomIntBetween(1, 10);
        indexDocs(indexName, numDocs);

        ObjectStoreService objectStoreService = internalCluster().getInstance(ObjectStoreService.class, indexNodeB);
        MockRepository repository = ObjectStoreTestUtils.getObjectStoreMockRepository(objectStoreService);

        logger.info("--> accessing translog would fail relocation");
        repository.setRandomControlIOExceptionRate(1.0);
        repository.setRandomDataFileIOExceptionRate(1.0);
        repository.setMaximumNumberOfFailures(Long.MAX_VALUE);
        repository.setRandomIOExceptionPattern(".*translog.*");

        logger.info("--> Replacing {} with {}", indexNodeA, indexNodeB);
        assertThat(findIndexShard(resolveIndex(indexName), 0).routingEntry().currentNodeId(), equalTo(getNodeId(indexNodeA)));
        var timeout = TimeValue.timeValueSeconds(30);
        clusterAdmin().execute(
            PutShutdownNodeAction.INSTANCE,
            new PutShutdownNodeAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                getNodeId(indexNodeA),
                SIGTERM,
                "node sigterm",
                null,
                null,
                timeout
            )
        ).actionGet(TimeValue.timeValueSeconds(10));

        ensureGreen(timeout);
        internalCluster().stopNode(indexNodeA);

        assertThat(repository.getFailureCount(), equalTo(0L));
        assertNodeHasNoCurrentRecoveries(indexNodeB);
        assertThat(findIndexShard(resolveIndex(indexName), 0).routingEntry().currentNodeId(), equalTo(getNodeId(indexNodeB)));
        assertThat(findIndexShard(resolveIndex(indexName), 0).docStats().getCount(), equalTo((long) numDocs));
    }

    public void testIndexShardRecoveryDoesNotUseTranslogOperationsBeforeFlush() throws Exception {
        final String indexNodeA = startIndexNode();

        String indexName = "test-index";
        createIndex(indexName, indexSettings(1, 0).put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true).build());
        indexRandom(
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            IntStream.range(0, between(0, 100)).mapToObj(n -> client().prepareIndex(indexName).setSource("num", n)).collect(toList())
        );

        final String indexNodeB = startIndexNode();
        ensureStableCluster(3);

        final ShardId shardId = new ShardId(resolveIndex(indexName), 0);
        final DiscoveryNodes discoveryNodes = clusterService().state().nodes();
        final IndexShardRoutingTable indexShardRoutingTable = clusterService().state().routingTable().shardRoutingTable(shardId);

        final IndexShard primary = internalCluster().getInstance(
            IndicesService.class,
            discoveryNodes.get(indexShardRoutingTable.primaryShard().currentNodeId()).getName()
        ).getShardOrNull(shardId);
        final long maxSeqNoBeforeFlush = primary.seqNoStats().getMaxSeqNo();
        assertBusy(() -> assertThat(primary.getLastSyncedGlobalCheckpoint(), equalTo(maxSeqNoBeforeFlush)));
        assertThat(indicesAdmin().prepareFlush(indexName).get().getFailedShards(), is(0));

        indexRandom(
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            IntStream.range(0, between(0, 100)).mapToObj(n -> client().prepareIndex(indexName).setSource("num", n)).collect(toList())
        );

        final long maxSeqNoAfterFlush = primary.seqNoStats().getMaxSeqNo();
        logger.info("--> stopping node {} in order to re-allocate indexing shard on node {}", indexNodeA, indexNodeB);
        internalCluster().stopNode(indexNodeA);
        ensureGreen(indexName);

        // noinspection OptionalGetWithoutIsPresent because it fails the test if absent
        final RecoveryState recoveryState = indicesAdmin().prepareRecoveries(indexName)
            .get()
            .shardRecoveryStates()
            .get(indexName)
            .stream()
            .filter(RecoveryState::getPrimary)
            .findFirst()
            .get();
        assertThat((long) recoveryState.getTranslog().recoveredOperations(), lessThanOrEqualTo(maxSeqNoAfterFlush - maxSeqNoBeforeFlush));
    }

    public void testNewCommitNotificationOfRecoveringSearchShard() throws Exception {
        String indexNode = startIndexNode();
        String searchNode = startSearchNode();
        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);
        int totalDocs = randomIntBetween(1, 10);
        indexDocs(indexName, totalDocs);
        IndexShard indexShard = findIndexShard(indexName);
        long initialGeneration = Lucene.readSegmentInfos(indexShard.store().directory()).getGeneration();
        logger.info("--> Indexed {} docs, initial indexing shard generation is {}", totalDocs, initialGeneration);

        // Establishing a handler on the indexing node for receiving the request from the search node to recover the initial commit
        CountDownLatch initialCommitRegistrationProcessed = new CountDownLatch(1);
        MockTransportService.getInstance(indexNode)
            .addRequestHandlingBehavior(TransportRegisterCommitForRecoveryAction.NAME, (handler, request, channel, task) -> {
                RegisterCommitRequest r = (RegisterCommitRequest) request;
                handler.messageReceived(request, channel, task);
                initialCommitRegistrationProcessed.countDown();
            });

        // Establishing a sender on the search node to block recovery after sending the request to the indexing node to register the commit
        ObjectStoreService objectStoreService = internalCluster().getInstance(ObjectStoreService.class, searchNode);
        MockRepository repository = ObjectStoreTestUtils.getObjectStoreMockRepository(objectStoreService);
        MockTransportService.getInstance(searchNode).addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(TransportRegisterCommitForRecoveryAction.NAME)) {
                logger.info("--> Blocking recovery on search node before sending request to register recovering commit");
                repository.setBlockOnAnyFiles();
            }
            connection.sendRequest(requestId, action, request, options);
        });

        // Block fetching data from indexing node in addition to blobstore to control the progress on search node
        CountDownLatch getVbccChunkLatch = new CountDownLatch(1);
        MockTransportService.getInstance(indexNode)
            .addRequestHandlingBehavior(
                TransportGetVirtualBatchedCompoundCommitChunkAction.NAME + "[p]",
                (handler, request, channel, task) -> {
                    safeAwait(getVbccChunkLatch);
                    handler.messageReceived(request, channel, task);
                }
            );

        // Establishing a handler on the search node for receiving the new commit notification request from the indexing node
        // and tracking the new commit notification response before it is sent to the indexing node.
        // countdown for both non-uploaded and uploaded when upload is delayed
        CountDownLatch newCommitNotificationReceived = new CountDownLatch(STATELESS_UPLOAD_DELAYED ? 2 : 1);
        AtomicLong newCommitNotificationResponseGeneration = new AtomicLong(-1L);
        MockTransportService.getInstance(searchNode)
            .addRequestHandlingBehavior(TransportNewCommitNotificationAction.NAME + "[u]", (handler, request, channel, task) -> {
                handler.messageReceived(request, new TestTransportChannel(new ChannelActionListener<>(channel).delegateFailure((l, tr) -> {
                    var termGens = ((NewCommitNotificationResponse) tr).getPrimaryTermAndGenerationsInUse();
                    // The search shard will use the latest notified generation.
                    // It can also sometimes still refers to the old generation if it is not closed fast enough
                    assertThat(termGens.size(), oneOf(1, 2));
                    termGens.stream()
                        .max(PrimaryTermAndGeneration::compareTo)
                        .ifPresent(termAndGen -> newCommitNotificationResponseGeneration.set(termAndGen.generation()));
                    l.onResponse(tr);
                })), task);
                assertThat(newCommitNotificationReceived.getCount(), greaterThan(0L));
                newCommitNotificationReceived.countDown();
            });

        logger.info("--> Initiating search shard recovery");
        setReplicaCount(1, indexName);
        ensureYellow(indexName);

        logger.info("--> Waiting for index node to process the registration of the initial commit recovery on the search node");
        safeAwait(initialCommitRegistrationProcessed);

        logger.info("--> Flushing a new commit and send out notification to the search node");
        client().admin().indices().prepareFlush(indexName).setForce(true).get();

        logger.info("--> Waiting for search node to process new commit notification request");
        safeAwait(newCommitNotificationReceived);
        assertThat(newCommitNotificationResponseGeneration.get(), equalTo(-1L));
        Index index = resolveIndices().entrySet().stream().filter(e -> e.getKey().getName().equals(indexName)).findAny().get().getKey();
        IndexShard searchShard = internalCluster().getInstance(IndicesService.class, searchNode).indexService(index).getShard(0);
        assertNull(searchShard.getEngineOrNull());

        logger.info("--> Unblocking the recovery of the search shard");
        repository.unblock();
        getVbccChunkLatch.countDown();
        ensureGreen(indexName);

        logger.info("--> Waiting for the new commit notification success");
        assertBusy(() -> assertThat(newCommitNotificationResponseGeneration.get(), greaterThan(initialGeneration)));

        // Assert that the search shard is on the new commit generation
        long searchGeneration = Lucene.readSegmentInfos(searchShard.store().directory()).getGeneration();
        assertThat(searchGeneration, equalTo(newCommitNotificationResponseGeneration.get()));

        // Assert that a search returns all the documents
        assertResponse(prepareSearch(indexName).setQuery(matchAllQuery()), searchResponse -> {
            assertNoFailures(searchResponse);
            assertEquals(totalDocs, searchResponse.getHits().getTotalHits().value);
        });
    }

    public void testRefreshOfRecoveringSearchShard() throws Exception {
        startIndexNode();
        var searchNode = startSearchNode();
        final var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);
        int totalDocs = randomIntBetween(1, 10);
        indexDocs(indexName, totalDocs);
        logger.info("--> Indexed {} docs", totalDocs);

        var unpromotableRefreshLatch = new CountDownLatch(1);
        var mockTransportService = (MockTransportService) internalCluster().getInstance(TransportService.class, searchNode);
        mockTransportService.addRequestHandlingBehavior(
            TransportUnpromotableShardRefreshAction.NAME + "[u]",
            (handler, request, channel, task) -> {
                handler.messageReceived(request, channel, task);
                unpromotableRefreshLatch.countDown();
            }
        );

        ObjectStoreService objectStoreService = internalCluster().getInstance(ObjectStoreService.class, searchNode);
        MockRepository repository = ObjectStoreTestUtils.getObjectStoreMockRepository(objectStoreService);
        repository.setBlockOnAnyFiles();
        setReplicaCount(1, indexName);
        var future = client().admin().indices().prepareRefresh(indexName).execute();
        safeAwait(unpromotableRefreshLatch);
        repository.unblock();

        var refreshResponse = future.get();
        assertThat("Refresh should have been successful", refreshResponse.getSuccessfulShards(), equalTo(1));

        assertResponse(prepareSearch(indexName).setQuery(matchAllQuery()), searchResponse -> {
            assertNoFailures(searchResponse);
            assertEquals(totalDocs, searchResponse.getHits().getTotalHits().value);
        });
    }

    public void testRefreshOfRecoveringSearchShardAndDeleteIndex() throws Exception {
        var indexNode = startIndexNode();
        var searchNode = startSearchNode();
        final var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);
        int totalDocs = randomIntBetween(1, 10);
        indexDocs(indexName, totalDocs);
        logger.info("--> Indexed {} docs", totalDocs);

        var unpromotableRefreshLatch = new CountDownLatch(1);
        var mockTransportService = (MockTransportService) internalCluster().getInstance(TransportService.class, searchNode);
        mockTransportService.addRequestHandlingBehavior(
            TransportUnpromotableShardRefreshAction.NAME + "[u]",
            (handler, request, channel, task) -> {
                handler.messageReceived(request, channel, task);
                unpromotableRefreshLatch.countDown();
            }
        );

        ObjectStoreService objectStoreService = internalCluster().getInstance(ObjectStoreService.class, searchNode);
        MockRepository repository = ObjectStoreTestUtils.getObjectStoreMockRepository(objectStoreService);
        repository.setBlockOnAnyFiles();
        setReplicaCount(1, indexName);
        var future = client(indexNode).admin().indices().prepareRefresh(indexName).execute();
        safeAwait(unpromotableRefreshLatch);

        logger.info("--> deleting index");
        assertAcked(client(indexNode).admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet());
        logger.info("--> unblocking recovery");
        repository.unblock();
        var refreshResponse = future.get();
        assertThat("Refresh should have failed", refreshResponse.getFailedShards(), equalTo(1));
        Throwable cause = ExceptionsHelper.unwrapCause(refreshResponse.getShardFailures()[0].getCause());
        assertThat("Cause should be engine closed", cause, instanceOf(AlreadyClosedException.class));
    }

    public void testPreferredNodeIdsAreUsedDuringRelocation() {
        startMasterOnlyNode();

        int maxNonUploadedCommits = randomIntBetween(1, 4);
        var nodeSettings = Settings.builder()
            .put(StatelessCommitService.STATELESS_UPLOAD_DELAYED.getKey(), true)
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), maxNonUploadedCommits)
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

        // check that the source indexing shard sent a new commit notification with the correct generation and node id
        final var sourceNotificationReceived = new CountDownLatch(1);
        MockTransportService.getInstance(searchNode)
            .addRequestHandlingBehavior(TransportNewCommitNotificationAction.NAME + "[u]", (handler, request, channel, task) -> {
                var notification = asInstanceOf(NewCommitNotificationRequest.class, request);
                assertThat(notification.getTerm(), equalTo(primaryTerm));

                if (notification.getGeneration() == beforeGeneration) {
                    assertThat(notification.getNodeId(), equalTo(getNodeId(indexNodeSource)));
                    sourceNotificationReceived.countDown();
                }
                handler.messageReceived(request, channel, task);
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
                    assertThat(notification.getNodeId(), equalTo(getNodeId(indexNodeTarget)));
                    targetNotificationReceived.countDown();
                }
                handler.messageReceived(request, channel, task);
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
                        assertThat(chunkRequest.getPreferredNodeId(), equalTo(getNodeId(indexNodeTarget)));
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

        // also waits for no relocating shards.
        ensureGreen(indexName);
    }
}
