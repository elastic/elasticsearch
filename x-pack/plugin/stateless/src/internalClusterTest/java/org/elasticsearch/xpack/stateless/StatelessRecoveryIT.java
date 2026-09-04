/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.command.AllocateEmptyPrimaryAllocationCommand;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.PostRecoveryMerger;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.xpack.shutdown.PutShutdownNodeAction;
import org.elasticsearch.xpack.shutdown.ShutdownPlugin;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreTestUtils;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_INTERVAL_SETTING;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_INTERVAL_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_RETRY_COUNT_SETTING;
import static org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.Type.SIGTERM;
import static org.elasticsearch.discovery.PeerFinder.DISCOVERY_FIND_PEERS_INTERVAL_SETTING;
import static org.elasticsearch.index.MergePolicyConfig.INDEX_MERGE_ENABLED;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit.blobNameFromGeneration;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class StatelessRecoveryIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.concatLists(
            List.of(MockRepository.Plugin.class, ShutdownPlugin.class, InternalSettingsPlugin.class),
            super.nodePlugins()
        );
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
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
            indexDocumentsThenFlushOrRefreshOrForceMerge(indexName); // produces several commits
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

    public void testStartingTranslogFileWrittenInCommit() throws Exception {
        var indexNode = startIndexNode(disableIndexingDiskAndMemoryControllersNodeSettings());
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

        var objectStoreService = getObjectStoreService(indexNode);
        Map<String, BlobMetadata> translogFiles = objectStoreService.getTranslogBlobContainer().listBlobs(operationPurpose);

        final String newIndex = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            newIndex,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), new TimeValue(1, TimeUnit.HOURS)).build()
        );
        ensureGreen(newIndex);

        Index index = resolveIndex(newIndex);
        IndexShard indexShard = findShard(index, 0, DiscoveryNodeRole.INDEX_ROLE, ShardRouting.Role.INDEX_ONLY);
        var blobContainerForCommit = objectStoreService.getProjectBlobContainer(indexShard.shardId(), indexShard.getOperationPrimaryTerm());
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
        final String indexNodeA = startIndexNode(disableIndexingDiskAndMemoryControllersNodeSettings());
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

        ObjectStoreService objectStoreService = getObjectStoreService(indexNodeB);
        MockRepository repository = ObjectStoreTestUtils.getObjectStoreMockRepository(objectStoreService);

        logger.info("--> accessing translog would fail relocation");
        // set exception filename pattern FIRST, before toggling IO exceptions for the repo
        repository.setRandomIOExceptionPattern(".*translog.*");
        repository.setRandomControlIOExceptionRate(1.0);
        repository.setRandomDataFileIOExceptionRate(1.0);
        repository.setMaximumNumberOfFailures(Long.MAX_VALUE);

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

        ensureGreen(timeout, indexName);
        internalCluster().stopNode(indexNodeA);

        assertThat(repository.getFailureCount(), equalTo(0L));
        assertNodeHasNoCurrentRecoveries(indexNodeB);
        assertThat(findIndexShard(resolveIndex(indexName), 0).routingEntry().currentNodeId(), equalTo(getNodeId(indexNodeB)));
        assertThat(findIndexShard(resolveIndex(indexName), 0).docStats().getCount(), equalTo((long) numDocs));
    }

    public void testIndexShardRecoveryDoesNotUseTranslogOperationsBeforeFlush() throws Exception {
        final String indexNodeA = startIndexNode(disableIndexingDiskAndMemoryControllersNodeSettings());

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

    /**
     * Tests that {@code allocate_empty_primary} in stateless mode creates a truly empty shard
     * but leaves orphaned blobs in the object store. After the empty allocation the test verifies:
     * <ol>
     *   <li>the primary term has advanced,</li>
     *   <li>the shard is empty (doc count = 0),</li>
     *   <li>new blobs exist under the new primary term, and</li>
     *   <li>blobs from the previous primary term remain orphaned — {@code allocate_empty_primary}
     *       uses {@code EmptyStoreRecoverySource} which bypasses {@code markRecoveredBcc}, so the
     *       commit cleaner is never made aware of the old blobs. Subsequent relocations also do not
     *       clean them up because the source sends its known blob list directly (short-circuiting
     *       the object store LIST), so the orphaned blobs are never rediscovered.</li>
     * </ol>
     */
    public void testAllocateEmptyPrimaryLeavesOrphanedBlobs() throws Exception {
        String indexNode = startIndexNode();
        ensureStableCluster(2);

        final String indexName = "test";
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        final ShardId shardId = findIndexShard(resolveIndex(indexName), 0).shardId();

        logger.debug("--> creating multiple commits so the object store has several blobs");
        int numFlushes = between(2, 4);
        for (int i = 0; i < numFlushes; i++) {
            indexDocs(indexName, between(5, 20));
            flush(indexName);
        }
        assertThat(findIndexShard(resolveIndex(indexName), 0).docStats().getCount(), greaterThan(0L));

        logger.debug("--> record blobs and primary term before the empty allocation");
        Set<PrimaryTermAndGeneration> blobsBefore = listBlobsTermAndGenerations(shardId);
        assertThat("should have blobs from at least one primary term", blobsBefore.size(), greaterThanOrEqualTo(1));
        long primaryTermBefore = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT)
            .get()
            .getState()
            .metadata()
            .getProject()
            .index(indexName)
            .primaryTerm(0);

        logger.debug("--> disable allocation so the shard won't relocate during shutdown");
        updateClusterSettings(Settings.builder().put("cluster.routing.allocation.enable", "none"));

        logger.debug("--> start a fresh index node before shutting down the old one");
        String newNode = startIndexNode();
        ensureStableCluster(3);

        logger.debug("--> SIGTERM the old index node and stop it");
        clusterAdmin().execute(
            PutShutdownNodeAction.INSTANCE,
            new PutShutdownNodeAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                getNodeId(indexNode),
                SIGTERM,
                "node decommission for test",
                null,
                null,
                TimeValue.timeValueSeconds(30)
            )
        ).actionGet(TimeValue.timeValueSeconds(10));
        internalCluster().stopNode(indexNode);

        logger.debug("--> wait until primary is UNASSIGNED");
        awaitClusterState(s -> s.routingTable().index(indexName).allPrimaryShardsUnassigned());

        logger.debug("--> force allocate_empty_primary on the new node while allocation is still disabled");
        ClusterRerouteUtils.reroute(client(), new AllocateEmptyPrimaryAllocationCommand(indexName, 0, newNode, true));

        logger.debug("--> re-enable allocation");
        updateClusterSettings(Settings.builder().putNull("cluster.routing.allocation.enable"));

        logger.debug("--> wait for the empty primary to be fully started");
        awaitClusterState(s -> s.routingTable().index(indexName).allPrimaryShardsActive());

        logger.debug("--> verify the primary term has advanced");
        long primaryTermAfter = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT)
            .get()
            .getState()
            .metadata()
            .getProject()
            .index(indexName)
            .primaryTerm(0);
        assertThat("primary term should have advanced after allocate_empty_primary", primaryTermAfter, greaterThan(primaryTermBefore));

        logger.debug("--> verify the shard is empty (doc count = 0)");
        assertThat(findIndexShard(resolveIndex(indexName), 0).docStats().getCount(), equalTo(0L));

        logger.debug("--> create new commits under the new primary term");
        int newFlushes = between(1, 3);
        for (int i = 0; i < newFlushes; i++) {
            indexDocs(indexName, between(1, 10));
            flush(indexName);
        }

        logger.debug("--> verify new blobs exist under the new primary term");
        Set<PrimaryTermAndGeneration> blobsAfter = listBlobsTermAndGenerations(shardId);
        Set<Long> termsAfter = blobsAfter.stream().map(PrimaryTermAndGeneration::primaryTerm).collect(Collectors.toSet());
        assertThat("new primary term blobs must exist", termsAfter.contains(primaryTermAfter), is(true));

        // BUG: allocate_empty_primary bypasses markRecoveredBcc so old blobs are never cleaned up. This is a minor bug which we might want
        // to fix. This assertion documents the current (incorrect) behavior; it should be removed once the leak is fixed.
        logger.debug("--> verify that old primary term blobs remain orphaned");
        assertThat(
            "old blobs should remain orphaned since allocate_empty_primary bypasses markRecoveredBcc",
            termsAfter.contains(primaryTermBefore),
            is(true)
        );
    }

    public void testPostRecoveryMerge() throws Exception {
        var indexNode = startIndexNode();
        var indexName = randomIndexName();
        createIndex(indexName, indexSettings(1, 0).put(INDEX_MERGE_ENABLED, false).build());

        final var initialSegmentCount = 20;
        for (int i = 0; i < initialSegmentCount; i++) {
            indexDoc(indexName, Integer.toString(i), "f", randomAlphaOfLength(10));
            refresh(indexName); // force a one-doc segment
        }
        flush(indexName); // commit all the one-doc segments

        final LongSupplier searchableSegmentCountSupplier = () -> indicesAdmin().prepareSegments(indexName)
            .get(SAFE_AWAIT_TIMEOUT)
            .getIndices()
            .get(indexName)
            .getShards()
            .get(0)
            .shards()[0].getSegments()
            .stream()
            .filter(Segment::isSearch)
            .count();

        assertEquals(initialSegmentCount, searchableSegmentCountSupplier.getAsLong());

        // Force a recovery by restarting the node, re-enabling merges while the node is down.
        // The delay for post merge recovery is large by default so we won't see merges.
        internalCluster().restartNode(indexNode, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                final var request = new UpdateSettingsRequest(Settings.builder().putNull(INDEX_MERGE_ENABLED).build(), indexName);
                request.reopen(true);
                safeGet(indicesAdmin().updateSettings(request));
                return super.onNodeStopped(nodeName);
            }
        });

        ensureGreen(indexName);
        var mergeStats = indicesAdmin().prepareStats(indexName).clear().setMerge(true).get().getIndex(indexName).getShards()[0].getStats()
            .getMerge();
        assertEquals(0, mergeStats.getCurrent());
        assertEquals(0, mergeStats.getTotal());
        assertEquals(initialSegmentCount, searchableSegmentCountSupplier.getAsLong());

        // Restart again but set the delay to zero.
        internalCluster().restartNode(indexNode, new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                return Settings.builder().put(PostRecoveryMerger.POST_RECOVERY_MERGER_DELAY.getKey(), TimeValue.ZERO).build();
            }
        });

        // And now we should see the merge.
        ensureGreen(indexName);
        assertBusy(() -> assertThat(searchableSegmentCountSupplier.getAsLong(), lessThan((long) initialSegmentCount)));
    }

}
