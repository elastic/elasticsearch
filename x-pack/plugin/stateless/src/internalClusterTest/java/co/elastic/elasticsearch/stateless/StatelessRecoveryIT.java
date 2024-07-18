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

import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreTestUtils;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
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
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.xpack.shutdown.PutShutdownNodeAction;
import org.elasticsearch.xpack.shutdown.ShutdownPlugin;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;

import static co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit.blobNameFromGeneration;
import static java.util.stream.Collectors.toList;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_INTERVAL_SETTING;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_INTERVAL_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_RETRY_COUNT_SETTING;
import static org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.Type.SIGTERM;
import static org.elasticsearch.discovery.PeerFinder.DISCOVERY_FIND_PEERS_INTERVAL_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class StatelessRecoveryIT extends AbstractStatelessIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.concatLists(List.of(MockRepository.Plugin.class, ShutdownPlugin.class), super.nodePlugins());
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
}
