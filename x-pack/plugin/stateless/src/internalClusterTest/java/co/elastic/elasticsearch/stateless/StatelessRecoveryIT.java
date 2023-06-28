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

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TestTransportChannel;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.oneOf;

public class StatelessRecoveryIT extends AbstractStatelessIntegTestCase {

    @Before
    public void init() {
        startMasterOnlyNode();
    }

    private void testTranslogRecovery(boolean heavyIndexing) throws Exception {
        startIndexNodes(2);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettingsWithRandomFastRefresh(1, 0).put(
                IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(),
                new TimeValue(1, TimeUnit.MINUTES)
            ).build()
        );
        ensureGreen(indexName);

        if (heavyIndexing) {
            indexDocuments(indexName); // produces several commits
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

        assertReplicatedTranslogConsistentWithShards();

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
        SearchResponse searchResponse = client().prepareSearch(indexName).setQuery(QueryBuilders.termQuery("custom", "value")).get();
        assertHitCount(searchResponse, customDocs);
    }

    public void testTranslogRecoveryWithHeavyIndexing() throws Exception {
        testTranslogRecovery(true);
    }

    public void testTranslogRecoveryWithLightIndexing() throws Exception {
        testTranslogRecovery(false);
    }

    public void testRelocatingIndexShards() throws Exception {
        final var numShards = randomIntBetween(1, 3);
        final var indexNodes = startIndexNodes(Math.max(2, numShards));

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettingsWithRandomFastRefresh(numShards, 0).build());
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

            final var initialPrimaryTerms = getPrimaryTerms(indexName);

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

                assertArrayEquals(initialPrimaryTerms, getPrimaryTerms(indexName));

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

    private long[] getPrimaryTerms(String indexName) {
        var response = client().admin().cluster().prepareState().get();
        var state = response.getState();

        var indexMetadata = state.metadata().index(indexName);
        long[] primaryTerms = new long[indexMetadata.getNumberOfShards()];
        for (int i = 0; i < primaryTerms.length; i++) {
            primaryTerms[i] = indexMetadata.primaryTerm(i);
        }
        return primaryTerms;
    }

    public void testRecoverIndexingShard() throws Exception {

        var indexingNode1 = startIndexNode();
        startSearchNode();

        var indexName = randomIdentifier();
        createIndex(indexName, indexSettingsWithRandomFastRefresh(1, 1).build());
        ensureGreen(indexName);

        int numDocsRound1 = randomIntBetween(1, 100);
        indexDocs(indexName, numDocsRound1);
        refresh(indexName);

        assertHitCount(client().prepareSearch(indexName).get(), numDocsRound1);

        if (randomBoolean()) {
            internalCluster().restartNode(indexingNode1);
        } else {
            internalCluster().stopNode(indexingNode1);
            startIndexNode(); // replacement node
        }

        ensureGreen(indexName);

        int numDocsRound2 = randomIntBetween(1, 100);
        indexDocs(indexName, numDocsRound2);
        refresh(indexName);
        assertHitCount(client().prepareSearch(indexName).get(), numDocsRound1 + numDocsRound2);
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
        assertHitCount(client().prepareSearch(indexName).get(), numDocs);
        internalCluster().stopNode(searchNode1);

        var searchNode2 = startSearchNode();
        ensureGreen(indexName);
        assertHitCount(client().prepareSearch(indexName).get(), numDocs);
        internalCluster().stopNode(searchNode2);
    }

    public void testRecoverMultipleIndexingShardsWithCoordinatingRetries() throws Exception {
        String firstIndexingShard = startIndexNode();
        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettingsWithRandomFastRefresh(1, 0).build());

        ensureGreen(indexName);

        MockTransportService transportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            firstIndexingShard
        );

        transportService.addRequestHandlingBehavior(
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

        SearchResponse searchResponse = client().prepareSearch(indexName).setQuery(QueryBuilders.termQuery("custom", "value")).get();
        assertHitCount(searchResponse, 1);
    }

    public void testStartingTranslogFileWrittenInCommit() throws Exception {
        List<String> indexNodes = startIndexNodes(1);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettingsWithRandomFastRefresh(1, 0).put(
                IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(),
                new TimeValue(1, TimeUnit.MINUTES)
            ).build()
        );
        ensureGreen(indexName);

        indexDocuments(indexName);

        assertReplicatedTranslogConsistentWithShards();

        var objectStoreService = internalCluster().getInstance(ObjectStoreService.class, indexNodes.get(0));
        Map<String, BlobMetadata> translogFiles = objectStoreService.getTranslogBlobContainer().listBlobs();

        final String newIndex = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            newIndex,
            indexSettingsWithRandomFastRefresh(1, 0).put(
                IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(),
                new TimeValue(1, TimeUnit.MINUTES)
            ).build()
        );
        ensureGreen(newIndex);

        Index index = resolveIndex(newIndex);
        IndexShard indexShard = findShard(index, 0, DiscoveryNodeRole.INDEX_ROLE, ShardRouting.Role.INDEX_ONLY);
        var blobContainerForCommit = objectStoreService.getBlobContainer(indexShard.shardId(), indexShard.getOperationPrimaryTerm());
        String commitFile = StatelessCompoundCommit.NAME + Lucene.readSegmentInfos(indexShard.store().directory()).getGeneration();
        assertThat(commitFile, blobContainerForCommit.blobExists(commitFile), is(true));
        StatelessCompoundCommit commit = StatelessCompoundCommit.readFromStore(
            new InputStreamStreamInput(blobContainerForCommit.readBlob(commitFile)),
            blobContainerForCommit.listBlobs().get(commitFile).length()
        );

        long initialRecoveryCommitStartingFile = commit.translogRecoveryStartFile();

        // Greater than or equal to because translog files start at 0
        assertThat(initialRecoveryCommitStartingFile, greaterThanOrEqualTo((long) translogFiles.size()));

        indexDocs(newIndex, randomIntBetween(1, 5));

        flush(newIndex);

        commitFile = StatelessCompoundCommit.NAME + Lucene.readSegmentInfos(indexShard.store().directory()).getGeneration();
        assertThat(commitFile, blobContainerForCommit.blobExists(commitFile), is(true));
        commit = StatelessCompoundCommit.readFromStore(
            new InputStreamStreamInput(blobContainerForCommit.readBlob(commitFile)),
            blobContainerForCommit.listBlobs().get(commitFile).length()
        );

        // Recovery file has advanced because of flush
        assertThat(commit.translogRecoveryStartFile(), greaterThan(initialRecoveryCommitStartingFile));
    }
}
