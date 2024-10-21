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
import co.elastic.elasticsearch.stateless.action.NewCommitNotificationResponse;
import co.elastic.elasticsearch.stateless.action.TransportGetVirtualBatchedCompoundCommitChunkAction;
import co.elastic.elasticsearch.stateless.action.TransportNewCommitNotificationAction;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.commits.VirtualBatchedCompoundCommit;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.engine.SearchEngine;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreTestUtils;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.TransportUnpromotableShardRefreshAction;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TestTransportChannel;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.shutdown.ShutdownPlugin;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.oneOf;

public class SearchShardRecoveryIT extends AbstractStatelessIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.concatLists(List.of(MockRepository.Plugin.class, ShutdownPlugin.class), super.nodePlugins());
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK);
    }

    @Before
    public void init() {
        startMasterOnlyNode();
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
        startIndexNode(
            Settings.builder()
                .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 10)
                .put(disableIndexingDiskAndMemoryControllersNodeSettings())
                .build()
        );

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

    public void testNewCommitNotificationOfRecoveringSearchShard() throws Exception {
        String indexNode = startIndexNode(disableIndexingDiskAndMemoryControllersNodeSettings());
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
        ObjectStoreService objectStoreService = getObjectStoreService(searchNode);
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
        CountDownLatch newCommitNotificationReceived = new CountDownLatch(2);
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
            assertEquals(totalDocs, searchResponse.getHits().getTotalHits().value());
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

        ObjectStoreService objectStoreService = getObjectStoreService(searchNode);
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
            assertEquals(totalDocs, searchResponse.getHits().getTotalHits().value());
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

        ObjectStoreService objectStoreService = getObjectStoreService(searchNode);
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

}
