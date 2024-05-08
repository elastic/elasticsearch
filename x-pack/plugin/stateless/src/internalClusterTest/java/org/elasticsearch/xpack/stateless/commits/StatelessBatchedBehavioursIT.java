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

package co.elastic.elasticsearch.stateless.commits;

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.StatelessMockRepositoryPlugin;
import co.elastic.elasticsearch.stateless.StatelessMockRepositoryStrategy;
import co.elastic.elasticsearch.stateless.TestStateless;
import co.elastic.elasticsearch.stateless.action.NewCommitNotificationRequest;
import co.elastic.elasticsearch.stateless.action.TransportNewCommitNotificationAction;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.UnavailableShardsException;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.LiveVersionMap;
import org.elasticsearch.index.engine.LiveVersionMapTestUtils;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.transport.MockTransportService;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertExists;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class StatelessBatchedBehavioursIT extends AbstractStatelessIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(Stateless.class);
        plugins.add(TestStateless.class);
        plugins.add(StatelessMockRepositoryPlugin.class);
        return List.copyOf(plugins);
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings()
            // Need to set the ObjectStoreType to MOCK for the StatelessMockRepository plugin.
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK.toString().toLowerCase(Locale.ROOT));
    }

    public void testGenerationalFileBlobReferenceIsRetainedCorrectly() throws Exception {
        final String indexNode = startMasterAndIndexNode(
            Settings.builder()
                .put(StatelessCommitService.STATELESS_UPLOAD_DELAYED.getKey(), true)
                .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 2)
                .build()
        );
        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build());
        ensureGreen(indexName);

        final List<String> docIds = IntStream.range(0, 8).mapToObj(i -> "doc-" + i).toList();

        for (String docId : docIds) {
            client().prepareIndex(indexName)
                .setId(docId)
                .setSource("field", randomUnicodeOfLength(50))
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get(TimeValue.timeValueSeconds(10));
        }

        for (String docId : docIds) {
            client().prepareDelete(indexName, docId)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get(TimeValue.timeValueSeconds(10));
        }

        // Make sure the delayed release actually happen for deleting the old commit files
        forceMerge();
        // speed up commit deletion since it uses delayed cluster consistency check from translog
        indexDoc(indexName, "doc-extra", "field", randomUnicodeOfLength(50));

        final ObjectStoreService objectStoreService = internalCluster().getInstance(ObjectStoreService.class, indexNode);
        final IndexShard indexShard = findIndexShard(indexName);
        final Engine indexEngine = indexShard.getEngineOrNull();
        final long generation = indexEngine.getLastCommittedSegmentInfos().getGeneration();
        final BlobContainer blobContainer = objectStoreService.getBlobContainer(indexShard.shardId(), indexShard.getOperationPrimaryTerm());
        assertBusy(() -> {
            final Set<String> blobFileNames = blobContainer.listBlobs(OperationPurpose.INDICES).keySet();
            assertThat(blobFileNames, equalTo(Set.of(StatelessCompoundCommit.blobNameFromGeneration(generation))));
        });
    }

    public void testFlushAfterRelocationWillThrowOnlyExpectedError() throws Exception {
        startMasterOnlyNode();
        final String oldIndexNode = startIndexNode();
        ensureStableCluster(2);

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build());
        ensureGreen(indexName);

        final IndexShard indexShard = findIndexShard(indexName);
        final IndexEngine indexEngine = (IndexEngine) indexShard.getEngineOrNull();
        final var statelessCommitService = (TestStatelessCommitService) indexEngine.getStatelessCommitService();
        final CyclicBarrier afterRelocatedBarrier = new CyclicBarrier(2);
        statelessCommitService.setStrategy(new TestStatelessCommitService.Strategy() {
            @Override
            public ActionListener<Void> markRelocating(
                Supplier<ActionListener<Void>> originalSupplier,
                ShardId shardId,
                long minRelocatedGeneration,
                ActionListener<Void> listener
            ) {
                return originalSupplier.get().delegateFailure((l, ignore) -> {
                    l.onResponse(null); // mark relocated
                    safeAwait(afterRelocatedBarrier);
                    safeAwait(afterRelocatedBarrier);
                });
            }
        });

        logger.info("--> indexing docs and refresh");
        indexDocs(indexName, 10);
        refresh(indexName);
        indexDocs(indexName, 1);

        final String newIndexNode = startIndexNode();
        logger.info("--> relocating index shard into {}", newIndexNode);
        updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", newIndexNode), indexName);

        safeAwait(afterRelocatedBarrier);
        logger.info("--> old primary relocated, starting post relocation flush");
        try {
            for (int i = 0; i < 2; i++) {
                final var e = expectThrows(UnavailableShardsException.class, () -> indexEngine.flush(true, true));
                assertThat(e.getMessage(), containsString("shard relocated"));
            }
        } finally {
            safeAwait(afterRelocatedBarrier);
        }
    }

    public void testDefaultToNotifyOnlyForUpload() {
        assumeFalse("skip test because stateless.upload.delayed is enabled", STATELESS_UPLOAD_DELAYED);

        final String indexNode = startMasterAndIndexNode();
        startSearchNode();

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build());
        ensureGreen(indexName);

        final MockTransportService indexTransportService = MockTransportService.getInstance(indexNode);
        final var requestRef = new AtomicReference<NewCommitNotificationRequest>();
        indexTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(TransportNewCommitNotificationAction.NAME + "[u]")) {
                final boolean success = requestRef.compareAndSet(null, (NewCommitNotificationRequest) request);
                assertThat("expect null requestRef, but got " + requestRef.get(), success, is(true));
            }
            connection.sendRequest(requestId, action, request, options);
        });

        final int numberOfRuns = between(1, 5);
        for (int i = 0; i < numberOfRuns; i++) {
            indexDocs(indexName, randomIntBetween(1, 100));
            refresh(indexName);
            final NewCommitNotificationRequest request = requestRef.getAndSet(null);
            assertThat(request, notNullValue());
            assertThat(request.isUploaded(), is(true));
        }
    }

    public void testIndexCanBeCleanedUpAfterTest() {
        startMasterAndIndexNode(
            Settings.builder()
                .put(StatelessCommitService.STATELESS_UPLOAD_DELAYED.getKey(), true)
                .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 10)
                .build()
        );
        startSearchNode();

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build());
        ensureGreen(indexName);
        indexDocs(indexName, randomIntBetween(10, 100));
        refresh(indexName);

        final String anotherIndexName = randomIdentifier();
        createIndex(anotherIndexName, indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build());
        ensureGreen(anotherIndexName);
        indexDocs(anotherIndexName, randomIntBetween(10, 100));
        refresh(anotherIndexName);
        // test should not fail in clean up because of the non-flushed commit
    }

    public void testBCCDoesNotHoldHoldCommitReferencesAfterIndexShardClose() throws Exception {
        final String indexNode = startMasterAndIndexNode(
            Settings.builder()
                .put(StatelessCommitService.STATELESS_UPLOAD_DELAYED.getKey(), true)
                .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 10)
                .build()
        );
        startSearchNode();

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build());
        ensureGreen(indexName);

        IndexShard indexShard = findIndexShard(indexName);
        long beforeIndexing = Lucene.readSegmentInfos(indexShard.store().directory()).getGeneration();

        indexDocs(indexName, randomIntBetween(10, 100));
        refresh(indexName);

        assertThat(Lucene.readSegmentInfos(indexShard.store().directory()).getGeneration(), greaterThan(beforeIndexing));

        assertAcked(client().admin().indices().prepareDelete(indexName));

        // This test would hang on close if the un-uploaded index commit references were not released on index shard close.
    }

    public void testNewCommitNotificationOnCreation() throws Exception {
        final String indexNode = startMasterAndIndexNode(
            Settings.builder().put(StatelessCommitService.STATELESS_UPLOAD_DELAYED.getKey(), true).build()
        );
        startSearchNode();

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build());
        ensureGreen(indexName);

        final MockTransportService indexTransportService = MockTransportService.getInstance(indexNode);
        final Queue<NewCommitNotificationRequest> requests = ConcurrentCollections.newQueue();
        indexTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(TransportNewCommitNotificationAction.NAME + "[u]")) {
                requests.add((NewCommitNotificationRequest) request);
            }
            connection.sendRequest(requestId, action, request, options);
        });

        final var indexShard = findIndexShard(indexName);
        // First empty commit is always uploaded
        final AtomicLong latestUploadedGeneration = new AtomicLong(
            indexShard.getEngineOrNull().getLastCommittedSegmentInfos().getGeneration()
        );
        final int numberOfRuns = between(1, 5);
        for (int i = 1; i <= numberOfRuns; i++) {
            indexDocs(indexName, randomIntBetween(1, 100));
            refresh(indexName);
            final boolean isUpload = i % STATELESS_UPLOAD_MAX_COMMITS == 0;
            assertBusy(() -> {
                final List<NewCommitNotificationRequest> requestList = List.copyOf(requests);
                assertThat(requestList.size(), equalTo(1 + (isUpload ? 1 : 0)));

                final var creationNotificationRequest = requestList.get(0);
                assertThat(creationNotificationRequest.isUploaded(), is(false));
                assertThat(creationNotificationRequest.getBatchedCompoundCommitGeneration(), equalTo(latestUploadedGeneration.get() + 1L));
                assertThat(
                    creationNotificationRequest.getLatestUploadedBatchedCompoundCommitTermAndGen().generation(),
                    equalTo(latestUploadedGeneration.get())
                );

                if (isUpload) {
                    final var uploadNotificationRequest = requestList.get(1);
                    assertThat(uploadNotificationRequest.isUploaded(), is(true));
                    assertThat(creationNotificationRequest.getCompoundCommit(), equalTo(uploadNotificationRequest.getCompoundCommit()));
                    assertThat(
                        creationNotificationRequest.getBatchedCompoundCommitGeneration(),
                        equalTo(uploadNotificationRequest.getBatchedCompoundCommitGeneration())
                    );
                    assertThat(
                        uploadNotificationRequest.getLatestUploadedBatchedCompoundCommitTermAndGen().generation(),
                        equalTo(uploadNotificationRequest.getBatchedCompoundCommitGeneration())
                    );
                    latestUploadedGeneration.set(uploadNotificationRequest.getBatchedCompoundCommitGeneration());
                }

                requests.clear();
            });
        }
    }

    public void testRefreshAndSearchWorksWithoutUpload() throws Exception {
        final AtomicBoolean shouldBlock = new AtomicBoolean(false);
        final CountDownLatch latch = new CountDownLatch(1);
        final String indexNode = startMasterAndIndexNode(
            Settings.builder().put(StatelessCommitService.STATELESS_UPLOAD_DELAYED.getKey(), true).build(),
            new StatelessMockRepositoryStrategy() {
                @Override
                public void blobContainerWriteMetadataBlob(
                    CheckedRunnable<IOException> original,
                    OperationPurpose purpose,
                    String blobName,
                    boolean failIfAlreadyExists,
                    boolean atomic,
                    CheckedConsumer<OutputStream, IOException> writer
                ) throws IOException {
                    if (shouldBlock.get() && StatelessCompoundCommit.startsWithBlobPrefix(blobName)) {
                        safeAwait(latch);
                    }
                    super.blobContainerWriteMetadataBlob(original, purpose, blobName, failIfAlreadyExists, atomic, writer);
                }
            }
        );
        startSearchNode();

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build());
        ensureGreen(indexName);

        final MockTransportService indexTransportService = MockTransportService.getInstance(indexNode);
        final AtomicBoolean seenUploadNotification = new AtomicBoolean(false);
        indexTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(TransportNewCommitNotificationAction.NAME + "[u]")) {
                if (((NewCommitNotificationRequest) request).isUploaded()) {
                    seenUploadNotification.set(true);
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });

        shouldBlock.set(true);
        int expectedTotalHits = 0;
        final int numberOfRuns = between(1, 5);
        for (int i = 0; i < numberOfRuns; i++) {
            final int numDocs = randomIntBetween(1, 10);
            indexDocs(indexName, numDocs);
            refresh(indexName);
            expectedTotalHits += numDocs;
            assertHitCount(prepareSearch(indexName).setTrackTotalHits(true), expectedTotalHits);
            assertThat(seenUploadNotification.get(), is(false));
        }

        // LiveVersionMapArchive should be cleared by non-upload notifications
        final String docId = randomIdentifier();
        indexDoc(indexName, docId, "field", randomUnicodeOfLength(10));

        final var indicesService = internalCluster().getInstance(IndicesService.class, indexNode);
        final var shardId = new ShardId(resolveIndex(indexName), 0);
        final var indexService = indicesService.indexServiceSafe(shardId.getIndex());
        final var indexShard = indexService.getShard(shardId.id());
        final var indexEngine = ((IndexEngine) indexShard.getEngineOrNull());
        final LiveVersionMap liveVersionMap = indexEngine.getLiveVersionMap();

        assertDocumentExists(indexName, docId);
        assertThat(LiveVersionMapTestUtils.get(liveVersionMap, docId), notNullValue());
        refresh(indexName);
        assertDocumentExists(indexName, docId);

        refresh(indexName); // LiveVersionMapArchive requires 2 refreshes to clear out
        assertBusy(() -> assertThat(LiveVersionMapTestUtils.get(liveVersionMap, docId), nullValue()));
        assertDocumentExists(indexName, docId);
        assertThat(seenUploadNotification.get(), is(false));

        // Unblock and ready to shutdown correctly
        latch.countDown();
    }

    private void assertDocumentExists(String indexName, String docId) {
        if (randomBoolean()) {
            assertResponse(client().prepareMultiGet().add(indexName, docId).setRealtime(true), response -> {
                assertThat(response.getResponses(), arrayWithSize(1));
                assertThat(response.getResponses()[0].getId(), equalTo(docId));
            });
        } else {
            assertExists(client().prepareGet(indexName, docId).setRealtime(true).get(TimeValue.timeValueSeconds(10)));
        }
    }
}
