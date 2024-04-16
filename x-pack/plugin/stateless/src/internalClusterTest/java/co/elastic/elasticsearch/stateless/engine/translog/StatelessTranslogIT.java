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

package co.elastic.elasticsearch.stateless.engine.translog;

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.IndexingDiskController;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class StatelessTranslogIT extends AbstractStatelessIntegTestCase {

    public void testTranslogFileHoldDirectoryOfReferencedFiles() throws Exception {
        startMasterOnlyNode();

        String indexNode = startIndexNode(
            Settings.builder().put(IndexingDiskController.INDEXING_DISK_INTERVAL_TIME_SETTING.getKey(), TimeValue.timeValueHours(1)).build()
        );
        ensureStableCluster(2);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE).build());
        ShardId shardId = new ShardId(resolveIndex(indexName), 0);
        ensureGreen(indexName);

        final int iters = randomIntBetween(1, 10);
        for (int i = 0; i < iters; i++) {
            indexDocs(indexName, randomIntBetween(1, 20));
        }

        var translogReplicator = internalCluster().getInstance(TranslogReplicator.class, indexNode);

        Set<TranslogReplicator.BlobTranslogFile> firstActiveTranslogFiles = translogReplicator.getActiveTranslogFiles();
        int firstFileCount = firstActiveTranslogFiles.size();
        assertThat(firstFileCount, greaterThan(0));

        var indexObjectStoreService = internalCluster().getInstance(ObjectStoreService.class, indexNode);
        BlobContainer translogBlobContainer = indexObjectStoreService.getTranslogBlobContainer();
        assertTranslogBlobsExist(firstActiveTranslogFiles, translogBlobContainer);

        final int iters2 = randomIntBetween(1, 10);
        for (int i = 0; i < iters2; i++) {
            indexDocs(indexName, randomIntBetween(1, 20));
        }

        Set<TranslogReplicator.BlobTranslogFile> secondActiveTranslogFiles = translogReplicator.getActiveTranslogFiles();
        assertThat(secondActiveTranslogFiles.size(), greaterThan(firstFileCount));
        assertTranslogBlobsExist(secondActiveTranslogFiles, translogBlobContainer);

        List<BlobMetadata> blobs = translogBlobContainer.listBlobs(operationPurpose)
            .entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .map(Map.Entry::getValue)
            .toList();

        assertDirectoryConsistency(blobs, translogBlobContainer, shardId);
    }

    public void testTranslogFileHoldDirectoryForIdleShards() throws Exception {
        startMasterOnlyNode();

        String indexNode = startIndexNode(
            Settings.builder().put(IndexingDiskController.INDEXING_DISK_INTERVAL_TIME_SETTING.getKey(), TimeValue.timeValueHours(1)).build()
        );
        ensureStableCluster(2);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1L)).build()
        );
        final String idleIndex = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            idleIndex,
            indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueHours(1L)).build()
        );
        ShardId shardId = new ShardId(resolveIndex(indexName), 0);
        ShardId idleShardId = new ShardId(resolveIndex(idleIndex), 0);

        ensureGreen(indexName, idleIndex);

        final int iters = randomIntBetween(1, 10);
        for (int i = 0; i < iters; i++) {
            indexDocs(indexName, randomIntBetween(1, 20));
            indexDocs(idleIndex, randomIntBetween(1, 20));
        }

        var translogReplicator = internalCluster().getInstance(TranslogReplicator.class, indexNode);

        Set<TranslogReplicator.BlobTranslogFile> firstActiveTranslogFiles = translogReplicator.getActiveTranslogFiles();
        int firstFileCount = firstActiveTranslogFiles.size();
        long maxUploadedFileAfterFirstIndex = translogReplicator.getMaxUploadedFile();
        assertThat(firstFileCount, greaterThan(0));

        var indexObjectStoreService = internalCluster().getInstance(ObjectStoreService.class, indexNode);
        BlobContainer translogBlobContainer = indexObjectStoreService.getTranslogBlobContainer();
        assertTranslogBlobsExist(firstActiveTranslogFiles, translogBlobContainer);

        final int iters2 = randomIntBetween(1, 10);
        for (int i = 0; i < iters2; i++) {
            indexDocs(indexName, randomIntBetween(1, 20));
        }

        Set<TranslogReplicator.BlobTranslogFile> secondActiveTranslogFiles = translogReplicator.getActiveTranslogFiles();
        assertThat(secondActiveTranslogFiles.size(), greaterThan(firstFileCount));
        assertTranslogBlobsExist(secondActiveTranslogFiles, translogBlobContainer);

        List<BlobMetadata> blobs = translogBlobContainer.listBlobs(operationPurpose)
            .entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .map(Map.Entry::getValue)
            .toList();

        assertDirectoryConsistency(blobs, translogBlobContainer, shardId);
        assertDirectoryConsistency(blobs, translogBlobContainer, idleShardId);
        BlobMetadata lastBlob = blobs.get(blobs.size() - 1);
        try (StreamInput streamInput = new InputStreamStreamInput(translogBlobContainer.readBlob(operationPurpose, lastBlob.name()))) {
            long generation = Long.parseLong(lastBlob.name());
            CompoundTranslogHeader header = CompoundTranslogHeader.readFromStore(lastBlob.name(), streamInput);
            TranslogMetadata metadata = header.metadata().get(idleShardId);
            long maxReferenced = Arrays.stream(metadata.directory().referencedTranslogFileOffsets())
                .mapToLong(r -> generation - r)
                .max()
                .getAsLong();
            assertThat(maxReferenced, lessThanOrEqualTo(maxUploadedFileAfterFirstIndex));
        }
    }

    public void testTranslogFileHoldDirectoryReflectsWhenFilesPruned() throws Exception {
        startMasterOnlyNode();

        String indexNode = startIndexNode(
            Settings.builder().put(IndexingDiskController.INDEXING_DISK_INTERVAL_TIME_SETTING.getKey(), TimeValue.timeValueHours(1)).build()
        );
        ensureStableCluster(2);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE).build());
        ShardId shardId = new ShardId(resolveIndex(indexName), 0);
        ensureGreen(indexName);

        final int iters = randomIntBetween(1, 10);
        for (int i = 0; i < iters; i++) {
            indexDocs(indexName, randomIntBetween(1, 20));
        }

        var translogReplicator = internalCluster().getInstance(TranslogReplicator.class, indexNode);

        Set<TranslogReplicator.BlobTranslogFile> firstActiveTranslogFiles = translogReplicator.getActiveTranslogFiles();
        int firstFileCount = firstActiveTranslogFiles.size();
        assertThat(firstFileCount, greaterThan(0));

        var indexObjectStoreService = internalCluster().getInstance(ObjectStoreService.class, indexNode);
        BlobContainer translogBlobContainer = indexObjectStoreService.getTranslogBlobContainer();
        assertTranslogBlobsExist(firstActiveTranslogFiles, translogBlobContainer);

        flush(indexName);

        assertBusy(() -> assertThat(translogReplicator.getActiveTranslogFiles().size(), equalTo(0)));
        assertBusy(() -> assertTrue(translogBlobContainer.listBlobs(operationPurpose).isEmpty()));

        long minReferencedFile = translogReplicator.getMaxUploadedFile() + 1;
        indexDocs(indexName, randomIntBetween(1, 20));
        long maxUploadedFile = translogReplicator.getMaxUploadedFile();

        final int iters2 = randomIntBetween(1, 10);
        for (int i = 0; i < iters2; i++) {
            indexDocs(indexName, randomIntBetween(1, 20));
        }

        Set<TranslogReplicator.BlobTranslogFile> secondActiveTranslogFiles = translogReplicator.getActiveTranslogFiles();
        assertThat(translogReplicator.getMaxUploadedFile(), greaterThan(maxUploadedFile));
        assertTranslogBlobsExist(secondActiveTranslogFiles, translogBlobContainer);

        List<BlobMetadata> blobs = translogBlobContainer.listBlobs(operationPurpose)
            .entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .map(Map.Entry::getValue)
            .toList();

        assertDirectoryConsistency(blobs, translogBlobContainer, shardId);
        BlobMetadata lastBlob = blobs.get(blobs.size() - 1);
        try (StreamInput streamInput = new InputStreamStreamInput(translogBlobContainer.readBlob(operationPurpose, lastBlob.name()))) {
            long generation = Long.parseLong(lastBlob.name());
            CompoundTranslogHeader header = CompoundTranslogHeader.readFromStore(lastBlob.name(), streamInput);
            TranslogMetadata metadata = header.metadata().get(shardId);
            long minReferenced = Arrays.stream(metadata.directory().referencedTranslogFileOffsets())
                .mapToLong(r -> generation - r)
                .min()
                .getAsLong();
            assertThat(minReferenced, equalTo(minReferencedFile));
        }
    }

    @TestLogging(
        value = "co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicator:debug,"
            + "co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicatorReader:debug",
        reason = "to ensure we translog events on DEBUG level"
    )
    public void testTranslogStressRecoveryTest() throws Exception {
        // TODO: Add Failures.REMOTE_FAIL once issues with remote failure have been resolved
        runStressTest(4, Failures.RESTART, Failures.LOCAL_FAIL);
    }

    @TestLogging(
        value = "co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicator:debug,"
            + "co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicatorReader:debug",
        reason = "to ensure we translog events on DEBUG level"
    )
    public void testTranslogRestartOnlyStressRecoveryTest() throws Exception {
        // Restarts take the longest so lower failure count
        runStressTest(2, Failures.RESTART);
    }

    // TODO: Uncomment once issues with remote failure have been resolved
    // @TestLogging(
    // value = "co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicator:debug,"
    // + "co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicatorReader:debug",
    // reason = "to ensure we translog events on DEBUG level"
    // )
    // public void testTranslogRemoteFailureOnlyStressRecoveryTest() throws Exception {
    // runStressTest(3, Failures.REMOTE_FAIL);
    // }

    @TestLogging(
        value = "co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicator:debug,"
            + "co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicatorReader:debug",
        reason = "to ensure we translog events on DEBUG level"
    )
    public void testTranslogLocalFailureOnlyStressRecoveryTest() throws Exception {
        runStressTest(3, Failures.LOCAL_FAIL);
    }

    private void runStressTest(int failureCount, Failures... failureTypes) throws Exception {
        TimeValue flushInterval = TimeValue.timeValueMillis(rarely() ? 200 : randomLongBetween(25, 100));
        logger.info("running test with translog flush interval {}", flushInterval);

        startMasterOnlyNode();
        startSearchNode();

        String indexNode1 = startMasterAndIndexNode(
            Settings.builder()
                .put(TranslogReplicator.FLUSH_INTERVAL_SETTING.getKey(), flushInterval)
                .put(IndexingDiskController.INDEXING_DISK_INTERVAL_TIME_SETTING.getKey(), TimeValue.timeValueHours(1))
                .build()
        );
        String indexNode2 = startMasterAndIndexNode(
            Settings.builder()
                .put(TranslogReplicator.FLUSH_INTERVAL_SETTING.getKey(), flushInterval)
                .put(IndexingDiskController.INDEXING_DISK_INTERVAL_TIME_SETTING.getKey(), TimeValue.timeValueHours(1))
                .build()
        );
        ensureStableCluster(4);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(4, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE).build());
        ensureGreen(indexName);

        AtomicLong minExpectedDocs = new AtomicLong();

        ArrayList<CountDownLatch> failureLatches = new ArrayList<>(failureCount);
        int reqN = 0;
        for (int i = 0; i < failureCount; i++) {
            int failThreshold = randomIntBetween(30, 60);
            failureLatches.add(new CountDownLatch(reqN + failThreshold));
            reqN += failThreshold;
        }
        reqN += randomIntBetween(30, 60);

        ExecutorService executorService = Executors.newFixedThreadPool(4);
        CountDownLatch allReqLatch = new CountDownLatch(reqN);
        Set<String> successes = ConcurrentCollections.newConcurrentSet();
        Set<String> failures = ConcurrentCollections.newConcurrentSet();

        logger.info("running test with {} requests", reqN);

        for (int n = 0; n < reqN; ++n) {
            executorService.execute(() -> {
                try {
                    var bulkRequest = client().prepareBulk();
                    // Occasionally set timeout to avoid retries
                    if (randomBoolean() && randomBoolean()) {
                        bulkRequest.setTimeout(TimeValue.timeValueMillis(100));
                    }
                    int numDocs = randomIntBetween(10, 50);
                    for (int i = 0; i < numDocs; i++) {
                        bulkRequest.add(new IndexRequest(indexName).source("field", randomUnicodeOfCodepointLengthBetween(1, 25)));
                    }
                    var bulkResponse = bulkRequest.get();

                    Arrays.stream(bulkResponse.getItems()).forEach(bulkItemResponse -> {
                        if (bulkItemResponse.isFailed()) {
                            failures.add(bulkItemResponse.getId());
                        } else {
                            minExpectedDocs.incrementAndGet();
                            successes.add(bulkItemResponse.getId());
                        }
                    });
                } catch (Exception e) {
                    logger.warn("exception on indexing thread", e);
                } finally {
                    failureLatches.forEach(CountDownLatch::countDown);
                }

                try {
                    // Flush approximately every 32 requests
                    if (randomBoolean() && randomBoolean() && randomBoolean() && randomBoolean() && randomBoolean()) {
                        // Do not use the test flush() helper method as it can trigger an assertion if a flush fails due to a node restart.
                        indicesAdmin().prepareFlush(indexName).get();
                    }
                } catch (Exception e) {
                    logger.warn("exception while flushing on indexing thread", e);
                } finally {
                    allReqLatch.countDown();
                }
            });
        }

        try {
            for (CountDownLatch latch : failureLatches) {
                safeAwait(latch);
                induceFailures(indexNode1, indexNode2, indexName, failureTypes);
            }

            safeAwait(allReqLatch);

            refresh(indexName);

            SearchResponse response = prepareSearch(indexName).setQuery(QueryBuilders.idsQuery().addIds(failures.toArray(new String[0])))
                .get();
            long failureHits = response.getHits().getTotalHits().value;
            response.decRef();
            logger.info(
                "Found ["
                    + failureHits
                    + "] hits matching failure ids. This is not an error condition as docs can be accepted by the system and still be "
                    + "indicated as failed to the client."
            );

            assertResponse(prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()), searchResponse -> {
                assertNoFailures(searchResponse);
                assertThat(searchResponse.getHits().getTotalHits().value, greaterThanOrEqualTo(minExpectedDocs.get()));
            });

            assertResponse(
                prepareSearch(indexName).setQuery(QueryBuilders.idsQuery().addIds(successes.toArray(new String[0]))),
                searchResponse -> {
                    assertNoFailures(searchResponse);
                    assertThat(searchResponse.getHits().getTotalHits().value, equalTo((long) successes.size()));
                }
            );
        } finally {
            long outstandingRequests = allReqLatch.getCount();
            if (outstandingRequests > 0) {
                logger.warn("test finished with {} outstanding requests", outstandingRequests);
            }

            executorService.shutdown();
            executorService.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    private enum Failures {
        RESTART,
        LOCAL_FAIL,
        REMOTE_FAIL
    }

    private void induceFailures(String indexNode1, String indexNode2, String indexName, Failures... failureTypes) throws Exception {
        Failures failure = randomFrom(failureTypes);
        logger.info("inducing failure of type: {}", failure.name());
        switch (failure) {
            case RESTART -> {
                internalCluster().restartNode(randomFrom(indexNode1, indexNode2));
                ensureStableCluster(4);
            }
            case LOCAL_FAIL -> {
                IndexShard indexShard = findIndexShard(resolveIndex(indexName), randomFrom(0, 1, 2, 3));
                indexShard.failShard("broken", new Exception("boom local"));
            }
            case REMOTE_FAIL -> {
                IndexShard indexShard = findIndexShard(resolveIndex(indexName), randomFrom(0, 1, 2, 3));
                ShardStateAction shardStateAction = internalCluster().getInstance(
                    ShardStateAction.class,
                    randomFrom(internalCluster().getMasterName(), indexNode1, indexNode2)
                );
                ShardRouting shardRouting = indexShard.routingEntry();
                PlainActionFuture<Void> listener = new PlainActionFuture<>();
                shardStateAction.remoteShardFailed(
                    indexShard.shardId(),
                    shardRouting.allocationId().getId(),
                    indexShard.getOperationPrimaryTerm(),
                    true,
                    "broken",
                    new Exception("boom remote"),
                    listener
                );
                listener.actionGet();
            }
        }
        ensureGreen(indexName);
    }

    private static void assertDirectoryConsistency(List<BlobMetadata> blobs, BlobContainer translogBlobContainer, ShardId shardId)
        throws IOException {
        long totalOps = 0;
        HashSet<Long> referencedFiles = new HashSet<>();
        for (BlobMetadata blob : blobs) {
            long generation = Long.parseLong(blob.name());
            try (StreamInput streamInput = new InputStreamStreamInput(translogBlobContainer.readBlob(operationPurpose, blob.name()))) {
                CompoundTranslogHeader header = CompoundTranslogHeader.readFromStore(blob.name(), streamInput);
                TranslogMetadata metadata = header.metadata().get(shardId);
                totalOps += metadata.totalOps();
                assertThat(metadata.directory().estimatedOperationsToRecover(), equalTo(totalOps));
                Set<Long> actualReferenced = Arrays.stream(metadata.directory().referencedTranslogFileOffsets())
                    .mapToLong(r -> generation - r)
                    .boxed()
                    .collect(Collectors.toSet());

                assertThat(actualReferenced, equalTo(referencedFiles));
                if (metadata.totalOps() > 0) {
                    referencedFiles.add(generation);
                }
            }
        }
    }

    private static void assertTranslogBlobsExist(Set<TranslogReplicator.BlobTranslogFile> shouldExist, BlobContainer container)
        throws IOException {
        for (TranslogReplicator.BlobTranslogFile translogFile : shouldExist) {
            assertTrue(container.blobExists(operationPurpose, translogFile.blobName()));
        }
    }

}
