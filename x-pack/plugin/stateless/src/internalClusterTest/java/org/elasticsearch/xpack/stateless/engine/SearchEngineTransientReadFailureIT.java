/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.engine;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.lucene.index.CorruptIndexException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.action.GetVirtualBatchedCompoundCommitChunkRequest;
import org.elasticsearch.xpack.stateless.action.TransportGetVirtualBatchedCompoundCommitChunkAction;
import org.elasticsearch.xpack.stateless.action.TransportNewCommitNotificationAction;
import org.elasticsearch.xpack.stateless.cache.SearchCommitPrefetcherDynamicSettings;
import org.elasticsearch.xpack.stateless.cache.reader.CacheBlobReaderService;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;

import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/// Reproduces a search shard reporting a [CorruptIndexException] ("codec footer mismatch [...] actual footer=0") when its index is deleted
/// while it reads a non-uploaded VBCC from the indexing node.
///
/// Once the index is removed from the indexing node, VBCC chunk requests fail with an `IndexNotFoundException`. Since that is a
/// `ResourceNotFoundException`, the search node assumes that the VBCC was uploaded in the meantime and retries the read from the object
/// store, where the never-uploaded BCC is not found. When that failure happens inside `BlobCacheBufferedIndexInput.refill()`, the buffer is
/// left with a limit covering the requested range but no data. For files that fit in a single buffer (like `.si` files), Lucene's
/// `CodecUtil.checkFooter(in, priorException)` then reads the footer from these stale zeros and throws a [CorruptIndexException], with the
/// real failure only attached as a suppressed exception.
///
/// To get the `.si` file pattern, the search shard processes a commit notification for a commit B that references a
/// segment written by an earlier commit A that the search shard never read, and the reads of commit A's bytes are delayed
/// until the index is deleted on the indexing node.
public class SearchEngineTransientReadFailureIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected Settings.Builder nodeSettings() {
        // keep every commit in a single non-uploaded VBCC so that the search node has to read it from the indexing node
        return super.nodeSettings().put(StatelessCommitService.STATELESS_UPLOAD_MAX_SIZE.getKey(), ByteSizeValue.ofGb(1))
            .put(StatelessCommitService.STATELESS_UPLOAD_VBCC_MAX_AGE.getKey(), TimeValue.timeValueDays(1))
            .put(StatelessCommitService.STATELESS_UPLOAD_MONITOR_INTERVAL.getKey(), TimeValue.timeValueDays(1))
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 1000)
            .put(disableIndexingDiskAndMemoryControllersNodeSettings());
    }

    public void testIndexDeletionDuringVbccReadIsReportedAsCorruption() throws Exception {
        startMasterOnlyNode();
        final var indexNode = startIndexNode();
        final var searchNode = startSearchNode(
            Settings.builder()
                .put(SearchCommitPrefetcherDynamicSettings.PREFETCH_COMMITS_UPON_NOTIFICATIONS_ENABLED_SETTING.getKey(), false)
                .put(SearchCommitPrefetcherDynamicSettings.STATELESS_SEARCH_USE_INTERNAL_FILES_REPLICATED_CONTENT.getKey(), false)
                // use the smallest chunks so that the reads of commit B do not also fetch the bytes of commit A
                .put(CacheBlobReaderService.TRANSPORT_BLOB_READER_CHUNK_SIZE_SETTING.getKey(), ByteSizeValue.ofKb(4))
                .build()
        );
        ensureStableCluster(3);

        final var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);
        indexDocsAndRefresh(indexName, 10);

        final var shardId = findIndexShard(indexName).shardId();
        final var commitService = internalCluster().getInstance(StatelessCommitService.class, indexNode);

        // hold back the commit notification of commit A
        // the handler runs on a transport thread, so the notification is captured and replayed later rather than blocked on
        final var holdNextNotification = new AtomicBoolean(true);
        final var heldNotification = new AtomicReference<CheckedRunnable<Exception>>();
        final var notificationHeld = new CountDownLatch(1);
        final var searchTransportService = MockTransportService.getInstance(searchNode);
        searchTransportService.addRequestHandlingBehavior(
            TransportNewCommitNotificationAction.NAME + "[u]",
            (handler, request, channel, task) -> {
                if (holdNextNotification.compareAndSet(true, false)) {
                    heldNotification.set(() -> handler.messageReceived(request, channel, task));
                    notificationHeld.countDown();
                } else {
                    handler.messageReceived(request, channel, task);
                }
            }
        );

        logger.info("--> creating commit A");
        indexDocs(indexName, 10);
        indicesAdmin().prepareRefresh(indexName).execute();
        safeAwait(notificationHeld);
        final var vbcc = commitService.getCurrentVirtualBcc(shardId);
        assertThat(vbcc, notNullValue());
        final long endOfCommitA = vbcc.getTotalSizeInBytes();
        logger.info("--> commit A ends at offset [{}] in VBCC [{}]", endOfCommitA, vbcc.getPrimaryTermAndGeneration());

        // delay the VBCC chunk requests for the bytes of commit A until the index is deleted on the indexing node
        final List<Runnable> delayedChunkRequests = new CopyOnWriteArrayList<>();
        final var chunkRequestDelayed = new CountDownLatch(1);
        final var releaseChunkRequests = new AtomicBoolean(false);
        searchTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(TransportGetVirtualBatchedCompoundCommitChunkAction.NAME + "[p]") && releaseChunkRequests.get() == false) {
                final var chunkRequest = asInstanceOf(GetVirtualBatchedCompoundCommitChunkRequest.class, request);
                if (chunkRequest.getOffset() < endOfCommitA) {
                    logger.info("--> delaying VBCC chunk request [{}] of length [{}]", chunkRequest.getOffset(), chunkRequest.getLength());
                    delayedChunkRequests.add(() -> {
                        try {
                            connection.sendRequest(requestId, action, request, options);
                        } catch (Exception e) {
                            throw new AssertionError(e);
                        }
                    });
                    chunkRequestDelayed.countDown();
                    return;
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });

        final var searchEngineFailure = new AtomicReference<Throwable>();
        final var searchEngineFailed = new CountDownLatch(1);
        final var searchNodeApplierBlocked = new CountDownLatch(1);
        final var unblockSearchNodeApplier = new CountDownLatch(1);
        try (var mockLog = MockLog.capture(Engine.class)) {
            mockLog.addExpectation(new MockLog.LoggingExpectation() {
                @Override
                public void match(LogEvent event) {
                    if (event.getLevel().equals(Level.WARN)
                        && event.getMessage().getFormattedMessage().startsWith("failed engine [failed to refresh segments")
                        && searchEngineFailure.compareAndSet(null, event.getThrown())) {
                        searchEngineFailed.countDown();
                    }
                }

                @Override
                public void assertMatched() {}
            });

            logger.info("--> creating commit B");
            indexDocs(indexName, 10);
            indicesAdmin().prepareRefresh(indexName).execute();
            safeAwait(chunkRequestDelayed);

            // keep the search shard open while the index is deleted on the indexing node
            internalCluster().getInstance(ClusterService.class, searchNode)
                .getClusterApplierService()
                .runOnApplierThread("block search node applier", Priority.IMMEDIATE, state -> {
                    searchNodeApplierBlocked.countDown();
                    safeAwait(unblockSearchNodeApplier);
                }, ActionListener.noop());
            safeAwait(searchNodeApplierBlocked);

            logger.info("--> deleting index");
            final var index = resolveIndex(indexName);
            final var deleteFuture = indicesAdmin().prepareDelete(indexName).execute();
            final var indexNodeIndicesService = internalCluster().getInstance(IndicesService.class, indexNode);
            assertBusy(() -> assertThat(indexNodeIndicesService.hasIndex(index), is(false)));

            logger.info("--> releasing [{}] delayed VBCC chunk requests", delayedChunkRequests.size());
            releaseChunkRequests.set(true);
            delayedChunkRequests.forEach(Runnable::run);

            safeAwait(searchEngineFailed);
            mockLog.assertAllExpectationsMatched();

            unblockSearchNodeApplier.countDown();
            safeGet(deleteFuture);
        } finally {
            unblockSearchNodeApplier.countDown();
            searchTransportService.clearAllRules();
            final var notification = heldNotification.getAndSet(null);
            if (notification != null) {
                notification.run();
            }
        }

        final var failure = searchEngineFailure.get();
        assertThat(failure, notNullValue());
        logger.info("--> search engine failed with", failure);

        assertThat(failure, instanceOf(CorruptIndexException.class));
        assertThat(failure.getMessage(), containsString("codec footer mismatch (file truncated?): actual footer=0"));
        assertThat(failure.getMessage(), containsString(".si]"));
        assertTrue(
            "expected a suppressed NoSuchFileException",
            Arrays.stream(failure.getSuppressed()).anyMatch(e -> ExceptionsHelper.unwrap(e, NoSuchFileException.class) != null)
        );
    }
}
