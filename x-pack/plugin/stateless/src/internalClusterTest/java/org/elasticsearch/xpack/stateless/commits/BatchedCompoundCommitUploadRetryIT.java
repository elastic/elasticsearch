/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryPlugin;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryStrategy;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Verifies that a batched compound commit blob is intact after its concurrent multipart upload has been retried.
 *
 * <p>In production an Azure block upload timed out twice and the third attempt reported success, but had written a blob
 * whose commit header was missing: part 0 held {@code source[headerSize .. partSize + headerSize)} rather than
 * {@code source[0 .. partSize)}, with the payload shifted forward into the header's place, while later parts stayed
 * correctly positioned. The blob's total length was unchanged, so nothing downstream rejected it. The shard only failed
 * later, when recovery read the commit back and hit
 * {@code CorruptIndexException: codec mismatch: actual codec=Lucene90CompoundData vs expected codec=stateless_commit}.
 *
 * <p>The corresponding unit tests in {@code VirtualBatchedCompoundCommitTests} re-read a range after abandoning it and
 * pass, so a synchronous re-read is not sufficient to reproduce the fault. This test exercises the real retry path:
 * {@code RetryableAction} re-running a full upload against a blob container, after a previous attempt had already
 * invoked the stream provider and partially consumed it.
 */
public class BatchedCompoundCommitUploadRetryIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(StatelessMockRepositoryPlugin.class);
        return plugins;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings()
            // keep commits batching together so a single flush produces one sizeable blob
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_SIZE.getKey(), ByteSizeValue.ofGb(1))
            .put(StatelessCommitService.STATELESS_UPLOAD_VBCC_MAX_AGE.getKey(), TimeValue.timeValueDays(1))
            .put(StatelessCommitService.STATELESS_UPLOAD_MONITOR_INTERVAL.getKey(), TimeValue.timeValueDays(1))
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 1000)
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            .put(ObjectStoreService.OBJECT_STORE_CONCURRENT_MULTIPART_UPLOADS.getKey(), true)
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK);
    }

    public void testCommitBlobIsIntactAfterRetriedMultipartUpload() throws Exception {
        final int failuresToInject = randomIntBetween(1, 3);
        final var remainingFailures = new AtomicInteger(failuresToInject);
        final var uploadAttempts = new AtomicInteger();

        final var failFirstAttempts = new StatelessMockRepositoryStrategy() {

            /**
             * Multipart variant. Only reached when the container advertises multipart support; the mock repository's
             * container does not, so on this suite the single-stream variant below is the one that runs. Kept so the
             * test does the right thing if run against a container that does support it.
             */
            @Override
            public void blobContainerWriteBlobAtomic(
                CheckedRunnable<IOException> originalRunnable,
                OperationPurpose purpose,
                String blobName,
                long blobSize,
                BlobContainer.BlobMultiPartInputStreamProvider provider,
                boolean failIfAlreadyExists,
                Executor executor
            ) throws IOException {
                if (blobName.startsWith(StatelessCompoundCommit.PREFIX)) {
                    uploadAttempts.incrementAndGet();
                    if (remainingFailures.getAndDecrement() > 0) {
                        // Invoke the provider and consume part of it, then abort the attempt: the shape of a timed-out
                        // stageBlock, where the stream was handed out, partially read, then closed short of EOF.
                        try (var stream = provider.apply(0L, blobSize)) {
                            stream.readNBytes(Math.toIntExact(Math.max(1L, blobSize / 3L)));
                        }
                        throw new IOException("simulated object store write timeout for [" + blobName + "]");
                    }
                }
                originalRunnable.run();
            }

            /** Single-stream variant, which is what the mock repository's container actually uses. */
            @Override
            public void blobContainerWriteBlobAtomic(
                CheckedRunnable<IOException> originalRunnable,
                OperationPurpose purpose,
                String blobName,
                InputStream inputStream,
                long blobSize,
                boolean failIfAlreadyExists
            ) throws IOException {
                if (blobName.startsWith(StatelessCompoundCommit.PREFIX)) {
                    uploadAttempts.incrementAndGet();
                    if (remainingFailures.getAndDecrement() > 0) {
                        // Partially consume the upload stream, then abort, leaving it closed short of EOF.
                        inputStream.readNBytes(Math.toIntExact(Math.max(1L, blobSize / 3L)));
                        throw new IOException("simulated object store write timeout for [" + blobName + "]");
                    }
                }
                originalRunnable.run();
            }
        };

        final String indexNode = startMasterAndIndexNode(failFirstAttempts);
        startSearchNode();
        ensureStableCluster(2);

        final String indexName = randomIndexName();
        createIndex(indexName, indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build());
        ensureGreen(indexName);

        // Index enough documents that the commit blob is large enough to be split into several parts.
        final int numDocs = randomIntBetween(200, 500);
        var bulkRequest = client().prepareBulk();
        for (int i = 0; i < numDocs; i++) {
            bulkRequest.add(client().prepareIndex(indexName).setId(String.valueOf(i)).setSource("field", randomAlphaOfLength(512)));
        }
        assertNoFailures(bulkRequest.get());

        // Forces the commit upload, which must survive the injected failures via RetryableAction.
        flush(indexName);

        // More attempts than injected failures means at least one retry ran and then succeeded.
        assertThat(uploadAttempts.get(), greaterThan(failuresToInject));

        // Stop interfering before recovery reads the blob back.
        setNodeRepositoryStrategy(indexNode, StatelessMockRepositoryStrategy.DEFAULT);

        // Closing and reopening forces the shard to recover from the object store, which is where the production
        // corruption surfaced. A blob written with a displaced header fails here with a CorruptIndexException.
        // (Restarting the node would be closer to production but this suite's index node is also the only master.)
        assertAcked(indicesAdmin().prepareClose(indexName));
        assertAcked(indicesAdmin().prepareOpen(indexName));
        ensureGreen(indexName);

        assertHitCount(prepareSearch(indexName).setSize(0).setTrackTotalHits(true), numDocs);
    }
}
