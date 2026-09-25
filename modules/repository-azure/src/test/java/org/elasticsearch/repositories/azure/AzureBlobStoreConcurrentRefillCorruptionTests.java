/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.azure;

import fixture.azure.AzureHttpHandler;
import fixture.azure.MockAzureBlobStore;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomPurpose;

/**
 * Regression test for the buffer dup+drop corruption in Azure uploads. Uploads blobs via {@link AzureBlobStore#writeBlobAtomic} while
 * the upload stream blocks both pool threads at the end of every burst, so that concurrent refill requests can race. Before the fix,
 * that race caused the upload to emit one 64KB buffer twice and skip the next, with the total length still matching. Every stored blob
 * is compared byte-by-byte with the source to catch silent corruption.
 */
@SuppressForbidden(reason = "use a http server")
public class AzureBlobStoreConcurrentRefillCorruptionTests extends AbstractAzureServerTestCase {

    // AzureBlobStore.DEFAULT_UPLOAD_BUFFERS_SIZE
    private static final int BUFFER_SIZE = ByteSizeUnit.KB.toIntBytes(64);
    // number of buffers per refill request (reactor-netty MonoSendMany.REFILL_SIZE)
    private static final int REFILL_SIZE = 64;
    private static final int BLOB_SIZE = ByteSizeUnit.MB.toIntBytes(42);
    private static final int MAX_UPLOADS = 100;
    private static final TimeValue BLOCK = TimeValue.timeValueMillis(40);

    @Override
    protected ByteSizeValue maxSinglePartUploadSize() {
        return ByteSizeValue.of(256, ByteSizeUnit.MB);
    }

    @Override
    protected Settings clientSettings() {
        return Settings.builder().put(AzureClientProvider.EVENT_LOOP_THREAD_COUNT.getKey(), 1).build();
    }

    @Override
    protected ExecutorBuilder<?> repositoryExecutorBuilder(Settings settings) {
        return new ScalingExecutorBuilder(AzureRepositoryPlugin.REPOSITORY_THREAD_POOL_NAME, 0, 2, TimeValue.timeValueSeconds(30L), false);
    }

    public void testConcurrentRefillsCorruptSinglePartUpload() throws Exception {
        final var handler = new AzureHttpHandler(ACCOUNT, CONTAINER, null, MockAzureBlobStore.LeaseExpiryPredicate.NEVER_EXPIRE);
        httpServer.createContext("/", handler);
        final BlobContainer blobContainer = builder().withMaxRetries(2).withTryTimeout(TimeValue.timeValueSeconds(30)).build();
        final Executor pool = threadPool.executor(AzureRepositoryPlugin.REPOSITORY_THREAD_POOL_NAME);

        final byte[] data = randomByteArrayOfLength(BLOB_SIZE);
        final var streamsOpened = new AtomicInteger();
        final var pairings = new AtomicInteger();
        final var failedUploads = new ArrayList<String>();

        for (int upload = 0; upload < MAX_UPLOADS; upload++) {
            final String blobName = "refill_race_" + upload;
            try {
                blobContainer.writeBlobAtomic(randomPurpose(), blobName, BLOB_SIZE, (offset, length) -> {
                    streamsOpened.incrementAndGet();
                    return new BurstBlockingInputStream(data, Math.toIntExact(offset), Math.toIntExact(length), pool, pairings);
                }, false, Runnable::run);
            } catch (IOException e) {
                // a dropped buffer without a matching duplicate leaves the body short; the SDK rejects it
                failedUploads.add(blobName + ": " + e.getCause());
                continue;
            }
            final BytesReference committed = handler.getMockBlobStore().getBlob(blobName, null).getContents();
            assertNotNull(blobName, committed);
            assertEquals(blobName, BLOB_SIZE, committed.length());
            final String corruption = describeCorruption(blobName, data, BytesReference.toBytes(committed));
            if (corruption != null) {
                fail(
                    corruption
                        + "\nafter "
                        + (upload + 1)
                        + " uploads, "
                        + pairings.get()
                        + " forced refill pairings, "
                        + streamsOpened.get()
                        + " streams opened, "
                        + failedUploads.size()
                        + " uploads failed: "
                        + failedUploads
                );
            }
            blobContainer.deleteBlobsIgnoringIfNotExists(randomPurpose(), List.of(blobName).iterator());
            if ((upload + 1) % 10 == 0) {
                logger.info(
                    "--> {} uploads verified, {} forced refill pairings, {} streams opened, {} uploads failed",
                    upload + 1,
                    pairings.get(),
                    streamsOpened.get(),
                    failedUploads.size()
                );
            }
        }
        logger.info(
            "--> no corruption in {} uploads ({} forced refill pairings, {} streams opened, {} uploads failed: {})",
            MAX_UPLOADS,
            pairings.get(),
            streamsOpened.get(),
            failedUploads.size(),
            failedUploads
        );
    }

    /** Returns a description of any 64KB chunks that differ from the source, or {@code null} if the blob matches. */
    @Nullable
    private static String describeCorruption(String blobName, byte[] data, byte[] committedBytes) {
        StringBuilder description = null;
        final int chunks = (data.length + BUFFER_SIZE - 1) / BUFFER_SIZE;
        for (int chunk = 0; chunk < chunks; chunk++) {
            final int from = chunk * BUFFER_SIZE;
            final int to = Math.min(from + BUFFER_SIZE, data.length);
            if (Arrays.equals(data, from, to, committedBytes, from, to)) {
                continue;
            }
            if (description == null) {
                description = new StringBuilder(blobName).append(" differs from the source:");
            }
            description.append("\n  chunk ").append(chunk).append(" [").append(from).append(", ").append(to).append(')');
            final int length = to - from;
            int source = -1;
            for (int otherFrom = 0; otherFrom + length <= data.length; otherFrom += BUFFER_SIZE) {
                if (Arrays.equals(committedBytes, from, to, data, otherFrom, otherFrom + length)) {
                    source = otherFrom / BUFFER_SIZE;
                    break;
                }
            }
            if (source < 0) {
                description.append(" matches no source chunk");
            } else {
                description.append(" holds source chunk ").append(source).append(" (").append(source - chunk).append(')');
            }
        }
        return description == null ? null : description.toString();
    }

    /** Blocks both pool threads briefly at the end of every burst of {@link #REFILL_SIZE} buffers, so refill requests queue up and race. */
    private final class BurstBlockingInputStream extends InputStream {
        private final byte[] data;
        private final int end;
        private final Executor pool;
        private final AtomicInteger pairings;
        private int position;

        BurstBlockingInputStream(byte[] data, int offset, int length, Executor pool, AtomicInteger pairings) {
            this.data = data;
            this.position = offset;
            this.end = offset + length;
            this.pool = pool;
            this.pairings = pairings;
        }

        @Override
        public int read() {
            return position < end ? data[position++] & 0xFF : -1;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (position >= end) {
                return -1;
            }
            final int n = Math.min(len, end - position);
            System.arraycopy(data, position, b, off, n);
            final int before = position;
            position += n;
            if (position / (REFILL_SIZE * BUFFER_SIZE) > before / (REFILL_SIZE * BUFFER_SIZE)) {
                blockPool();
            }
            return n;
        }

        private void blockPool() {
            pairings.incrementAndGet();
            final CountDownLatch release = new CountDownLatch(1);
            for (int i = 0; i < 2; i++) {
                pool.execute(() -> {
                    try {
                        release.await(10, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
            threadPool.schedule(release::countDown, BLOCK, threadPool.generic());
        }
    }
}
