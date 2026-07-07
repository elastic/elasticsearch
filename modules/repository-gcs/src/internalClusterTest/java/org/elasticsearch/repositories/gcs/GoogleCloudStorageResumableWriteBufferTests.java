/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.gcs;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomPurpose;

/// Isolates the tests that need a small, fixed resumable-write buffer so they can assert exact chunk
/// sizes. The 1mb pin is deliberately kept out of [AbstractGoogleCloudStorageBlobStoreRepositoryTestCase]:
/// applied class-wide it shreds every large upload into dozens of 1mb chunks (16x the round-trips) and
/// amplifies injected-error retry storms, which is what tripped `testWriteFileMultipleOfChunkSize` in
/// #152286. Only this class overrides the buffer size (and records chunk sizes), so the other tests keep
/// the production-default 16MB buffer.
@SuppressForbidden(reason = "this test uses a HttpServer to emulate a Google Cloud Storage endpoint")
public class GoogleCloudStorageResumableWriteBufferTests extends AbstractGoogleCloudStorageBlobStoreRepositoryTestCase {

    private ChunkRecordingHttpHandler chunkRecordingHandler;

    @Override
    protected Optional<ByteSizeValue> resumableWriteBufferSize() {
        return Optional.of(ByteSizeValue.ofMb(1));
    }

    @Override
    protected HttpHandler maybeWrapForRecording(HttpHandler blobStoreHandler) {
        chunkRecordingHandler = new ChunkRecordingHttpHandler(blobStoreHandler);
        return chunkRecordingHandler;
    }

    @Override
    protected boolean suppressErrorInjection() {
        return chunkRecordingHandler != null && chunkRecordingHandler.recording;
    }

    public void testResumableWriteBufferInAction() throws Exception {
        // buffer size is fixed at 1mb by resumableWriteBufferSize() for the "test" client
        final int bufferSizeBytes = Math.toIntExact(ByteSizeValue.ofMb(1).getBytes());
        final int numFullChunks = randomIntBetween(2, 4);
        // lastChunkSize < bufferSizeBytes to guarantee numFullChunks non-final chunks
        final int lastChunkSize = randomIntBetween(1, bufferSizeBytes - 1);
        final int blobSize = bufferSizeBytes * numFullChunks + lastChunkSize;

        final String repoName = createRepository(randomRepositoryName(), false);

        chunkRecordingHandler.recording = true;
        try (BlobStore store = newBlobStore(repoName)) {
            final BlobContainer container = store.blobContainer(BlobPath.EMPTY);
            container.writeBlob(randomPurpose(), "test-resumable-write-buffer", new BytesArray(randomByteArrayOfLength(blobSize)), true);
            // Verify chunk sizes before deleting
            final List<Integer> sizes = new ArrayList<>(chunkRecordingHandler.recordedNonFinalChunkSizes);
            assertTrue("expected at least " + numFullChunks + " non-final chunks, got " + sizes, sizes.size() >= numFullChunks);
            for (final int size : sizes) {
                assertEquals("each non-final chunk must be exactly " + bufferSizeBytes + " bytes", bufferSizeBytes, size);
            }
            container.delete(randomPurpose());
        } finally {
            chunkRecordingHandler.recording = false;
        }
    }

    @SuppressForbidden(reason = "this test uses a HttpServer to emulate a Google Cloud Storage endpoint")
    private static class ChunkRecordingHttpHandler implements DelegatingHttpHandler {
        private final HttpHandler delegate;
        private volatile boolean recording = false;
        final Queue<Integer> recordedNonFinalChunkSizes = new ConcurrentLinkedQueue<>();

        ChunkRecordingHttpHandler(HttpHandler delegate) {
            this.delegate = delegate;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (recording && "PUT".equals(exchange.getRequestMethod())) {
                final String contentRange = exchange.getRequestHeaders().getFirst("Content-Range");
                if (contentRange != null && contentRange.endsWith("/*")) {
                    // "bytes start-end/*", split on '-' and '/'
                    final String[] parts = contentRange.substring("bytes ".length()).split("[-/]");
                    recordedNonFinalChunkSizes.add(Integer.parseInt(parts[1]) - Integer.parseInt(parts[0]) + 1);
                }
            }
            delegate.handle(exchange);
        }

        @Override
        public HttpHandler getDelegate() {
            return delegate;
        }
    }
}
