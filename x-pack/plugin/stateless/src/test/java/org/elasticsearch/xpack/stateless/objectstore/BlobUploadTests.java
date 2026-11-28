/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.objectstore;

import org.apache.lucene.util.StringHelper;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.CheckedSupplier;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.commits.BccUploadMetrics;
import org.elasticsearch.xpack.stateless.commits.Blob;
import org.elasticsearch.xpack.stateless.commits.MergeBlob;
import org.elasticsearch.xpack.stateless.commits.UploadableBlob;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectory;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService.UploadTiming;
import org.elasticsearch.xpack.stateless.test.FakeStatelessNode;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Tests the upload of a blob that is not a batched compound commit, to check the mechanics shared by all the kinds of blobs.
 */
public class BlobUploadTests extends ESTestCase {

    private static final String PART_START_LATENCY = ObjectStoreService.BCC_MULTIPART_UPLOAD_PART_START_LATENCY_HISTOGRAM;
    private static final String PART_DURATION = ObjectStoreService.BCC_UPLOAD_PART_DURATION_HISTOGRAM;
    private static final String UPLOAD_DURATION = BccUploadMetrics.BCC_OBJECT_STORE_UPLOAD_DURATION_HISTOGRAM_METRIC;
    private static final Map<String, Object> BLOB_ATTRIBUTES = Map.of("es_test_blob", "merge");

    /**
     * A blob whose bytes are held in memory, that counts what the upload does with it.
     */
    private static class TestBlob extends AbstractRefCounted implements UploadableBlob {
        private final ShardId shardId;
        private final MergeBlob blob;
        private final byte[] bytes;
        private final CheckedSupplier<InputStream, IOException> wholeStream;
        final AtomicInteger wholeStreamsOpened = new AtomicInteger();
        final AtomicInteger wholeStreamsClosed = new AtomicInteger();

        TestBlob(ShardId shardId, long primaryTerm, byte[] bytes) {
            this(shardId, primaryTerm, bytes, () -> new ByteArrayInputStream(bytes));
        }

        TestBlob(ShardId shardId, long primaryTerm, byte[] bytes, CheckedSupplier<InputStream, IOException> wholeStream) {
            this.shardId = shardId;
            this.blob = new MergeBlob(MergeBlob.blobName(StringHelper.randomId()), primaryTerm);
            this.bytes = bytes;
            this.wholeStream = wholeStream;
        }

        @Override
        protected void closeInternal() {}

        @Override
        public ShardId shardId() {
            return shardId;
        }

        @Override
        public Blob blob() {
            return blob;
        }

        @Override
        public long length() {
            return bytes.length;
        }

        @Override
        public InputStream openStream() throws IOException {
            wholeStreamsOpened.incrementAndGet();
            // the repositories close the stream they are given and the upload closes it again, so only the first close counts
            return new FilterInputStream(wholeStream.get()) {
                private final AtomicBoolean closed = new AtomicBoolean();

                @Override
                public void close() throws IOException {
                    super.close();
                    if (closed.compareAndSet(false, true)) {
                        wholeStreamsClosed.incrementAndGet();
                    }
                }
            };
        }

        @Override
        public boolean supportsPartialStreams() {
            return false;
        }

        @Override
        public InputStream openStream(long offset, long length) {
            throw new UnsupportedOperationException("partial streams are not supported");
        }
    }

    private final RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();

    private List<Measurement> measurements(String name) {
        return meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_HISTOGRAM, name);
    }

    private byte[] randomBytes() {
        return randomByteArrayOfLength(between(1, 10_000));
    }

    private SubscribableListener<UploadTiming> upload(FakeStatelessNode node, TestBlob blob) {
        final var listener = new SubscribableListener<UploadTiming>();
        node.objectStoreService.uploadBlob(container(node, blob.blob().primaryTerm()), blob, BLOB_ATTRIBUTES, listener);
        return listener;
    }

    private void assertUploadDurationRecorded(boolean success) {
        final var uploadDurations = measurements(UPLOAD_DURATION);
        assertThat(uploadDurations, hasSize(1));
        assertThat(uploadDurations.getFirst().getLong(), greaterThanOrEqualTo(0L));
        assertThat(
            uploadDurations.getFirst().attributes(),
            equalTo(Map.of("es_test_blob", "merge", ObjectStoreService.UPLOAD_SUCCESS_ATTRIBUTE_KEY, success))
        );
    }

    // the container the shard's directory reads from, to check the upload writes where the blob is read
    private static BlobContainer container(FakeStatelessNode node, long primaryTerm) {
        return BlobStoreCacheDirectory.unwrapDirectory(node.indexingStore.directory()).getBlobContainer(primaryTerm);
    }

    private static byte[] readBlob(BlobContainer container, String blobName) throws IOException {
        try (var in = container.readBlob(OperationPurpose.INDICES, blobName)) {
            return in.readAllBytes();
        }
    }

    private FakeStatelessNode node(long primaryTerm) throws IOException {
        return new FakeStatelessNode(
            this::newEnvironment,
            this::newNodeEnvironment,
            xContentRegistry(),
            primaryTerm,
            TestProjectResolvers.DEFAULT_PROJECT_ONLY,
            meterRegistry
        );
    }

    public void testFailureOpeningTheStreamFailsTheUpload() throws Exception {
        final long primaryTerm = randomLongBetween(1, 42);
        try (var node = node(primaryTerm)) {
            final var failure = new IOException("simulated failure while opening the blob");
            final var blob = new TestBlob(node.shardId, primaryTerm, randomBytes(), () -> { throw failure; });

            assertThat(safeAwaitFailure(upload(node, blob)), sameInstance(failure));

            assertFalse(container(node, primaryTerm).listBlobs(OperationPurpose.INDICES).containsKey(blob.blob().blobName()));
            assertThat(blob.wholeStreamsOpened.get(), equalTo(1));
            assertThat(blob.refCount(), equalTo(1));
            assertBusy(() -> assertUploadDurationRecorded(false));
        }
    }

    public void testBlobThatDoesNotSupportPartialStreamsIsUploadedWholeWhateverTheContainer() throws IOException {
        final long primaryTerm = randomLongBetween(1, 42);
        try (
            var node = new FakeStatelessNode(
                this::newEnvironment,
                this::newNodeEnvironment,
                xContentRegistry(),
                primaryTerm,
                TestProjectResolvers.DEFAULT_PROJECT_ONLY,
                meterRegistry
            ) {
                @Override
                protected Settings nodeSettings() {
                    return Settings.builder()
                        .put(super.nodeSettings())
                        .put(ObjectStoreService.OBJECT_STORE_CONCURRENT_MULTIPART_UPLOADS.getKey(), true)
                        .build();
                }

                @Override
                public BlobContainer wrapBlobContainer(BlobPath path, BlobContainer innerContainer) {
                    return new FilterBlobContainer(innerContainer) {
                        @Override
                        protected BlobContainer wrapChild(BlobContainer child) {
                            return child;
                        }

                        @Override
                        public boolean supportsConcurrentMultipartUploads() {
                            return true;
                        }
                    };
                }
            }
        ) {
            final var blob = new TestBlob(node.shardId, primaryTerm, randomBytes());

            safeAwait(upload(node, blob));

            assertArrayEquals(blob.bytes, readBlob(container(node, primaryTerm), blob.blob().blobName()));
            assertThat(blob.wholeStreamsOpened.get(), equalTo(1));
            assertThat(blob.wholeStreamsClosed.get(), equalTo(1));
            assertThat(blob.refCount(), equalTo(1));
            assertUploadDurationRecorded(true);
            assertThat(measurements(PART_START_LATENCY), hasSize(0));
            assertThat(measurements(PART_DURATION), hasSize(0));
        }
    }
}
