/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.OptionalBytesReference;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.repositories.RepositoryVerificationException;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.elasticsearch.repositories.blobstore.testkit.ContendedRegisterAnalyzeAction.bytesFromLong;
import static org.elasticsearch.repositories.blobstore.testkit.ContendedRegisterAnalyzeAction.longFromBytes;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class RepositoryAnalysisFailureIT extends AbstractSnapshotIntegTestCase {

    private DisruptableBlobStore blobStore;

    @Before
    public void suppressConsistencyChecks() {
        disableRepoConsistencyCheck("repository is not used for snapshots");
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestPlugin.class, LocalStateCompositeXPackPlugin.class, SnapshotRepositoryTestKit.class);
    }

    @Before
    public void createBlobStore() {
        createRepositoryNoVerify("test-repo", TestPlugin.DISRUPTABLE_REPO_TYPE);

        blobStore = new DisruptableBlobStore();
        for (final RepositoriesService repositoriesService : internalCluster().getInstances(RepositoriesService.class)) {
            try {
                ((DisruptableRepository) repositoriesService.repository("test-repo")).setBlobStore(blobStore);
            } catch (RepositoryMissingException e) {
                // it's only present on voting masters and data nodes
            }
        }
    }

    public void testSuccess() {
        final RepositoryAnalyzeAction.Request request = new RepositoryAnalyzeAction.Request("test-repo");
        request.blobCount(1);
        request.maxBlobSize(ByteSizeValue.ofBytes(10L));

        analyseRepository(request);
    }

    public void testFailsOnReadError() {
        final RepositoryAnalyzeAction.Request request = new RepositoryAnalyzeAction.Request("test-repo");
        request.maxBlobSize(ByteSizeValue.ofBytes(10L));
        request.abortWritePermitted(false);

        final CountDown countDown = new CountDown(between(1, request.getBlobCount()));
        blobStore.setDisruption(new Disruption() {
            @Override
            public byte[] onRead(byte[] actualContents, long position, long length) throws IOException {
                if (countDown.countDown()) {
                    throw new IOException("simulated");
                }
                return actualContents;
            }
        });

        final Exception exception = expectThrows(RepositoryVerificationException.class, () -> analyseRepository(request));
        final IOException ioException = (IOException) ExceptionsHelper.unwrap(exception, IOException.class);
        assert ioException != null : exception;
        assertThat(ioException.getMessage(), equalTo("simulated"));
    }

    public void testFailsOnNotFoundAfterWrite() {
        final RepositoryAnalyzeAction.Request request = new RepositoryAnalyzeAction.Request("test-repo");
        request.maxBlobSize(ByteSizeValue.ofBytes(10L));
        request.abortWritePermitted(false);
        request.rareActionProbability(0.0); // not found on an early read or an overwrite is ok

        final CountDown countDown = new CountDown(between(1, request.getBlobCount()));

        blobStore.setDisruption(new Disruption() {
            @Override
            public byte[] onRead(byte[] actualContents, long position, long length) {
                if (countDown.countDown()) {
                    return null;
                }
                return actualContents;
            }
        });

        expectThrows(RepositoryVerificationException.class, () -> analyseRepository(request));
    }

    public void testFailsOnChecksumMismatch() {
        final RepositoryAnalyzeAction.Request request = new RepositoryAnalyzeAction.Request("test-repo");
        request.maxBlobSize(ByteSizeValue.ofBytes(10L));
        request.abortWritePermitted(false);
        // The analysis can perform writeAndOverwrite as a rare action.
        // Since a read is performed towards the end of overwrite or write (rarely),
        // it can return either the old (write) or the new (overwrite) content and both
        // are considered to be correct.
        // This test disrupts reads and relies on the disrupted content to be different from
        // correct contents to trigger the expected failure. However, in rare cases,
        // the disrupted old content could be identical to the new content or vice versa which
        // leads to CI failures. Therefore, we disable rare actions to improve CI stability.
        request.rareActionProbability(0.0);

        final CountDown countDown = new CountDown(between(1, request.getBlobCount()));

        blobStore.setDisruption(new Disruption() {
            @Override
            public byte[] onRead(byte[] actualContents, long position, long length) {
                final byte[] disruptedContents = actualContents == null ? null : Arrays.copyOf(actualContents, actualContents.length);
                if (actualContents != null && countDown.countDown()) {
                    // CRC32 should always detect a single bit flip
                    disruptedContents[Math.toIntExact(position + randomLongBetween(0, length - 1))] ^= (byte) (1 << between(0, 7));
                }
                return disruptedContents;
            }
        });

        expectThrows(RepositoryVerificationException.class, () -> analyseRepository(request));
    }

    public void testFailsOnWriteException() {
        final RepositoryAnalyzeAction.Request request = new RepositoryAnalyzeAction.Request("test-repo");
        request.maxBlobSize(ByteSizeValue.ofBytes(10L));
        request.abortWritePermitted(false);

        final CountDown countDown = new CountDown(between(1, request.getBlobCount()));

        blobStore.setDisruption(new Disruption() {

            @Override
            public void onWrite() throws IOException {
                if (countDown.countDown()) {
                    throw new IOException("simulated");
                }
            }

        });

        final Exception exception = expectThrows(RepositoryVerificationException.class, () -> analyseRepository(request));
        final IOException ioException = (IOException) ExceptionsHelper.unwrap(exception, IOException.class);
        assert ioException != null : exception;
        assertThat(ioException.getMessage(), equalTo("simulated"));
    }

    public void testFailsOnIncompleteListing() {
        final RepositoryAnalyzeAction.Request request = new RepositoryAnalyzeAction.Request("test-repo");
        request.maxBlobSize(ByteSizeValue.ofBytes(10L));
        request.abortWritePermitted(false);

        blobStore.setDisruption(new Disruption() {

            @Override
            public Map<String, BlobMetadata> onList(Map<String, BlobMetadata> actualListing) {
                final HashMap<String, BlobMetadata> listing = new HashMap<>(actualListing);
                listing.keySet().iterator().remove();
                return listing;
            }

        });

        expectThrows(RepositoryVerificationException.class, () -> analyseRepository(request));
    }

    public void testFailsOnListingException() {
        final RepositoryAnalyzeAction.Request request = new RepositoryAnalyzeAction.Request("test-repo");
        request.maxBlobSize(ByteSizeValue.ofBytes(10L));
        request.abortWritePermitted(false);

        final CountDown countDown = new CountDown(1);
        blobStore.setDisruption(new Disruption() {

            @Override
            public Map<String, BlobMetadata> onList(Map<String, BlobMetadata> actualListing) throws IOException {
                if (countDown.countDown()) {
                    throw new IOException("simulated");
                }
                return actualListing;
            }
        });

        expectThrows(RepositoryVerificationException.class, () -> analyseRepository(request));
    }

    public void testFailsOnDeleteException() {
        final RepositoryAnalyzeAction.Request request = new RepositoryAnalyzeAction.Request("test-repo");
        request.maxBlobSize(ByteSizeValue.ofBytes(10L));
        request.abortWritePermitted(false);

        blobStore.setDisruption(new Disruption() {
            @Override
            public void onDelete() throws IOException {
                throw new IOException("simulated");
            }
        });

        expectThrows(RepositoryVerificationException.class, () -> analyseRepository(request));
    }

    public void testFailsOnIncompleteDelete() {
        final RepositoryAnalyzeAction.Request request = new RepositoryAnalyzeAction.Request("test-repo");
        request.maxBlobSize(ByteSizeValue.ofBytes(10L));
        request.abortWritePermitted(false);

        blobStore.setDisruption(new Disruption() {

            volatile boolean isDeleted;

            @Override
            public void onDelete() {
                isDeleted = true;
            }

            @Override
            public Map<String, BlobMetadata> onList(Map<String, BlobMetadata> actualListing) {
                if (isDeleted) {
                    assertThat(actualListing, anEmptyMap());
                    return Collections.singletonMap("leftover", new BlobMetadata("leftover", 1));
                } else {
                    return actualListing;
                }
            }
        });

        expectThrows(RepositoryVerificationException.class, () -> analyseRepository(request));
    }

    public void testFailsIfBlobCreatedOnAbort() {
        final RepositoryAnalyzeAction.Request request = new RepositoryAnalyzeAction.Request("test-repo");
        request.maxBlobSize(ByteSizeValue.ofBytes(10L));
        request.rareActionProbability(0.7); // abort writes quite often

        final AtomicBoolean writeWasAborted = new AtomicBoolean();
        blobStore.setDisruption(new Disruption() {
            @Override
            public boolean createBlobOnAbort() {
                writeWasAborted.set(true);
                return true;
            }
        });

        try {
            analyseRepository(request);
            assertFalse(writeWasAborted.get());
        } catch (RepositoryVerificationException e) {
            assertTrue(writeWasAborted.get());
        }
    }

    public void testFailsIfRegisterIncorrect() {
        final RepositoryAnalyzeAction.Request request = new RepositoryAnalyzeAction.Request("test-repo");

        blobStore.setDisruption(new Disruption() {
            private final AtomicBoolean registerWasCorrupted = new AtomicBoolean();

            @Override
            public BytesReference onContendedCompareAndExchange(BytesRegister register, BytesReference expected, BytesReference updated) {
                if (registerWasCorrupted.compareAndSet(false, true)) {
                    register.updateAndGet(bytes -> bytesFromLong(longFromBytes(bytes) + 1));
                }
                return register.compareAndExchange(expected, updated);
            }
        });
        expectThrows(RepositoryVerificationException.class, () -> analyseRepository(request));
    }

    public void testFailsIfRegisterHoldsSpuriousValue() {
        final RepositoryAnalyzeAction.Request request = new RepositoryAnalyzeAction.Request("test-repo");

        final AtomicBoolean sawSpuriousValue = new AtomicBoolean();
        final long expectedMax = Math.max(request.getConcurrency(), internalCluster().getNodeNames().length);
        blobStore.setDisruption(new Disruption() {
            @Override
            public BytesReference onContendedCompareAndExchange(BytesRegister register, BytesReference expected, BytesReference updated) {
                if (randomBoolean() && sawSpuriousValue.compareAndSet(false, true)) {
                    final var currentValue = longFromBytes(register.get());
                    if (currentValue == expectedMax) {
                        return bytesFromLong(
                            randomFrom(
                                randomLongBetween(0L, expectedMax - 1),
                                randomLongBetween(expectedMax + 1, Long.MAX_VALUE),
                                randomLongBetween(Long.MIN_VALUE, -1)
                            )
                        );
                    } else {
                        return bytesFromLong(
                            randomFrom(expectedMax, randomLongBetween(expectedMax, Long.MAX_VALUE), randomLongBetween(Long.MIN_VALUE, -1))
                        );
                    }
                }
                return register.compareAndExchange(expected, updated);
            }
        });
        try {
            analyseRepository(request);
            assertFalse(sawSpuriousValue.get());
        } catch (RepositoryVerificationException e) {
            if (sawSpuriousValue.get() == false) {
                fail(e, "did not see spurious value, so why did the verification fail?");
            }
        }
    }

    public void testTimesOutSpinningRegisterAnalysis() {
        final RepositoryAnalyzeAction.Request request = new RepositoryAnalyzeAction.Request("test-repo");
        request.timeout(TimeValue.timeValueMillis(between(1, 1000)));

        blobStore.setDisruption(new Disruption() {
            @Override
            public boolean compareAndExchangeReturnsWitness(String key) {
                // let uncontended accesses succeed but all contended ones fail
                return isContendedRegisterKey(key) == false;
            }
        });
        final var exception = expectThrows(RepositoryVerificationException.class, () -> analyseRepository(request));
        assertThat(exception.getMessage(), containsString("analysis failed"));
        assertThat(
            asInstanceOf(RepositoryVerificationException.class, exception.getCause()).getMessage(),
            containsString("analysis timed out")
        );
    }

    public void testFailsIfAllRegisterOperationsInconclusive() {
        final RepositoryAnalyzeAction.Request request = new RepositoryAnalyzeAction.Request("test-repo");
        blobStore.setDisruption(new Disruption() {
            @Override
            public boolean compareAndExchangeReturnsWitness(String key) {
                return false;
            }
        });
        final var exception = expectThrows(RepositoryVerificationException.class, () -> analyseRepository(request));
        assertThat(exception.getMessage(), containsString("analysis failed"));
        assertThat(
            asInstanceOf(RepositoryVerificationException.class, ExceptionsHelper.unwrapCause(exception.getCause())).getMessage(),
            allOf(containsString("uncontended register operation failed"), containsString("did not observe any value"))
        );
    }

    public void testFailsIfEmptyRegisterRejected() {
        final RepositoryAnalyzeAction.Request request = new RepositoryAnalyzeAction.Request("test-repo");
        blobStore.setDisruption(new Disruption() {
            @Override
            public boolean acceptsEmptyRegister() {
                return false;
            }
        });
        final var exception = expectThrows(RepositoryVerificationException.class, () -> analyseRepository(request));
        assertThat(exception.getMessage(), containsString("analysis failed"));
        final var cause = ExceptionsHelper.unwrapCause(exception.getCause());
        if (cause instanceof IOException ioException) {
            assertThat(ioException.getMessage(), containsString("empty register update rejected"));
        } else {
            assertThat(
                asInstanceOf(RepositoryVerificationException.class, ExceptionsHelper.unwrapCause(exception.getCause())).getMessage(),
                anyOf(
                    allOf(containsString("uncontended register operation failed"), containsString("did not observe any value")),
                    containsString("but instead had value [OptionalBytesReference[MISSING]]")
                )
            );
        }
    }

    private void analyseRepository(RepositoryAnalyzeAction.Request request) {
        client().execute(RepositoryAnalyzeAction.INSTANCE, request).actionGet(5L, TimeUnit.MINUTES);
    }

    private static void assertPurpose(OperationPurpose purpose) {
        assertEquals(OperationPurpose.REPOSITORY_ANALYSIS, purpose);
    }

    public static class TestPlugin extends Plugin implements RepositoryPlugin {

        static final String DISRUPTABLE_REPO_TYPE = "disruptable";

        @Override
        public Map<String, Repository.Factory> getRepositories(
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings,
            RepositoriesMetrics repositoriesMetrics
        ) {
            return Map.of(
                DISRUPTABLE_REPO_TYPE,
                metadata -> new DisruptableRepository(
                    metadata,
                    namedXContentRegistry,
                    clusterService,
                    bigArrays,
                    recoverySettings,
                    BlobPath.EMPTY
                )
            );
        }
    }

    static class DisruptableRepository extends BlobStoreRepository {

        private final AtomicReference<BlobStore> blobStoreRef = new AtomicReference<>();

        DisruptableRepository(
            RepositoryMetadata metadata,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings,
            BlobPath basePath
        ) {
            super(metadata, namedXContentRegistry, clusterService, bigArrays, recoverySettings, basePath);
        }

        void setBlobStore(BlobStore blobStore) {
            assertTrue(blobStoreRef.compareAndSet(null, blobStore));
        }

        @Override
        protected BlobStore createBlobStore() {
            final BlobStore blobStore = blobStoreRef.get();
            assertNotNull(blobStore);
            return blobStore;
        }
    }

    static class DisruptableBlobStore implements BlobStore {

        @Nullable // if deleted
        private DisruptableBlobContainer blobContainer;

        private Disruption disruption = Disruption.NONE;

        @Override
        public BlobContainer blobContainer(BlobPath path) {
            synchronized (this) {
                if (blobContainer == null) {
                    blobContainer = new DisruptableBlobContainer(path, this::deleteContainer, disruption);
                }
                return blobContainer;
            }
        }

        @Override
        public void deleteBlobsIgnoringIfNotExists(OperationPurpose purpose, Iterator<String> blobNames) {
            assertPurpose(purpose);
        }

        private void deleteContainer(DisruptableBlobContainer container) {
            blobContainer = null;
        }

        @Override
        public void close() {}

        public void setDisruption(Disruption disruption) {
            assertThat("cannot change disruption while blob container exists", blobContainer, nullValue());
            this.disruption = disruption;
        }
    }

    interface Disruption {

        Disruption NONE = new Disruption() {
        };

        default byte[] onRead(byte[] actualContents, long position, long length) throws IOException {
            return actualContents;
        }

        default void onWrite() throws IOException {}

        default Map<String, BlobMetadata> onList(Map<String, BlobMetadata> actualListing) throws IOException {
            return actualListing;
        }

        default void onDelete() throws IOException {}

        default boolean createBlobOnAbort() {
            return false;
        }

        default boolean compareAndExchangeReturnsWitness(String key) {
            return true;
        }

        default boolean acceptsEmptyRegister() {
            return true;
        }

        default BytesReference onContendedCompareAndExchange(BytesRegister register, BytesReference expected, BytesReference updated) {
            return register.compareAndExchange(expected, updated);
        }
    }

    static class DisruptableBlobContainer implements BlobContainer {

        private final BlobPath path;
        private final Consumer<DisruptableBlobContainer> deleteContainer;
        private final Disruption disruption;
        private final Map<String, byte[]> blobs = ConcurrentCollections.newConcurrentMap();
        private final Map<String, BytesRegister> registers = ConcurrentCollections.newConcurrentMap();

        DisruptableBlobContainer(BlobPath path, Consumer<DisruptableBlobContainer> deleteContainer, Disruption disruption) {
            this.path = path;
            this.deleteContainer = deleteContainer;
            this.disruption = disruption;
        }

        @Override
        public BlobPath path() {
            return path;
        }

        @Override
        public boolean blobExists(OperationPurpose purpose, String blobName) {
            assertPurpose(purpose);
            return blobs.containsKey(blobName);
        }

        @Override
        public InputStream readBlob(OperationPurpose purpose, String blobName) throws IOException {
            assertPurpose(purpose);
            final byte[] actualContents = blobs.get(blobName);
            final byte[] disruptedContents = disruption.onRead(actualContents, 0L, actualContents == null ? 0L : actualContents.length);
            if (disruptedContents == null) {
                throw new FileNotFoundException(blobName + " not found");
            }
            return new ByteArrayInputStream(disruptedContents);
        }

        @Override
        public InputStream readBlob(OperationPurpose purpose, String blobName, long position, long length) throws IOException {
            assertPurpose(purpose);
            final byte[] actualContents = blobs.get(blobName);
            final byte[] disruptedContents = disruption.onRead(actualContents, position, length);
            if (disruptedContents == null) {
                throw new FileNotFoundException(blobName + " not found");
            }
            final int truncatedLength = Math.toIntExact(Math.min(length, disruptedContents.length - position));
            return new ByteArrayInputStream(disruptedContents, Math.toIntExact(position), truncatedLength);
        }

        @Override
        public void writeBlob(
            OperationPurpose purpose,
            String blobName,
            InputStream inputStream,
            long blobSize,
            boolean failIfAlreadyExists
        ) throws IOException {
            assertPurpose(purpose);
            writeBlobAtomic(blobName, inputStream, failIfAlreadyExists);
        }

        @Override
        public void writeBlob(OperationPurpose purpose, String blobName, BytesReference bytes, boolean failIfAlreadyExists)
            throws IOException {
            assertPurpose(purpose);
            writeBlob(purpose, blobName, bytes.streamInput(), bytes.length(), failIfAlreadyExists);
        }

        @Override
        public void writeMetadataBlob(
            OperationPurpose purpose,
            String blobName,
            boolean failIfAlreadyExists,
            boolean atomic,
            CheckedConsumer<OutputStream, IOException> writer
        ) throws IOException {
            assertPurpose(purpose);
            final BytesStreamOutput out = new BytesStreamOutput();
            writer.accept(out);
            if (atomic) {
                writeBlobAtomic(purpose, blobName, out.bytes(), failIfAlreadyExists);
            } else {
                writeBlob(purpose, blobName, out.bytes(), failIfAlreadyExists);
            }
        }

        @Override
        public void writeBlobAtomic(OperationPurpose purpose, String blobName, BytesReference bytes, boolean failIfAlreadyExists)
            throws IOException {
            assertPurpose(purpose);
            final StreamInput inputStream;
            try {
                inputStream = bytes.streamInput();
            } catch (BlobWriteAbortedException e) {
                if (disruption.createBlobOnAbort()) {
                    blobs.put(blobName, new byte[0]);
                }
                throw e;
            }
            writeBlobAtomic(blobName, inputStream, failIfAlreadyExists);
        }

        private void writeBlobAtomic(String blobName, InputStream inputStream, boolean failIfAlreadyExists) throws IOException {
            if (failIfAlreadyExists && blobs.get(blobName) != null) {
                throw new FileAlreadyExistsException(blobName);
            }

            final byte[] contents;
            try {
                contents = inputStream.readAllBytes();
            } catch (BlobWriteAbortedException e) {
                if (disruption.createBlobOnAbort()) {
                    blobs.put(blobName, new byte[0]);
                }
                throw e;
            }
            disruption.onWrite();
            blobs.put(blobName, contents);
        }

        @Override
        public DeleteResult delete(OperationPurpose purpose) throws IOException {
            assertPurpose(purpose);
            disruption.onDelete();
            deleteContainer.accept(this);
            final DeleteResult deleteResult = new DeleteResult(blobs.size(), blobs.values().stream().mapToLong(b -> b.length).sum());
            blobs.clear();
            return deleteResult;
        }

        @Override
        public void deleteBlobsIgnoringIfNotExists(OperationPurpose purpose, Iterator<String> blobNames) {
            assertPurpose(purpose);
            blobNames.forEachRemaining(blobs.keySet()::remove);
        }

        @Override
        public Map<String, BlobMetadata> listBlobs(OperationPurpose purpose) throws IOException {
            assertPurpose(purpose);
            return disruption.onList(
                blobs.entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> new BlobMetadata(e.getKey(), e.getValue().length)))
            );
        }

        @Override
        public Map<String, BlobContainer> children(OperationPurpose purpose) {
            assertPurpose(purpose);
            return Map.of();
        }

        @Override
        public Map<String, BlobMetadata> listBlobsByPrefix(OperationPurpose purpose, String blobNamePrefix) throws IOException {
            assertPurpose(purpose);
            final Map<String, BlobMetadata> blobMetadataByName = listBlobs(purpose);
            blobMetadataByName.keySet().removeIf(s -> s.startsWith(blobNamePrefix) == false);
            return blobMetadataByName;
        }

        @Override
        public void compareAndExchangeRegister(
            OperationPurpose purpose,
            String key,
            BytesReference expected,
            BytesReference updated,
            ActionListener<OptionalBytesReference> listener
        ) {
            assertPurpose(purpose);
            final boolean isContendedRegister = isContendedRegisterKey(key); // validate key
            if (disruption.acceptsEmptyRegister() == false && updated.length() == 0) {
                if (randomBoolean()) {
                    listener.onResponse(OptionalBytesReference.MISSING);
                } else {
                    listener.onFailure(new IOException("empty register update rejected"));
                }
            } else if (disruption.compareAndExchangeReturnsWitness(key)) {
                final var register = registers.computeIfAbsent(key, ignored -> new BytesRegister());
                if (isContendedRegister) {
                    listener.onResponse(OptionalBytesReference.of(disruption.onContendedCompareAndExchange(register, expected, updated)));
                } else {
                    listener.onResponse(OptionalBytesReference.of(register.compareAndExchange(expected, updated)));
                }
            } else {
                listener.onResponse(OptionalBytesReference.MISSING);
            }
        }
    }

    static boolean isContendedRegisterKey(String key) {
        if (key.startsWith(RepositoryAnalyzeAction.CONTENDED_REGISTER_NAME_PREFIX)) {
            return true;
        }
        if (key.startsWith(RepositoryAnalyzeAction.UNCONTENDED_REGISTER_NAME_PREFIX)) {
            return false;
        }
        return fail(null, "unknown register: %s", key);
    }

}
