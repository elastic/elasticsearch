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

import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.fs.FsBlobContainer;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.repositories.SnapshotMetrics;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.mockstore.BlobStoreWrapper;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.test.ESTestCase.asInstanceOf;
import static org.elasticsearch.test.ESTestCase.safeGet;

/**
 * {@link FsRepository} implementation for testing purpose that emulates the concurrent multipart uploads on the file system.
 */
class ConcurrentMultiPartUploadsMockFsRepository extends FsRepository {

    public static final String TYPE = ObjectStoreService.ObjectStoreType.MOCK.toString().toLowerCase(Locale.ROOT);
    public static final String REPOSITORY_THREAD_POOL_NAME = "repository_fs";

    public static final String MULTIPART_UPLOAD_THRESHOLD_SIZE = REPOSITORY_THREAD_POOL_NAME
        + ".concurrent_multipart_upload.threshold_size";
    public static final Setting<ByteSizeValue> MULTIPART_UPLOAD_THRESHOLD_SIZE_SETTING = Setting.byteSizeSetting(
        REPOSITORY_THREAD_POOL_NAME + ".concurrent_multipart_upload.threshold_size",
        ByteSizeValue.of(128, ByteSizeUnit.KB),
        Setting.Property.NodeScope
    );

    public static final String MULTIPART_UPLOAD_PART_SIZE = REPOSITORY_THREAD_POOL_NAME + ".concurrent_multipart_upload.part_size";
    private static final Setting<ByteSizeValue> MULTIPART_UPLOAD_PART_SIZE_SETTING = Setting.byteSizeSetting(
        MULTIPART_UPLOAD_PART_SIZE,
        ByteSizeValue.of(16, ByteSizeUnit.KB),
        Setting.Property.NodeScope
    );

    private static final StandardOpenOption[] STANDARD_OPEN_OPTIONS = new StandardOpenOption[] {
        StandardOpenOption.READ,
        StandardOpenOption.WRITE,
        StandardOpenOption.CREATE_NEW,
        StandardOpenOption.SPARSE };

    private final ByteSizeValue largeBlobThreshold;
    private final ByteSizeValue multiPartUploadSize;

    public static class Plugin extends org.elasticsearch.plugins.Plugin implements RepositoryPlugin {

        @Override
        public Map<String, Factory> getRepositories(
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings,
            RepositoriesMetrics repositoriesMetrics,
            SnapshotMetrics snapshotMetrics
        ) {
            return Map.of(
                TYPE,
                (projectId, metadata) -> new ConcurrentMultiPartUploadsMockFsRepository(
                    projectId,
                    metadata,
                    env,
                    namedXContentRegistry,
                    clusterService,
                    bigArrays,
                    recoverySettings,
                    snapshotMetrics
                )
            );
        }

        @Override
        public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
            return List.of(new ScalingExecutorBuilder(REPOSITORY_THREAD_POOL_NAME, 0, 5, TimeValue.timeValueSeconds(30L), false));
        }

        @Override
        public List<Setting<?>> getSettings() {
            return List.of(MULTIPART_UPLOAD_THRESHOLD_SIZE_SETTING, MULTIPART_UPLOAD_PART_SIZE_SETTING);
        }
    }

    private final ExecutorService executor;

    ConcurrentMultiPartUploadsMockFsRepository(
        ProjectId projectId,
        RepositoryMetadata metadata,
        Environment environment,
        NamedXContentRegistry namedXContentRegistry,
        ClusterService clusterService,
        BigArrays bigArrays,
        RecoverySettings recoverySettings,
        SnapshotMetrics snapshotMetrics
    ) {
        super(projectId, metadata, environment, namedXContentRegistry, clusterService, bigArrays, recoverySettings, snapshotMetrics);
        this.executor = clusterService.threadPool().executor(REPOSITORY_THREAD_POOL_NAME);
        this.largeBlobThreshold = MULTIPART_UPLOAD_THRESHOLD_SIZE_SETTING.get(clusterService.getSettings());
        this.multiPartUploadSize = MULTIPART_UPLOAD_PART_SIZE_SETTING.get(clusterService.getSettings());
    }

    long getLargeBlobThresholdInBytes() {
        return largeBlobThreshold.getBytes();
    }

    long getMultiPartUploadSize() {
        return multiPartUploadSize.getBytes();
    }

    @Override
    protected BlobStore createBlobStore() throws Exception {
        var fsBlobStore = asInstanceOf(FsBlobStore.class, super.createBlobStore());
        return new BlobStoreWrapper(fsBlobStore) {
            @Override
            public BlobContainer blobContainer(BlobPath blobPath) {
                var fsBlobContainer = asInstanceOf(FsBlobContainer.class, fsBlobStore.blobContainer(blobPath));
                return new MockFsBlobContainer(fsBlobContainer, fsBlobContainer.getPath());
            }
        };
    }

    private class MockFsBlobContainer extends FilterBlobContainer {

        private final Path path;

        private MockFsBlobContainer(BlobContainer delegate, Path path) {
            super(delegate);
            this.path = path;
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            var fsBlobContainer = asInstanceOf(FsBlobContainer.class, child);
            return new MockFsBlobContainer(fsBlobContainer, fsBlobContainer.getPath());
        }

        @Override
        public boolean supportsConcurrentMultipartUploads() {
            return true;
        }

        @Override
        public void writeBlobAtomic(
            OperationPurpose purpose,
            String blobName,
            long blobSize,
            BlobMultiPartInputStreamProvider provider,
            boolean failIfAlreadyExists
        ) throws IOException {
            if (blobSize <= getLargeBlobThresholdInBytes()) {
                try (var stream = provider.apply(0L, blobSize)) {
                    super.writeBlobAtomic(purpose, blobName, stream, blobSize, failIfAlreadyExists);
                }
            } else {
                final String tempBlob = FsBlobContainer.tempBlobName(blobName);
                try {
                    var tempBlobPath = path.resolve(tempBlob);
                    try (var fileChannel = FileChannel.open(tempBlobPath, STANDARD_OPEN_OPTIONS)) {
                        final var bytesWritten = new AtomicLong();
                        final var future = new PlainActionFuture<Void>();
                        try (var refCountingListener = new RefCountingListener(future)) {
                            long offset = 0L;
                            long remaining = blobSize;
                            while (remaining > 0) {
                                long partSize = Math.min(remaining, getMultiPartUploadSize());
                                final long partOffset = offset;
                                executor.execute(ActionRunnable.run(refCountingListener.acquire(), () -> {
                                    var bytesCopied = 0L;
                                    try (var input = provider.apply(partOffset, partSize)) {
                                        bytesCopied += copy(input, fileChannel, partOffset);
                                        bytesWritten.addAndGet(bytesCopied);
                                    }
                                    if (bytesCopied != partSize) {
                                        throw new IllegalStateException(
                                            format("[%d] bytes copied for writing part, expecting [%d] bytes", bytesCopied, partSize)
                                        );
                                    }

                                }));
                                remaining -= partSize;
                                assert 0 <= remaining : remaining;
                                offset += partSize;
                            }
                            assert 0 == remaining : remaining;
                            assert offset == blobSize;
                        }
                        try {
                            safeGet(future);
                        } catch (AssertionError assertion) {
                            // An explanation for this ugly catch block:
                            // The executor will wrap an IOException during copy into an ExecutionException
                            // causing safeGet to in turn wrap that in an AssertionError that won't be caught.
                            // In order to better match the semantics of sequential copy we unwrap the ExecutionException
                            // back into an IOException here. This allows e.g. corruption handling to be tested.
                            if (assertion.getCause() instanceof ExecutionException ee && ee.getCause() instanceof IOException ioe) {
                                throw ioe;
                            }
                            throw assertion;
                        }

                        if (bytesWritten.get() != blobSize) {
                            throw new IllegalStateException(
                                format("[%d] bytes copied for writing blob, expecting [%d] bytes", bytesWritten.get(), blobSize)
                            );
                        }
                    }
                    IOUtils.fsync(tempBlobPath, false);
                    Files.move(tempBlobPath, path.resolve(blobName), StandardCopyOption.ATOMIC_MOVE);
                } catch (Exception ex) {
                    try {
                        deleteBlobsIgnoringIfNotExists(purpose, Iterators.single(tempBlob));
                    } catch (IOException e) {
                        ex.addSuppressed(e);
                    }
                    throw ex;
                } finally {
                    IOUtils.fsync(path, true);
                }
            }
        }
    }

    private static long copy(InputStream in, FileChannel fileChannel, long position) throws IOException {
        assert ThreadPool.assertCurrentThreadPool(REPOSITORY_THREAD_POOL_NAME);
        final var buffer = new byte[8192];
        var byteCount = 0L;

        int bytesRead;
        while ((bytesRead = in.read(buffer)) != -1) {
            var bytesWritten = fileChannel.write(ByteBuffer.wrap(buffer).limit(bytesRead), position + byteCount);
            assert bytesWritten == bytesRead;
            byteCount += bytesWritten;
        }
        return byteCount;
    }
}
