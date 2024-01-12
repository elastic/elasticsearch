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

package co.elastic.elasticsearch.stateless.lucene;

import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.commits.BlobLocation;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.test.FakeStatelessNode;

import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.mockfile.FilterFileSystemProvider;
import org.apache.lucene.tests.mockfile.HandleTrackingFS;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.common.BlobCacheBufferedIndexInput;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.fs.FsBlobContainer;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.store.ESIndexInputTestCase;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.PathUtilsForTesting;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.LuceneFilesExtensions;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.searchablesnapshots.AbstractSearchableSnapshotsTestCase.randomChecksumBytes;
import static org.elasticsearch.xpack.searchablesnapshots.AbstractSearchableSnapshotsTestCase.randomIOContext;
import static org.elasticsearch.xpack.searchablesnapshots.cache.common.TestUtils.pageAligned;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;

public class ReopeningIndexInputTests extends ESIndexInputTestCase {

    public void testRandomReads() throws IOException {
        final Path dataPath = createTempDir();
        final ShardId shardId = new ShardId(new Index("_index_name", "_index_id"), 0);
        final ThreadPool threadPool = getThreadPool("testRandomReads");
        final ByteSizeValue cacheSize = ByteSizeValue.ofBytes(randomLongBetween(0, 10_000_000));
        final var settings = Settings.builder()
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), cacheSize)
            .put(
                SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(),
                pageAligned(new ByteSizeValue(randomIntBetween(4, 1024), ByteSizeUnit.KB))
            )
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .putList(Environment.PATH_DATA_SETTING.getKey(), dataPath.toAbsolutePath().toString())
            .build();
        final Path indexDataPath = dataPath.resolve("index");
        final Path blobStorePath = PathUtils.get(createTempDir().toString());
        try (
            NodeEnvironment nodeEnvironment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            SharedBlobCacheService<FileCacheKey> sharedBlobCacheService = new SharedBlobCacheService<>(
                nodeEnvironment,
                settings,
                threadPool,
                Stateless.SHARD_READ_THREAD_POOL,
                BlobCacheMetrics.NOOP
            );
            FsBlobStore blobStore = new FsBlobStore(randomIntBetween(1, 8) * 1024, blobStorePath, false);
            IndexDirectory indexDirectory = new IndexDirectory(
                newFSDirectory(indexDataPath),
                new SearchDirectory(sharedBlobCacheService, shardId, null)
            )
        ) {
            final FsBlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.EMPTY, blobStorePath);
            indexDirectory.getSearchDirectory().setBlobContainer(value -> blobContainer);

            for (int i = 0; i < 100; i++) {
                final String fileName = "file_" + i + randomFileExtension();
                final byte[] bytes = randomChecksumBytes(randomIntBetween(1, 100_000)).v2();
                try (IndexOutput output = indexDirectory.createOutput(fileName, IOContext.DEFAULT)) {
                    output.writeBytes(bytes, bytes.length);
                }
                // number of operations to allow before the file is uploaded to the object store and deleted from disk
                final CountDown operationsCountDown = new CountDown(randomIntBetween(1, 25));

                try (IndexInput indexInput = indexDirectory.openInput(fileName, randomIOContext())) {
                    assertEquals(bytes.length, indexInput.length());
                    assertEquals(0, indexInput.getFilePointer());

                    SimulateUploadIndexInput input = new SimulateUploadIndexInput(
                        fileName,
                        bytes.length,
                        indexDirectory,
                        indexInput,
                        operationsCountDown,
                        blobContainer
                    );
                    byte[] output = randomReadAndSlice(input, bytes.length);
                    assertArrayEquals(bytes, output);

                    assertThat(indexDirectory.fileLength(fileName), equalTo((long) bytes.length));
                    if (input.isCountedDown() == false) {
                        operationsCountDown.fastForward();
                        input.upload();
                    }
                }

                expectThrowsAnyOf(
                    List.of(FileNotFoundException.class, NoSuchFileException.class),
                    () -> indexDirectory.getDelegate().fileLength(fileName)
                );
            }
        } finally {
            assertTrue(ThreadPool.terminate(threadPool, 10L, TimeUnit.SECONDS));
        }
    }

    public void testReadsAfterDelete() throws IOException {
        final Path dataPath = createTempDir();
        final ShardId shardId = new ShardId(new Index("_index_name", "_index_id"), 0);
        final ThreadPool threadPool = getThreadPool("testReadsAfterDelete");
        final ByteSizeValue cacheSize = ByteSizeValue.ofBytes(randomLongBetween(0, 10_000_000));
        final var settings = Settings.builder()
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), cacheSize)
            .put(
                SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(),
                pageAligned(new ByteSizeValue(randomIntBetween(4, 1024), ByteSizeUnit.KB))
            )
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .putList(Environment.PATH_DATA_SETTING.getKey(), dataPath.toAbsolutePath().toString())
            .build();
        final Path indexDataPath = dataPath.resolve("index");
        final Path blobStorePath = PathUtils.get(createTempDir().toString());
        try (
            NodeEnvironment nodeEnvironment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            SharedBlobCacheService<FileCacheKey> sharedBlobCacheService = new SharedBlobCacheService<>(
                nodeEnvironment,
                settings,
                threadPool,
                Stateless.SHARD_READ_THREAD_POOL,
                BlobCacheMetrics.NOOP
            );
            FsBlobStore blobStore = new FsBlobStore(randomIntBetween(1, 8) * 1024, blobStorePath, false);
            IndexDirectory indexDirectory = new IndexDirectory(
                newFSDirectory(indexDataPath),
                new SearchDirectory(sharedBlobCacheService, shardId, null)
            )
        ) {
            final FsBlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.EMPTY, blobStorePath);
            indexDirectory.getSearchDirectory().setBlobContainer(value -> blobContainer);

            final int fileLength = randomIntBetween(1024, 10240);
            assertThat(fileLength, greaterThanOrEqualTo(BlobCacheBufferedIndexInput.BUFFER_SIZE));
            final byte[] bytes = randomByteArrayOfLength(fileLength);

            final String fileName = "file." + randomFrom(LuceneFilesExtensions.values()).getExtension();
            try (IndexOutput output = indexDirectory.createOutput(fileName, IOContext.DEFAULT)) {
                output.writeBytes(bytes, fileLength);
            }
            assertThat(indexDirectory.fileLength(fileName), equalTo((long) fileLength));

            final IndexInput input = indexDirectory.openInput(fileName, IOContext.DEFAULT);
            final IndexInput clone = input.clone();
            indexDirectory.deleteFile(fileName);

            assertThat(indexDirectory.getDelegate().fileLength(fileName), equalTo((long) fileLength));
            expectThrows(NoSuchFileException.class, () -> indexDirectory.fileLength(fileName));

            randomRead(input, bytes);
            randomRead(clone, bytes);

            input.close();

            expectThrows(NoSuchFileException.class, () -> indexDirectory.getDelegate().fileLength(fileName));
            expectThrows(NoSuchFileException.class, () -> indexDirectory.fileLength(fileName));

        } finally {
            assertTrue(ThreadPool.terminate(threadPool, 10L, TimeUnit.SECONDS));
        }
    }

    public void testFileHandleIsClosedAfterUpload() throws IOException {
        // we can only verify non memory mapped files
        final String fileName = "file."
            + randomFrom(
                Arrays.stream(LuceneFilesExtensions.values())
                    .filter(ext -> ext.shouldMmap() == false)
                    .map(LuceneFilesExtensions::getExtension)
                    .toList()
            );

        final FileSystem fileSystem = PathUtils.getDefaultFileSystem();
        final Set<String> openHandles = ConcurrentCollections.newConcurrentSet();
        final FilterFileSystemProvider provider = new HandleTrackingFS("handletrackingfs://", fileSystem) {
            @Override
            protected void onOpen(Path path, Object stream) {
                var file = path.getFileName().toString();
                if (file.equals(fileName)) {
                    var added = openHandles.add(file);
                    assert added : file + " already opened";
                }
            }

            @Override
            protected void onClose(Path path, Object stream) {
                var file = path.getFileName().toString();
                if (file.equals(fileName)) {
                    var removed = openHandles.remove(file);
                    assert removed : file + " already removed";
                }
            }
        };

        final long primaryTerm = randomLongBetween(1L, 1000L);
        PathUtilsForTesting.installMock(provider.getFileSystem(null));
        final Path path = PathUtils.get(createTempDir().toString());
        try (
            var node = new FakeStatelessNode(
                settings -> TestEnvironment.newEnvironment(
                    Settings.builder().put(settings).put(Environment.PATH_HOME_SETTING.getKey(), path.toAbsolutePath()).build()
                ),
                settings -> {
                    var build = Settings.builder().put(settings).put(Environment.PATH_HOME_SETTING.getKey(), path.toAbsolutePath()).build();
                    return new NodeEnvironment(build, TestEnvironment.newEnvironment(build));
                },
                xContentRegistry(),
                primaryTerm
            )
        ) {
            final var indexDirectory = node.indexingDirectory;
            final var indexPath = node.environment.dataFiles()[0].resolve(node.shardId.getIndex().getUUID()).resolve("0").resolve("index");

            final byte[] bytes = randomByteArrayOfLength(4096);
            try (IndexOutput output = indexDirectory.createOutput(fileName, IOContext.DEFAULT)) {
                output.writeBytes(bytes, bytes.length);
            }
            indexDirectory.getSearchDirectory()
                .getBlobContainer(primaryTerm)
                .writeBlob(OperationPurpose.INDICES, "blob_" + fileName, BytesReference.fromByteBuffer(ByteBuffer.wrap(bytes)), true);

            var input = indexDirectory.openInput(fileName, IOContext.DEFAULT);
            assertThat(input, instanceOf(IndexDirectory.ReopeningIndexInput.class));
            assertThat(((IndexDirectory.ReopeningIndexInput) input).getDelegate().isCached(), equalTo(false));
            readNBytes(input, bytes, 0L, 1024);

            var clone = input.clone();
            assertThat(clone, instanceOf(IndexDirectory.ReopeningIndexInput.class));
            assertThat(((IndexDirectory.ReopeningIndexInput) clone).getDelegate().isCached(), equalTo(false));
            readNBytes(clone, bytes, input.getFilePointer(), 1024);

            var slice = clone.slice("slice", 512L, 2048);
            assertThat(slice, instanceOf(IndexDirectory.ReopeningIndexInput.class));
            assertThat(((IndexDirectory.ReopeningIndexInput) slice).getDelegate().isCached(), equalTo(false));
            readNBytes(slice, bytes, 512L, 1024);

            assertThat(fileName, openHandles.contains(fileName), equalTo(true));
            assertThat(Files.exists(indexPath.resolve(fileName)), equalTo(true));
            assertThat(indexDirectory.getDelegate().fileLength(fileName), equalTo((long) bytes.length));

            indexDirectory.updateCommit(
                new StatelessCompoundCommit(
                    indexDirectory.getSearchDirectory().getShardId(),
                    1L,
                    1L,
                    "_na_",
                    Map.of(fileName, new BlobLocation(primaryTerm, "blob_" + fileName, bytes.length, 0L, bytes.length))
                ),
                Set.of(fileName)
            );

            readNBytes(input, bytes, input.getFilePointer(), 1024);

            assertThat(openHandles.contains(fileName), equalTo(false));
            assertThat(Files.exists(indexPath.resolve(fileName)), equalTo(false));
            expectThrows(NoSuchFileException.class, () -> indexDirectory.getDelegate().fileLength(fileName));
            assertThat(((IndexDirectory.ReopeningIndexInput) input).getDelegate().isCached(), equalTo(true));
            assertThat(((IndexDirectory.ReopeningIndexInput) clone).getDelegate().isCached(), equalTo(false));
            assertThat(((IndexDirectory.ReopeningIndexInput) slice).getDelegate().isCached(), equalTo(false));

            if (randomBoolean()) {
                clone.seek(0L);
            } else {
                readNBytes(clone, bytes, clone.getFilePointer(), 1024);
            }
            assertThat(((IndexDirectory.ReopeningIndexInput) clone).getDelegate().isCached(), equalTo(true));

            if (randomBoolean()) {
                slice.seek(1024L);
            } else {
                readNBytes(slice, bytes, 512 + slice.getFilePointer(), 1024);
            }
            assertThat(((IndexDirectory.ReopeningIndexInput) slice).getDelegate().isCached(), equalTo(true));

            input.close();
        } finally {
            PathUtilsForTesting.teardown();
        }
    }

    private static TestThreadPool getThreadPool(String name) {
        return new TestThreadPool(name, Stateless.statelessExecutorBuilders(Settings.EMPTY, true));
    }

    private void randomRead(IndexInput input, byte[] bytes) throws IOException {
        int pos = (int) input.getFilePointer();
        int remaining = (int) (input.length() - pos);
        if (remaining > 1) {
            int len = randomIntBetween(1, remaining);
            byte[] actual = randomReadAndSlice(input, len);
            byte[] expected = new byte[len];
            System.arraycopy(bytes, Math.toIntExact(pos), expected, 0, len);
            assertArrayEquals(expected, actual);
        }
    }

    private void readNBytes(IndexInput input, byte[] bytes, long pos, int len) throws IOException {
        byte[] actual = new byte[len];
        input.readBytes(actual, 0, actual.length);
        byte[] expected = new byte[len];
        System.arraycopy(bytes, Math.toIntExact(pos), expected, 0, len);
        assertArrayEquals(expected, actual);
    }

    /**
     * An {@link IndexInput} that simulates the upload of a file after a given number of operations has been executed
     */
    private static class SimulateUploadIndexInput extends FilterIndexInput {

        private final String fileName;
        private final long fileLength;
        private final IndexDirectory directory;
        private final CountDown operationsCountDown;
        private final BlobContainer blobContainer;

        private boolean isClone;

        private SimulateUploadIndexInput(
            String fileName,
            long fileLength,
            IndexDirectory directory,
            IndexInput input,
            CountDown operationsCountDown,
            BlobContainer blobContainer
        ) {
            super("random", input);
            this.fileName = fileName;
            this.fileLength = fileLength;
            this.directory = directory;
            this.operationsCountDown = operationsCountDown;
            this.blobContainer = blobContainer;
        }

        private void maybeUpload() {
            if (operationsCountDown.isCountedDown() == false && operationsCountDown.countDown()) {
                upload();
            }
        }

        private void upload() {
            assertThat(operationsCountDown.isCountedDown(), equalTo(true));
            try (IndexInput input = directory.openInput(fileName, IOContext.READONCE)) {
                final long length = input.length();
                final String blobName = "_blob_" + fileName;
                final InputStream inputStream = new InputStreamIndexInput(input, length);
                blobContainer.writeBlob(randomFrom(OperationPurpose.values()), blobName, inputStream, length, randomBoolean());

                directory.updateCommit(
                    new StatelessCompoundCommit(
                        directory.getSearchDirectory().getShardId(),
                        2L,
                        1L,
                        "_na_",
                        Map.of(fileName, new BlobLocation(1L, blobName, length, 0L, length))
                    ),
                    null
                );
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }

        public boolean isCountedDown() {
            return operationsCountDown.isCountedDown();
        }

        @Override
        public byte readByte() throws IOException {
            maybeUpload();
            var read = super.readByte();
            maybeUpload();
            return read;
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            maybeUpload();
            super.readBytes(b, offset, len);
            maybeUpload();
        }

        @Override
        public void seek(long pos) throws IOException {
            maybeUpload();
            super.seek(pos);
            maybeUpload();
        }

        @Override
        public IndexInput clone() {
            maybeUpload();
            var clone = new SimulateUploadIndexInput(fileName, fileLength, directory, in.clone(), operationsCountDown, blobContainer);
            clone.isClone = true;
            maybeUpload();
            return clone;
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
            maybeUpload();
            var slice = new SimulateUploadIndexInput(
                fileName,
                fileLength,
                directory,
                in.slice(sliceDescription, offset, length),
                operationsCountDown,
                blobContainer
            );
            slice.isClone = true;
            maybeUpload();
            return slice;
        }

        @Override
        public void close() throws IOException {
            maybeUpload();
            if (isClone == false) {
                super.close();
            }
        }
    }
}
