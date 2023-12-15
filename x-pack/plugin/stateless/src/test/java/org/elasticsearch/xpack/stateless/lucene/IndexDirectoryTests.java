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

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.mockfile.FilterFileChannel;
import org.apache.lucene.tests.mockfile.FilterFileSystemProvider;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.common.BlobCacheBufferedIndexInput;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.fs.FsBlobContainer;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.PathUtilsForTesting;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.LuceneFilesExtensions;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystem;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class IndexDirectoryTests extends ESTestCase {

    public void testDisablesFsync() throws IOException {
        final FileSystem fileSystem = PathUtils.getDefaultFileSystem();
        final FilterFileSystemProvider provider = new FilterFileSystemProvider("mock://", fileSystem) {
            @Override
            public FileChannel newFileChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
                return new FilterFileChannel(super.newFileChannel(path, options, attrs)) {
                    @Override
                    public void force(boolean metaData) {
                        throw new AssertionError("fsync should be disabled by the index directory");
                    }
                };
            }
        };
        PathUtilsForTesting.installMock(provider.getFileSystem(null));
        final Path path = PathUtils.get(createTempDir().toString());
        try (
            Directory directory = new IndexDirectory(FSDirectory.open(path), new SearchDirectory(null, null));
            IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig())
        ) {
            indexWriter.commit();
        } finally {
            PathUtilsForTesting.teardown();
        }
    }

    public void testEstimateSizeInBytes() throws Exception {
        final Path dataPath = createTempDir();
        final ShardId shardId = new ShardId(new Index("_index_name", "_index_id"), 0);
        final ThreadPool threadPool = getThreadPool("testEstimateSizeInBytes");
        final var settings = Settings.builder()
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofKb(4L))
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofKb(4L))
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
            IndexDirectory directory = new IndexDirectory(
                newFSDirectory(indexDataPath),
                new SearchDirectory(sharedBlobCacheService, shardId)
            )
        ) {
            final FsBlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.EMPTY, blobStorePath);
            directory.getSearchDirectory().setBlobContainer(value -> blobContainer);

            var filesSizes = new HashMap<String, Integer>();
            var iters = randomIntBetween(1, 50);

            for (int i = 0; i < iters; i++) {
                String name = "file_" + i;
                int length = randomIntBetween(16, 2048);
                try (
                    IndexOutput output = randomBoolean()
                        ? directory.createOutput(name, IOContext.DEFAULT)
                        : directory.createTempOutput(name, ".tmp", IOContext.DEFAULT)
                ) {
                    output.writeBytes(randomByteArrayOfLength(length), length);
                    filesSizes.put(output.getName(), length);
                }
                if (randomBoolean()) {
                    continue; // accumulate file on disk
                }

                switch (randomInt(3)) {
                    case 0 ->
                    // single deletion
                    {
                        var fileName = randomFrom(filesSizes.keySet());
                        var sizeBeforeDeletion = directory.estimateSizeInBytes();
                        directory.deleteFile(fileName);
                        var fileLength = filesSizes.remove(fileName);
                        assertThat(directory.estimateSizeInBytes(), equalTo(sizeBeforeDeletion - fileLength));
                    }
                    case 1 ->
                    // single upload
                    {
                        var fileName = randomFrom(filesSizes.keySet());
                        var totalSize = directory.estimateSizeInBytes();
                        directory.updateCommit(
                            new StatelessCompoundCommit(
                                directory.getSearchDirectory().getShardId(),
                                2L,
                                1L,
                                "_na_",
                                Map.of(fileName, new BlobLocation(1L, "_blob", length, 0L, length))
                            ),
                            null
                        );
                        var fileLength = filesSizes.remove(fileName);
                        assertThat(directory.estimateSizeInBytes(), equalTo(totalSize - fileLength));
                    }
                    case 2 ->
                    // multiple deletions
                    {
                        var fileNames = randomSubsetOf(filesSizes.keySet());
                        var sizeBeforeDeletion = directory.estimateSizeInBytes();
                        var totalSize = 0L;
                        for (String fileName : fileNames) {
                            try {
                                var currentSize = directory.estimateSizeInBytes();
                                directory.deleteFile(fileName);
                                var fileLength = filesSizes.remove(fileName);
                                totalSize += fileLength;
                                assertThat(directory.estimateSizeInBytes(), equalTo(currentSize - fileLength));
                            } catch (IOException e) {
                                throw new AssertionError(e);
                            }
                        }
                        assertThat(directory.estimateSizeInBytes(), equalTo(sizeBeforeDeletion - totalSize));

                    }
                    case 3 ->
                    // multiple uploads
                    {
                        var files = randomSubsetOf(filesSizes.entrySet());
                        var sizeBeforeUpload = directory.estimateSizeInBytes();
                        var totalSize = files.stream().mapToLong(Map.Entry::getValue).sum();
                        directory.updateCommit(
                            new StatelessCompoundCommit(
                                directory.getSearchDirectory().getShardId(),
                                2L,
                                1L,
                                "_na_",
                                files.stream()
                                    .collect(
                                        Collectors.toMap(
                                            Map.Entry::getKey,
                                            o -> new BlobLocation(1L, "blob_" + o.getKey(), o.getValue(), 0L, o.getValue())
                                        )
                                    )
                            ),
                            null
                        );
                        files.forEach(file -> filesSizes.remove(file.getKey()));
                        assertThat(directory.estimateSizeInBytes(), equalTo(sizeBeforeUpload - totalSize));
                    }
                    default -> throw new AssertionError("Illegal randomization branch");
                }
                assertThat(directory.estimateSizeInBytes(), equalTo(filesSizes.values().stream().mapToLong(value -> value).sum()));
            }
        } finally {
            assertTrue(ThreadPool.terminate(threadPool, 10L, TimeUnit.SECONDS));
        }
    }

    public void testEstimateSizeInBytesAfterDeletion() throws Exception {
        final Path dataPath = createTempDir();
        final ShardId shardId = new ShardId(new Index("_index_name", "_index_id"), 0);
        final ThreadPool threadPool = getThreadPool("testEstimateSizeInBytes");
        final var settings = Settings.builder()
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofKb(4L))
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofKb(4L))
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
            IndexDirectory directory = new IndexDirectory(
                newFSDirectory(indexDataPath),
                new SearchDirectory(sharedBlobCacheService, shardId)
            )
        ) {
            final FsBlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.EMPTY, blobStorePath);
            directory.getSearchDirectory().setBlobContainer(value -> blobContainer);

            final int fileLength = randomIntBetween(1024, 10240);
            assertThat(fileLength, greaterThanOrEqualTo(BlobCacheBufferedIndexInput.BUFFER_SIZE));

            final String fileName = "file." + randomFrom(LuceneFilesExtensions.values()).getExtension();
            try (IndexOutput output = directory.createOutput(fileName, IOContext.DEFAULT)) {
                output.writeBytes(randomByteArrayOfLength(fileLength), fileLength);
            }
            assertThat(directory.fileLength(fileName), equalTo((long) fileLength));
            final long sizeBeforeDeletion = directory.estimateSizeInBytes();

            final IndexInput input = directory.openInput(fileName, IOContext.DEFAULT);
            directory.deleteFile(fileName);

            assertThat(
                "File is deleted but not closed, estimate of directory size is unchanged",
                directory.estimateSizeInBytes(),
                equalTo(sizeBeforeDeletion)
            );
            expectThrows(
                NoSuchFileException.class,
                "File is deleted, accessing file length should throw",
                () -> directory.fileLength(fileName)
            );
            assertThat(
                "File is deleted but not closed, it still exists in the delegate directory",
                directory.getDelegate().fileLength(fileName),
                equalTo((long) fileLength)
            );

            input.close();

            assertThat(
                "File is deleted and closed, estimate of directory size has changed",
                directory.estimateSizeInBytes(),
                equalTo(sizeBeforeDeletion - fileLength)
            );
            expectThrows(
                NoSuchFileException.class,
                "File is deleted, accessing file length should throw",
                () -> directory.fileLength(fileName)
            );
            expectThrows(
                NoSuchFileException.class,
                "File is deleted, accessing file length should  for the delegate directory too",
                () -> directory.getDelegate().fileLength(fileName)
            );

        } finally {
            assertTrue(ThreadPool.terminate(threadPool, 10L, TimeUnit.SECONDS));
        }
    }

    public void testPruneSearchDirectoryMetadata() throws IOException {
        final Path dataPath = createTempDir();
        final ShardId shardId = new ShardId(new Index("_index_name", "_index_id"), 0);
        final ThreadPool threadPool = getThreadPool("testEstimateSizeInBytes");
        final var settings = Settings.builder()
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofKb(4L))
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofKb(4L))
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
            IndexDirectory directory = new IndexDirectory(
                newFSDirectory(indexDataPath),
                new SearchDirectory(sharedBlobCacheService, shardId)
            )
        ) {
            final FsBlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.EMPTY, blobStorePath);
            directory.getSearchDirectory().setBlobContainer(value -> blobContainer);

            Set<String> files = randomSet(1, 10, () -> randomAlphaOfLength(10));

            StatelessCompoundCommit commit = createCommit(directory, files, 2L);

            directory.updateCommit(commit, null);
            assertEquals(files, Set.of(directory.getSearchDirectory().listAll()));
            Set<String> newFiles = randomSet(1, 10, () -> randomAlphaOfLength(10));
            newFiles.removeAll(files);

            StatelessCompoundCommit newCommit = createCommit(directory, newFiles, 3L);
            directory.updateCommit(newCommit, Sets.union(files, newFiles));

            assertEquals(Sets.union(files, newFiles), Set.of(directory.getSearchDirectory().listAll()));

            directory.updateCommit(newCommit, Sets.union(newFiles, files));

            assertEquals(Sets.union(files, newFiles), Set.of(directory.getSearchDirectory().listAll()));

            directory.updateCommit(newCommit, newFiles);

            assertEquals(newFiles, Set.of(directory.getSearchDirectory().listAll()));
        } finally {
            assertTrue(ThreadPool.terminate(threadPool, 10L, TimeUnit.SECONDS));
        }
    }

    private static TestThreadPool getThreadPool(String name) {
        return new TestThreadPool(name, Stateless.statelessExecutorBuilders(Settings.EMPTY, true));
    }

    private static StatelessCompoundCommit createCommit(IndexDirectory directory, Set<String> files, long generation) {
        StatelessCompoundCommit commit = new StatelessCompoundCommit(
            directory.getSearchDirectory().getShardId(),
            generation,
            1L,
            "_na_",
            files.stream()
                .collect(
                    Collectors.toMap(Function.identity(), o -> new BlobLocation(1L, "blob_" + o, between(100, 200), 0L, between(1, 100)))
                )
        );
        return commit;
    }
}
