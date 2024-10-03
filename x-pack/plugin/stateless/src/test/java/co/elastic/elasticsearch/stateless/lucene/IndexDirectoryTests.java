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
 *
 * This file was contributed to by generative AI
 */

package co.elastic.elasticsearch.stateless.lucene;

import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.cache.StatelessSharedBlobCacheService;
import co.elastic.elasticsearch.stateless.commits.BatchedCompoundCommit;
import co.elastic.elasticsearch.stateless.commits.BlobFileRanges;
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
import org.elasticsearch.blobcache.common.BlobCacheBufferedIndexInput;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.fs.FsBlobContainer;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.PathUtilsForTesting;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.LuceneFilesExtensions;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystem;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static co.elastic.elasticsearch.stateless.TestUtils.newCacheService;
import static co.elastic.elasticsearch.stateless.commits.BlobLocationTestUtils.createBlobFileRanges;
import static co.elastic.elasticsearch.stateless.commits.BlobLocationTestUtils.createBlobLocation;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

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
            Directory directory = new IndexDirectory(FSDirectory.open(path), new IndexBlobStoreCacheDirectory(null, null), null);
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
            StatelessSharedBlobCacheService sharedBlobCacheService = newCacheService(nodeEnvironment, settings, threadPool);
            FsBlobStore blobStore = new FsBlobStore(randomIntBetween(1, 8) * 1024, blobStorePath, false);
            IndexDirectory directory = new IndexDirectory(
                newFSDirectory(indexDataPath),
                new IndexBlobStoreCacheDirectory(sharedBlobCacheService, shardId),
                null
            )
        ) {
            final FsBlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.EMPTY, blobStorePath);
            directory.getBlobStoreCacheDirectory().setBlobContainer(value -> blobContainer);

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
                            2L,
                            length,
                            Set.of(fileName),
                            Map.of(fileName, createBlobFileRanges(1L, fileName.hashCode(), 0L, length))
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
                            2L,
                            files.stream().mapToLong(Map.Entry::getValue).sum(),
                            files.stream().map(Map.Entry::getKey).collect(Collectors.toSet()),
                            files.stream()
                                .collect(
                                    Collectors.toMap(
                                        Map.Entry::getKey,
                                        o -> createBlobFileRanges(1L, o.getKey().hashCode(), 0L, o.getValue())
                                    )
                                )
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
            StatelessSharedBlobCacheService sharedBlobCacheService = newCacheService(nodeEnvironment, settings, threadPool);
            FsBlobStore blobStore = new FsBlobStore(randomIntBetween(1, 8) * 1024, blobStorePath, false);
            IndexDirectory directory = new IndexDirectory(
                newFSDirectory(indexDataPath),
                new IndexBlobStoreCacheDirectory(sharedBlobCacheService, shardId),
                null
            )
        ) {
            final FsBlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.EMPTY, blobStorePath);
            directory.getBlobStoreCacheDirectory().setBlobContainer(value -> blobContainer);

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
            StatelessSharedBlobCacheService sharedBlobCacheService = newCacheService(nodeEnvironment, settings, threadPool);
            FsBlobStore blobStore = new FsBlobStore(randomIntBetween(1, 8) * 1024, blobStorePath, false);
            IndexDirectory directory = new IndexDirectory(
                newFSDirectory(indexDataPath),
                new IndexBlobStoreCacheDirectory(sharedBlobCacheService, shardId),
                null
            )
        ) {
            final FsBlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.EMPTY, blobStorePath);
            directory.getBlobStoreCacheDirectory().setBlobContainer(value -> blobContainer);

            Set<String> files = randomSet(1, 10, () -> randomAlphaOfLength(10));

            StatelessCompoundCommit commit = createCommit(directory, files, 2L);

            directory.updateRecoveryCommit(
                commit.generation(),
                commit.nodeEphemeralId(),
                commit.translogRecoveryStartFile(),
                commit.sizeInBytes(),
                Maps.transformValues(commit.commitFiles(), BlobFileRanges::new)
            );
            assertEquals(files, Set.of(directory.getBlobStoreCacheDirectory().listAll()));
            Set<String> newFiles = randomSet(1, 10, () -> randomAlphaOfLength(10));
            newFiles.removeAll(files);

            StatelessCompoundCommit newCommit = createCommit(directory, newFiles, 3L);
            directory.updateCommit(
                3L,
                newCommit.getAllFilesSizeInBytes(),
                newFiles,
                Stream.concat(commit.commitFiles().entrySet().stream(), newCommit.commitFiles().entrySet().stream())
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> new BlobFileRanges(e.getValue())))
            );

            assertEquals(Sets.union(files, newFiles), Set.of(directory.getBlobStoreCacheDirectory().listAll()));

            directory.updateCommit(
                3L,
                newCommit.getAllFilesSizeInBytes(),
                newFiles,
                Stream.concat(commit.commitFiles().entrySet().stream(), newCommit.commitFiles().entrySet().stream())
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> new BlobFileRanges(e.getValue())))
            );

            assertEquals(Sets.union(files, newFiles), Set.of(directory.getBlobStoreCacheDirectory().listAll()));

            directory.updateCommit(
                3,
                newCommit.getAllFilesSizeInBytes(),
                newFiles,
                Maps.transformValues(newCommit.commitFiles(), BlobFileRanges::new)
            );

            assertEquals(newFiles, Set.of(directory.getBlobStoreCacheDirectory().listAll()));
        } finally {
            assertTrue(ThreadPool.terminate(threadPool, 10L, TimeUnit.SECONDS));
        }
    }

    public void testOnGenerationalFileDeletion() throws IOException {
        final Path path = PathUtils.get(createTempDir().toString());
        final ShardId shardId = new ShardId(new Index(randomIdentifier(), randomUUID()), between(0, 10));
        final List<Tuple<ShardId, String>> capturedOnDeletions = new ArrayList<>();
        final IndexBlobStoreCacheDirectory indexBlobStoreCacheDirectory = new IndexBlobStoreCacheDirectory(null, shardId);
        indexBlobStoreCacheDirectory.setBlobContainer(ignore -> mock(BlobContainer.class));
        try (
            IndexDirectory directory = new IndexDirectory(
                FSDirectory.open(path),
                indexBlobStoreCacheDirectory,
                (shardId1, filename) -> capturedOnDeletions.add(new Tuple<>(shardId1, filename))
            )
        ) {
            final int fileLength = between(50, 100);
            final String generationalFilename = randomFrom("_0_1.fnm", "_0_1_Lucene90_0.dvd", "_0_1_Lucene90_0.dvm");
            try (IndexOutput output = directory.createOutput(generationalFilename, IOContext.DEFAULT)) {
                final byte[] bytes = randomByteArrayOfLength(fileLength);
                output.writeBytes(bytes, bytes.length);
            }
            // randomly mark the generational file as uploaded
            if (randomBoolean()) {
                directory.updateCommit(
                    1,
                    fileLength,
                    Set.of(generationalFilename),
                    Map.of(generationalFilename, createBlobFileRanges(1L, 4L, 0L, fileLength))
                );
            }
            assertThat(capturedOnDeletions, empty());
            directory.deleteFile(generationalFilename);
            assertThat(capturedOnDeletions, contains(new Tuple<>(shardId, generationalFilename)));
            capturedOnDeletions.clear();

            // only the first deletion works
            expectThrows(FileNotFoundException.class, () -> directory.deleteFile(generationalFilename));
            assertThat(capturedOnDeletions, empty());

            // no callback for non-generational file
            final String otherFilename = randomFrom("segments_6", "_0.cfe", "_0.cfs", "_2.si");
            try (IndexOutput output = directory.createOutput(otherFilename, IOContext.DEFAULT)) {
                final byte[] bytes = randomByteArrayOfLength(between(50, 100));
                output.writeBytes(bytes, bytes.length);
            }
            directory.deleteFile(otherFilename);
            assertThat(capturedOnDeletions, empty());
        }
    }

    public void testUpdateRecoveryCommit() throws IOException {
        final Path path = PathUtils.get(createTempDir().toString());
        final ShardId shardId = new ShardId(new Index(randomIdentifier(), randomUUID()), between(0, 10));
        final IndexBlobStoreCacheDirectory indexBlobStoreCacheDirectory = new IndexBlobStoreCacheDirectory(null, shardId);
        indexBlobStoreCacheDirectory.setBlobContainer(ignore -> mock(BlobContainer.class));
        try (IndexDirectory directory = new IndexDirectory(FSDirectory.open(path), indexBlobStoreCacheDirectory, null)) {
            Set<String> files = randomSet(1, 10, () -> randomAlphaOfLength(10));
            var recoveryCommit = createCommit(directory, files, 4L);
            directory.updateRecoveryCommit(
                recoveryCommit.generation(),
                recoveryCommit.nodeEphemeralId(),
                recoveryCommit.translogRecoveryStartFile(),
                recoveryCommit.sizeInBytes(),
                BlobFileRanges.computeLastCommitBlobFileRanges(
                    new BatchedCompoundCommit(recoveryCommit.primaryTermAndGeneration(), List.of(recoveryCommit)),
                    randomBoolean()
                )
            );

            assertThat(directory.getRecoveryCommitMetadataNodeEphemeralId().isPresent(), is(true));
            assertThat(directory.getRecoveryCommitMetadataNodeEphemeralId().get(), is(equalTo(recoveryCommit.nodeEphemeralId())));

            assertThat(directory.getTranslogRecoveryStartFile(), is(equalTo(recoveryCommit.translogRecoveryStartFile())));
            assertEquals(files, Set.of(indexBlobStoreCacheDirectory.listAll()));
        }
    }

    private static TestThreadPool getThreadPool(String name) {
        return new TestThreadPool(name, Stateless.statelessExecutorBuilders(Settings.EMPTY, true));
    }

    private static StatelessCompoundCommit createCommit(IndexDirectory directory, Set<String> files, long generation) {
        Map<String, BlobLocation> commitFiles = files.stream()
            .collect(Collectors.toMap(Function.identity(), o -> createBlobLocation(1L, o.hashCode(), 0L, between(1, 100))));
        return new StatelessCompoundCommit(
            directory.getBlobStoreCacheDirectory().getShardId(),
            generation,
            1L,
            "_na_",
            commitFiles,
            commitFiles.values().stream().mapToLong(BlobLocation::fileLength).sum(),
            files
        );
    }
}
