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
import co.elastic.elasticsearch.stateless.cache.StatelessSharedBlobCacheService;
import co.elastic.elasticsearch.stateless.cache.reader.CacheBlobReader;
import co.elastic.elasticsearch.stateless.cache.reader.CacheBlobReaderService;
import co.elastic.elasticsearch.stateless.cache.reader.MutableObjectStoreUploadTracker;
import co.elastic.elasticsearch.stateless.cache.reader.ObjectStoreUploadTracker;
import co.elastic.elasticsearch.stateless.commits.BlobLocation;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.test.FakeStatelessNode;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.LongFunction;
import java.util.stream.Collectors;

import static org.elasticsearch.blobcache.BlobCacheUtils.toIntBytes;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;

public class SearchDirectoryTests extends ESTestCase {

    @Override
    public String[] tmpPaths() {
        // cache requires a single data path
        return new String[] { createTempDir().toAbsolutePath().toString() };
    }

    /**
     * Test that SearchIndexInput can be read from the cache while the blob in object store keeps growing in size.
     *
     * In production, the batched compound commits are expanded in cache by appending compound commits. For simplicity, this test appends
     * Lucene files instead.
     */
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch-serverless/issues/1499")
    public void testExpandingCacheRegions() throws Exception {
        var regionSize = ByteSizeValue.ofBytes(SharedBytes.PAGE_SIZE * randomLongBetween(1L, 4L)); // regions are 4kb..16kb
        var cacheSize = ByteSizeValue.ofBytes(regionSize.getBytes() * 100L); // 100 regions

        logger.debug("--> using cache size [{}] with region size [{}]", cacheSize, regionSize);
        try (var node = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry()) {
            @Override
            protected Settings nodeSettings() {
                return Settings.builder()
                    .put(super.nodeSettings())
                    .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), cacheSize)
                    .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), regionSize)
                    .build();
            }

            @Override
            protected SearchDirectory createSearchDirectory(
                StatelessSharedBlobCacheService sharedCacheService,
                ShardId shardId,
                CacheBlobReaderService cacheBlobReaderService,
                MutableObjectStoreUploadTracker objectStoreUploadTracker
            ) {
                var customCacheBlobReaderService = new CacheBlobReaderService(sharedCacheService, client) {
                    @Override
                    public CacheBlobReader getCacheBlobReader(
                        ShardId shardId,
                        LongFunction<BlobContainer> blobContainer,
                        BlobLocation location,
                        ObjectStoreUploadTracker objectStoreUploadTracker
                    ) {
                        var originalCacheBlobReader = cacheBlobReaderService.getCacheBlobReader(
                            shardId,
                            blobContainer,
                            location,
                            objectStoreUploadTracker
                        );
                        return new CacheBlobReader() {
                            @Override
                            public ByteRange getRange(long position, int length) {
                                var start = BlobCacheUtils.toPageAlignedSize(Math.max(position - SharedBytes.PAGE_SIZE + 1L, 0L));
                                var end = BlobCacheUtils.toPageAlignedSize(position + length);
                                return ByteRange.of(start, end);
                            }

                            @Override
                            public InputStream getRangeInputStream(long position, int length) throws IOException {
                                return originalCacheBlobReader.getRangeInputStream(position, length);
                            }
                        };
                    }
                };
                return super.createSearchDirectory(sharedCacheService, shardId, customCacheBlobReaderService, objectStoreUploadTracker);
            }

            @Override
            protected StatelessSharedBlobCacheService createCacheService(
                NodeEnvironment nodeEnvironment,
                Settings settings,
                ThreadPool threadPool
            ) {
                return new StatelessSharedBlobCacheService(
                    nodeEnvironment,
                    settings,
                    threadPool,
                    Stateless.SHARD_READ_THREAD_POOL,
                    BlobCacheMetrics.NOOP
                ) {
                    @Override
                    protected boolean assertOffsetsWithinFileLength(long offset, long length, long fileLength) {
                        // this test tries to read beyond the file length
                        return true;
                    }
                };
            }
        }) {
            final var primaryTerm = randomLongBetween(1L, 10L);
            final var indexDirectory = IndexDirectory.unwrapDirectory(node.indexingStore.directory());
            final var searchDirectory = indexDirectory.getSearchDirectory();
            final var blobContainer = searchDirectory.getBlobContainer(primaryTerm);
            final int minFileSize = CodecUtil.footerLength();

            final String blobName = "blob";
            long blobLength = 0L;
            long generation = 0L;

            final var files = new ArrayList<ChecksummedFile>();
            StatelessSharedBlobCacheService.CacheFile previousCacheFile = null;
            ChecksummedFile previousFile = null;
            IndexInput previousInput = null;
            long previousBlobLength = 0L;

            final int cacheSizeInBytes = toIntBytes(cacheSize.getBytes());
            final long regionSizeInBytes = regionSize.getBytes();

            // This test tries to avoid evictions by not fully writing the cache and leaves at least 1 free region:
            // this is important because we use tryRead() later to check that a region has been expanded.
            while (blobLength <= cacheSizeInBytes - regionSizeInBytes * 2) {
                // always leave a region free in cache
                int fileLength = randomIntBetween(minFileSize, toIntBytes(cacheSizeInBytes - regionSizeInBytes - blobLength));
                var fileName = randomIdentifier();
                var fileOffset = blobLength;

                logger.debug("--> write file [{}] of length [{}]", fileName, fileLength);
                try (var output = indexDirectory.createOutput(fileName, IOContext.DEFAULT)) {
                    byte[] randomBytes = randomByteArrayOfLength(fileLength - minFileSize);
                    output.writeBytes(randomBytes, randomBytes.length);
                    CodecUtil.writeFooter(output);
                }

                logger.debug("--> compute checksum of file [{}]", fileName);
                long fileChecksum;
                try (var input = indexDirectory.openInput(fileName, IOContext.DEFAULT)) {
                    fileChecksum = CodecUtil.retrieveChecksum(input);
                }

                final var fileLengthPlusPadding = BlobCacheUtils.toPageAlignedSize(fileLength);
                blobLength += fileLengthPlusPadding;

                logger.debug(
                    "--> add file [{}] of length [{}] to blob [{}] of length [{}] at offset [{}]",
                    fileName,
                    fileLength,
                    blobName,
                    blobLength,
                    fileOffset
                );

                var file = new ChecksummedFile(fileName, fileLength, fileOffset, fileChecksum, blobLength);
                files.add(file);

                logger.debug("--> now overwrite blob with [{}] files in object store", files.size());
                var stream = new ChecksummedFilesInputStream(node.indexingShardPath.resolveIndex(), List.copyOf(files));
                blobContainer.writeBlob(OperationPurpose.INDICES, blobName, stream, blobLength, false);
                assertThat(stream.bytesRead, equalTo(blobLength));

                logger.debug("--> update commit with file [{}]", fileName);
                // bypass index directory so that files remain on disk and can be read again by ChecksumedFilesInputStream
                searchDirectory.updateCommit(
                    new StatelessCompoundCommit(
                        node.shardId,
                        primaryTerm,
                        generation,
                        node.node.getId(),
                        files.stream()
                            .collect(
                                Collectors.toUnmodifiableMap(
                                    ChecksummedFile::fileName,
                                    f -> new BlobLocation(primaryTerm, blobName, f.fileOffset, f.fileLength)
                                )
                            ),
                        blobLength,
                        files.stream().map(ChecksummedFile::fileName).collect(Collectors.toSet())
                    ),
                    randomBoolean() ? null : new PrimaryTermAndGeneration(primaryTerm, generation)
                );
                generation += 1L;

                if (previousCacheFile != null && randomBoolean()) {
                    assertBusyCacheFile(previousCacheFile, previousFile, blobName, previousBlobLength);
                }

                logger.debug("--> open file [{}] from cache", fileName);
                var input = searchDirectory.openInput(file.fileName(), IOContext.DEFAULT);
                assertThat(input, instanceOf(SearchIndexInput.class));

                logger.debug("--> now fully read file [{}] from cache and verify its checksum", file.fileName());
                CodecUtil.checksumEntireFile(input);
                assertThat(CodecUtil.retrieveChecksum(input), equalTo(file.fileChecksum()));

                logger.debug("--> verify cache file");
                var cacheFile = SearchDirectoryTestUtils.getCacheFile((SearchIndexInput) input);
                assertBusyCacheFile(cacheFile, file, blobName, blobLength);

                if (previousFile != null) {
                    try {
                        ByteBuffer buffer = ByteBuffer.allocate(1);
                        logger.debug("--> verify previous file [{}] checksum again from cache", previousFile.fileName());
                        CodecUtil.checksumEntireFile(previousInput);
                        assertThat(CodecUtil.retrieveChecksum(previousInput), equalTo(previousFile.fileChecksum()));

                        logger.debug("--> verify previous cache file");
                        assertBusyCacheFile(previousCacheFile, previousFile, blobName, blobLength);

                        var length = BlobCacheUtils.toPageAlignedSize(previousFile.fileOffset() + previousFile.fileLength());
                        assertThat(length, equalTo(file.fileOffset()));
                        assertThat(length, lessThan(blobLength));

                        logger.debug("--> verify that previous cache file ending region has been expanded");
                        assertTrue(
                            "Data is available beyond previous cache file's length",
                            previousCacheFile.tryRead(buffer, previousCacheFile.getLength())
                        );
                        buffer.clear();
                    } finally {
                        IOUtils.close(previousInput);
                    }
                }

                previousBlobLength = blobLength;
                previousCacheFile = cacheFile;
                previousInput = input;
                previousFile = file;
            }
            assertThat(indexDirectory.getSearchDirectory().getCacheService().getStats().evictCount(), equalTo(0L));
        }
    }

    private static void assertBusyCacheFile(
        StatelessSharedBlobCacheService.CacheFile cacheFile,
        ChecksummedFile file,
        String blobName,
        long blobLength
    ) throws Exception {
        // There is a race between cacheFile.tryRead() and the thread that completes the writing of the last region in cache, potentially
        // making tryRead() not work the first time, so we use assertBusy to retry.
        //
        // The race is between the completion of the gap to fill, which updates the SparseFileTracker's {@code complete} volatile field when
        // the last range is completed, and the tryRead() method that uses SparseFileTracker's {@code checkAvailable} method that also reads
        // the {@code complete} volatile field.
        assertBusy(() -> {
            assertThat("Cache key refers to the single blob", cacheFile.getCacheKey().fileName(), equalTo(blobName));

            ByteBuffer buffer = ByteBuffer.allocate(1);
            long position = file.fileOffset();
            assertThat("File's offset is page aligned", position % SharedBytes.PAGE_SIZE, equalTo(0L));
            assertTrue("File's first byte is available in cache", cacheFile.tryRead(buffer, position));
            buffer.clear();

            position = file.fileOffset() + file.fileLength() - 1L;
            assertTrue("File's last byte is available in cache", cacheFile.tryRead(buffer, position));
            buffer.clear();

            position = blobLength - 1L;
            assertTrue("Blob's last region byte (including padding) is available cache", cacheFile.tryRead(buffer, position));
            buffer.clear();
        });
    }

    private record ChecksummedFile(String fileName, long fileLength, long fileOffset, long fileChecksum, long blobLength) {}

    private static class ChecksummedFilesInputStream extends InputStream {

        private final Iterator<ChecksummedFile> iterator;
        private final Path directory;

        private InputStream current;
        private long bytesRead;

        private ChecksummedFilesInputStream(Path directory, List<ChecksummedFile> commitsFiles) {
            this.iterator = commitsFiles.iterator();
            this.directory = directory;
        }

        private InputStream nextStream() {
            var next = iterator.next();
            try {
                InputStream stream = new BufferedInputStream(Files.newInputStream(directory.resolve(next.fileName())));
                // check if some padding bytes are needed, and if so add them now
                int padding = Math.toIntExact(next.blobLength() - next.fileLength() - next.fileOffset());
                if (padding > 0) {
                    byte[] bytes = new byte[padding];
                    Arrays.fill(bytes, (byte) 0);
                    stream = new SequenceInputStream(stream, new ByteArrayInputStream(bytes));
                }
                return stream;
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }

        private void closeStreamAndNullify() throws IOException {
            try {
                IOUtils.close(current);
            } finally {
                current = null;
            }
        }

        @Override
        public int read() throws IOException {
            if (current == null) {
                if (iterator.hasNext() == false) {
                    return -1;
                }
                current = nextStream();
            }
            int read = current.read();
            if (read == -1) {
                closeStreamAndNullify();
                return read();
            }
            bytesRead += 1;
            return read;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (current == null) {
                if (iterator.hasNext() == false) {
                    return -1;
                }
                current = nextStream();
            }
            int read = current.read(b, off, len);
            if (read == -1) {
                closeStreamAndNullify();
                return read(b, off, len);
            }
            bytesRead += read;
            return read;
        }

        @Override
        public void close() throws IOException {
            closeStreamAndNullify();
        }
    }
}
