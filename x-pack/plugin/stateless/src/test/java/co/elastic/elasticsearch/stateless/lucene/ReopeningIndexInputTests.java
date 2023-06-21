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

import co.elastic.elasticsearch.stateless.commits.BlobLocation;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;

import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.fs.FsBlobContainer;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.lucene.store.ESIndexInputTestCase;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.blobcache.shared.SharedBytes.pageAligned;
import static org.elasticsearch.xpack.searchablesnapshots.AbstractSearchableSnapshotsTestCase.randomChecksumBytes;
import static org.elasticsearch.xpack.searchablesnapshots.AbstractSearchableSnapshotsTestCase.randomIOContext;

public class ReopeningIndexInputTests extends ESIndexInputTestCase {

    public void testRandomReads() throws IOException {
        final Path dataPath = createTempDir();
        final ShardId shardId = new ShardId(new Index("_index_name", "_index_id"), 0);
        final ThreadPool threadPool = new TestThreadPool("testRandomReads");
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
                ThreadPool.Names.GENERIC
            );
            FsBlobStore blobStore = new FsBlobStore(randomIntBetween(1, 8) * 1024, blobStorePath, false);
            IndexDirectory indexDirectory = new IndexDirectory(newFSDirectory(indexDataPath), sharedBlobCacheService, shardId)
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

                    if (input.isCountedDown()) {
                        expectThrowsAnyOf(
                            List.of(FileNotFoundException.class, NoSuchFileException.class),
                            () -> indexDirectory.getDelegate().fileLength(fileName)
                        );
                    }
                }
            }
        } finally {
            assertTrue(ThreadPool.terminate(threadPool, 10L, TimeUnit.SECONDS));
        }
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
                try (IndexInput input = directory.openInput(fileName, IOContext.READONCE)) {
                    final long length = input.length();
                    final String blobName = "_blob_" + fileName;
                    final InputStream inputStream = new InputStreamIndexInput(input, length);
                    blobContainer.writeBlob(blobName, inputStream, length, randomBoolean());

                    directory.updateCommit(
                        new StatelessCompoundCommit(
                            directory.getSearchDirectory().getShardId(),
                            2L,
                            1L,
                            "_na_",
                            Map.of(fileName, new BlobLocation(1L, blobName, length, 0L, length))
                        )
                    );
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
        }

        public boolean isCountedDown() {
            return operationsCountDown.isCountedDown();
        }

        @Override
        public byte readByte() throws IOException {
            maybeUpload();
            return super.readByte();
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            maybeUpload();
            super.readBytes(b, offset, len);
        }

        @Override
        public void seek(long pos) throws IOException {
            maybeUpload();
            super.seek(pos);
        }

        @Override
        public IndexInput clone() {
            maybeUpload();
            var clone = new SimulateUploadIndexInput(fileName, fileLength, directory, in.clone(), operationsCountDown, blobContainer);
            clone.isClone = true;
            return clone;
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
            return new SimulateUploadIndexInput(
                fileName,
                fileLength,
                directory,
                in.slice(sliceDescription, offset, length),
                operationsCountDown,
                blobContainer
            );
        }

        @Override
        public void close() throws IOException {
            if (isClone == false) {
                super.close();
            }
        }
    }
}
