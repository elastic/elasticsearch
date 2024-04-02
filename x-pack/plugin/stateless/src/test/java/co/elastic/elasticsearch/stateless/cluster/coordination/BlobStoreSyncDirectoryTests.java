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

package co.elastic.elasticsearch.stateless.cluster.coordination;

import co.elastic.elasticsearch.stateless.test.FakeStatelessNode;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.IOContext;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Streams;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class BlobStoreSyncDirectoryTests extends ESTestCase {
    public void testFilesAreUploadedInIncreasingSizeOrderToTheBlobStoreDuringSync() throws Exception {
        long term = 1;
        final List<String> uploadedFiles = new ArrayList<>();
        try (var statelessNode = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry()) {
            @Override
            public BlobContainer wrapBlobContainer(BlobPath path, BlobContainer innerContainer) {
                return new FilterBlobContainer(super.wrapBlobContainer(path, innerContainer)) {
                    @Override
                    protected BlobContainer wrapChild(BlobContainer child) {
                        return child;
                    }

                    @Override
                    public void writeBlob(
                        OperationPurpose purpose,
                        String blobName,
                        InputStream inputStream,
                        long blobSize,
                        boolean failIfAlreadyExists
                    ) throws IOException {
                        uploadedFiles.add(blobName);
                        super.writeBlob(purpose, blobName, inputStream, blobSize, failIfAlreadyExists);
                    }
                };
            }
        };
            var syncDirectory = new BlobStoreSyncDirectory(
                new ByteBuffersDirectory(),
                () -> statelessNode.objectStoreService.getClusterStateBlobContainerForTerm(term),
                EsExecutors.DIRECT_EXECUTOR_SERVICE // Execute in the same thread pool, in order to get predictable ordering
            )
        ) {
            int numberOfFiles = randomIntBetween(10, 20);
            final List<Tuple<String, byte[]>> writtenFiles = new ArrayList<>(numberOfFiles);
            for (int i = 0; i < numberOfFiles; i++) {
                var fileName = randomAlphaOfLength(10);
                try (var output = syncDirectory.createOutput(fileName, IOContext.DEFAULT)) {
                    var data = randomByteArrayOfLength(randomIntBetween(1, 1024));
                    output.writeBytes(data, 0, data.length);
                    writtenFiles.add(Tuple.tuple(fileName, data));
                }
            }
            syncDirectory.sync(writtenFiles.stream().map(Tuple::v1).toList());

            BlobContainer termBlobContainer = statelessNode.objectStoreService.getClusterStateBlobContainerForTerm(term);
            var writtenFilesOrderedBySize = writtenFiles.stream()
                .sorted(Comparator.<Tuple<String, byte[]>>comparingInt(writtenFile -> writtenFile.v2().length).reversed())
                .toList();

            for (int i = 0; i < writtenFilesOrderedBySize.size(); i++) {
                var writtenFile = writtenFilesOrderedBySize.get(i);
                assertThat(uploadedFiles.get(i), is(equalTo(writtenFile.v1())));

                try (var inputStream = termBlobContainer.readBlob(randomFrom(OperationPurpose.values()), writtenFile.v1())) {
                    var outputStream = new ByteArrayOutputStream();
                    Streams.copy(inputStream, outputStream);

                    assertArrayEquals(writtenFile.v2(), outputStream.toByteArray());
                }
            }
        }
    }

    public void testOnlyUploadsNewFilesFromLatestCommit() throws Exception {
        final Map<String, Long> uploadCount = new HashMap<>();
        try (var statelessNode = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry()) {
            @Override
            public BlobContainer wrapBlobContainer(BlobPath path, BlobContainer innerContainer) {
                return new FilterBlobContainer(super.wrapBlobContainer(path, innerContainer)) {
                    @Override
                    protected BlobContainer wrapChild(BlobContainer child) {
                        return child;
                    }

                    @Override
                    public void writeBlob(
                        OperationPurpose purpose,
                        String blobName,
                        InputStream inputStream,
                        long blobSize,
                        boolean failIfAlreadyExists
                    ) throws IOException {
                        trackUpload(blobName);
                        super.writeBlob(purpose, blobName, inputStream, blobSize, failIfAlreadyExists);
                    }

                    @Override
                    public void writeMetadataBlob(
                        OperationPurpose purpose,
                        String blobName,
                        boolean failIfAlreadyExists,
                        boolean atomic,
                        CheckedConsumer<OutputStream, IOException> writer
                    ) throws IOException {
                        trackUpload(blobName);
                        super.writeMetadataBlob(purpose, blobName, failIfAlreadyExists, atomic, writer);
                    }

                    private void trackUpload(String blobName) {
                        uploadCount.compute(blobName, (blob, uploadCount) -> uploadCount == null ? 1 : uploadCount + 1);
                    }
                };
            }
        };
            var syncDirectory = new BlobStoreSyncDirectory(
                new ByteBuffersDirectory(),
                () -> statelessNode.objectStoreService.getClusterStateBlobContainerForTerm(1),
                EsExecutors.DIRECT_EXECUTOR_SERVICE
            );
            var indexWriter = new IndexWriter(syncDirectory, getIndexWriterConfig())
        ) {
            indexRandomDocuments(indexWriter, randomIntBetween(10, 20));
            indexWriter.commit();

            indexRandomDocuments(indexWriter, randomIntBetween(10, 20));
            indexWriter.commit();

            assertThat(uploadCount.values().stream().allMatch(uploadFileCount -> uploadFileCount == 1), is(true));
        }
    }

    public void testDeletesUnusedFilesAfterSuccessfulCommit() throws Exception {
        final Set<String> uploadedFiles = new HashSet<>();
        final Set<String> deletedFiles = new HashSet<>();
        final AtomicBoolean failCommitUpload = new AtomicBoolean();
        try (var statelessNode = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry()) {
            @Override
            public BlobContainer wrapBlobContainer(BlobPath path, BlobContainer innerContainer) {
                return new FilterBlobContainer(super.wrapBlobContainer(path, innerContainer)) {
                    @Override
                    protected BlobContainer wrapChild(BlobContainer child) {
                        return child;
                    }

                    @Override
                    public void writeBlob(
                        OperationPurpose purpose,
                        String blobName,
                        InputStream inputStream,
                        long blobSize,
                        boolean failIfAlreadyExists
                    ) throws IOException {
                        trackUpload(blobName);
                        super.writeBlob(purpose, blobName, inputStream, blobSize, failIfAlreadyExists);
                    }

                    @Override
                    public void writeMetadataBlob(
                        OperationPurpose purpose,
                        String blobName,
                        boolean failIfAlreadyExists,
                        boolean atomic,
                        CheckedConsumer<OutputStream, IOException> writer
                    ) throws IOException {
                        if (failCommitUpload.get()) {
                            throw new IOException("Unable to upload " + blobName);
                        }
                        trackUpload(blobName);
                        super.writeMetadataBlob(purpose, blobName, failIfAlreadyExists, atomic, writer);
                    }

                    @Override
                    public void deleteBlobsIgnoringIfNotExists(OperationPurpose purpose, Iterator<String> blobNames) throws IOException {
                        List<String> blobsToDelete = new ArrayList<>();
                        blobNames.forEachRemaining(blobsToDelete::add);

                        assertThat(uploadedFiles.containsAll(blobsToDelete), is(true));

                        deletedFiles.addAll(blobsToDelete);
                        super.deleteBlobsIgnoringIfNotExists(purpose, blobsToDelete.iterator());
                    }

                    private void trackUpload(String blobName) {
                        uploadedFiles.add(blobName);
                    }
                };
            }
        };
            var dir = new BlobStoreSyncDirectory(
                new ByteBuffersDirectory(),
                () -> statelessNode.objectStoreService.getClusterStateBlobContainerForTerm(1),
                EsExecutors.DIRECT_EXECUTOR_SERVICE
            );
            var indexWriter = new IndexWriter(dir, getIndexWriterConfig())
        ) {
            indexRandomDocuments(indexWriter, randomIntBetween(100, 200));
            indexWriter.commit();

            indexWriter.deleteAll();
            indexWriter.commit();

            // Lucene deletes the old segment files when it commits a new segment with content
            indexRandomDocuments(indexWriter, randomIntBetween(10, 20));
            assertThat(deletedFiles, is(empty()));
            if (randomBoolean()) {
                failCommitUpload.set(true);
                expectThrows(Exception.class, indexWriter::commit);
                assertThat(deletedFiles, is(empty()));

                failCommitUpload.set(false);
            }

            indexWriter.commit();
            assertThat(deletedFiles, is(not(empty())));
        }
    }

    public void testCloseCancelsOnGoingUploads() throws Exception {
        List<String> uploadedFiles = Collections.synchronizedList(new ArrayList<>());
        try (var statelessNode = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry()) {
            @Override
            public BlobContainer wrapBlobContainer(BlobPath path, BlobContainer innerContainer) {
                return new FilterBlobContainer(super.wrapBlobContainer(path, innerContainer)) {
                    @Override
                    protected BlobContainer wrapChild(BlobContainer child) {
                        return child;
                    }

                    @Override
                    public void writeBlob(
                        OperationPurpose purpose,
                        String blobName,
                        InputStream inputStream,
                        long blobSize,
                        boolean failIfAlreadyExists
                    ) throws IOException {
                        super.writeBlob(purpose, blobName, inputStream, blobSize, failIfAlreadyExists);
                    }
                };
            }
        };
            var dir = new BlobStoreSyncDirectory(
                new ByteBuffersDirectory(),
                () -> statelessNode.objectStoreService.getClusterStateBlobContainerForTerm(1),
                statelessNode.threadPool.executor(ThreadPool.Names.SNAPSHOT)
            );
            var indexWriter = new IndexWriter(dir, getIndexWriterConfig())
        ) {
            var threadPool = statelessNode.threadPool;
            var snapshotExecutor = (EsThreadPoolExecutor) threadPool.executor(ThreadPool.Names.SNAPSHOT);
            var maxPoolSize = snapshotExecutor.getMaximumPoolSize();
            var blockExecutionLatch = new CountDownLatch(1);

            for (int i = 0; i < maxPoolSize; i++) {
                snapshotExecutor.submit(() -> {
                    try {
                        blockExecutionLatch.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                });
            }

            indexRandomDocuments(indexWriter, randomIntBetween(100, 200));
            var commitFuture = threadPool.executor(ThreadPool.Names.GENERIC).submit(() -> {
                try {
                    indexWriter.commit();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

            dir.close();

            blockExecutionLatch.countDown();

            expectThrows(Exception.class, commitFuture::get);
            assertThat(uploadedFiles, is(empty()));
        }
    }

    private void indexRandomDocuments(IndexWriter indexWriter, int numberOfDocsSecondCommit) throws IOException {
        for (int i = 0; i < numberOfDocsSecondCommit; i++) {
            var doc = new Document();
            doc.add(new StringField("test", randomAlphaOfLength(10), Field.Store.YES));
            indexWriter.addDocument(doc);
        }
    }

    private IndexWriterConfig getIndexWriterConfig() {
        final IndexWriterConfig indexWriterConfig = new IndexWriterConfig(new KeywordAnalyzer());
        indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        indexWriterConfig.setCommitOnClose(false);
        indexWriterConfig.setMergeScheduler(new SerialMergeScheduler());
        indexWriterConfig.setMergePolicy(new TieredMergePolicy().setDeletesPctAllowed(5));

        return indexWriterConfig;
    }
}
