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

package co.elastic.elasticsearch.stateless.commits;

import co.elastic.elasticsearch.stateless.ObjectStoreService;
import co.elastic.elasticsearch.stateless.lucene.StatelessCommitRef;
import co.elastic.elasticsearch.stateless.test.FakeStatelessNode;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoDeletionPolicy;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class StatelessCommitServiceTests extends ESTestCase {

    private ThreadPool threadPool;
    private int primaryTerm;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(StatelessCommitServiceTests.class.getName(), Settings.EMPTY);
        primaryTerm = randomIntBetween(1, 100);
    }

    @Override
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        super.tearDown();
    }

    public void testCommitUpload() throws Exception {
        Set<String> uploadedBlobs = Collections.newSetFromMap(new ConcurrentHashMap<>());
        try (var testHarness = createNode(fileCapture(uploadedBlobs), fileCapture(uploadedBlobs))) {

            List<StatelessCommitRef> commitRefs = generateIndexCommits(testHarness, randomIntBetween(3, 8));
            List<String> commitFiles = commitRefs.stream()
                .flatMap(ref -> ref.getCommitFiles().stream())
                .filter(file -> file.startsWith(IndexFileNames.SEGMENTS) == false)
                .toList();
            List<String> compoundCommitFiles = commitRefs.stream().map(ref -> StatelessCompoundCommit.NAME + ref.getGeneration()).toList();

            for (StatelessCommitRef commitRef : commitRefs) {
                testHarness.commitService.onCommitCreation(commitRef);
            }
            for (StatelessCommitRef commitRef : commitRefs) {
                PlainActionFuture<Void> future = PlainActionFuture.newFuture();
                testHarness.commitService.addListenerForUploadedGeneration(testHarness.shardId, commitRef.getGeneration(), future);
                future.actionGet();
            }
            for (StatelessCommitRef commitRef : commitRefs) {
                var generationalFiles = commitRef.getCommitFiles().stream().filter(StatelessCommitService::isGenerationalFile).toList();
                var internalFiles = returnInternalFiles(testHarness, List.of(StatelessCompoundCommit.NAME + commitRef.getGeneration()));
                assertThat(generationalFiles, everyItem(is(in(internalFiles))));
            }

            ArrayList<String> uploadedFiles = new ArrayList<>();
            uploadedFiles.addAll(uploadedBlobs);
            uploadedFiles.addAll(returnInternalFiles(testHarness, compoundCommitFiles));

            assertThat(
                "Expected that all commit files " + commitFiles + " have been uploaded " + uploadedFiles,
                uploadedFiles,
                hasItems(commitFiles.toArray(String[]::new))
            );

            assertThat(
                "Expected that all compound commit files " + compoundCommitFiles + " have been uploaded " + uploadedFiles,
                uploadedFiles,
                hasItems(compoundCommitFiles.toArray(String[]::new))
            );
        }
    }

    public void testCommitUploadIncludesRetries() throws Exception {
        Set<String> uploadedBlobs = Collections.newSetFromMap(new ConcurrentHashMap<>());
        Set<String> failedFiles = Collections.newSetFromMap(new ConcurrentHashMap<>());

        try (var testHarness = createNode((fileName, runnable) -> {
            if (failedFiles.contains(fileName) == false && randomBoolean()) {
                failedFiles.add(fileName);
                throw new IOException("Failed");
            } else {
                // Rarely fail a second time
                if (failedFiles.contains(fileName) && rarely()) {
                    throw new IOException("Failed");
                } else {
                    runnable.run();
                    uploadedBlobs.add(fileName);
                }
            }

        }, fileCapture(uploadedBlobs))) {

            List<StatelessCommitRef> commitRefs = generateIndexCommits(testHarness, randomIntBetween(2, 4));
            List<String> commitFiles = commitRefs.stream()
                .flatMap(ref -> ref.getCommitFiles().stream())
                .filter(file -> file.startsWith(IndexFileNames.SEGMENTS) == false)
                .toList();
            List<String> compoundCommitFiles = commitRefs.stream().map(ref -> StatelessCompoundCommit.NAME + ref.getGeneration()).toList();

            for (StatelessCommitRef commitRef : commitRefs) {
                testHarness.commitService.onCommitCreation(commitRef);
            }
            for (StatelessCommitRef commitRef : commitRefs) {
                PlainActionFuture<Void> future = PlainActionFuture.newFuture();
                testHarness.commitService.addListenerForUploadedGeneration(testHarness.shardId, commitRef.getGeneration(), future);
                future.actionGet();
            }

            ArrayList<String> uploadedFiles = new ArrayList<>();
            uploadedFiles.addAll(uploadedBlobs);
            uploadedFiles.addAll(returnInternalFiles(testHarness, compoundCommitFiles));

            assertThat(
                "Expected that all commit files " + commitFiles + " have been uploaded " + uploadedFiles,
                uploadedFiles,
                hasItems(commitFiles.toArray(String[]::new))
            );

            assertThat(
                "Expected that all compound commit files " + compoundCommitFiles + " have been uploaded " + uploadedFiles,
                uploadedFiles,
                hasItems(compoundCommitFiles.toArray(String[]::new))
            );
        }
    }

    public void testSecondCommitDefersSchedulingForFirstCommit() throws Exception {
        Set<String> uploadedBlobs = Collections.newSetFromMap(new ConcurrentHashMap<>());
        AtomicReference<String> commitFileToBlock = new AtomicReference<>();
        AtomicReference<String> firstCommitFile = new AtomicReference<>();
        AtomicReference<String> secondCommitFile = new AtomicReference<>();

        CountDownLatch startingUpload = new CountDownLatch(1);
        CountDownLatch blocking = new CountDownLatch(1);

        CheckedBiConsumer<String, CheckedRunnable<IOException>, IOException> compoundCommitFileCapture = fileCapture(uploadedBlobs);
        try (var testHarness = createNode(fileCapture(uploadedBlobs), (compoundCommitFile, runnable) -> {
            if (compoundCommitFile.equals(firstCommitFile.get())) {
                try {
                    startingUpload.countDown();
                    blocking.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                assertFalse(uploadedBlobs.contains(secondCommitFile.get()));
            } else {
                assertEquals(compoundCommitFile, secondCommitFile.get());
                assertTrue(uploadedBlobs.contains(firstCommitFile.get()));
            }
            compoundCommitFileCapture.accept(compoundCommitFile, runnable);
        })) {

            List<StatelessCommitRef> commitRefs = generateIndexCommits(testHarness, 2);
            StatelessCommitRef firstCommit = commitRefs.get(0);
            StatelessCommitRef secondCommit = commitRefs.get(1);

            commitFileToBlock.set(
                firstCommit.getAdditionalFiles()
                    .stream()
                    .filter(file -> file.startsWith(IndexFileNames.SEGMENTS) == false)
                    .findFirst()
                    .get()
            );

            List<String> commitFiles = commitRefs.stream()
                .flatMap(ref -> ref.getCommitFiles().stream())
                .filter(file -> file.startsWith(IndexFileNames.SEGMENTS) == false)
                .toList();

            List<String> compoundCommitFiles = commitRefs.stream().map(ref -> StatelessCompoundCommit.NAME + ref.getGeneration()).toList();
            firstCommitFile.set(compoundCommitFiles.get(0));
            secondCommitFile.set(compoundCommitFiles.get(1));

            testHarness.commitService.onCommitCreation(firstCommit);
            startingUpload.await();
            assertThat(uploadedBlobs, not(hasItems(commitFileToBlock.get())));
            assertThat(uploadedBlobs, not(hasItems(firstCommitFile.get())));

            testHarness.commitService.onCommitCreation(secondCommit);

            assertThat(uploadedBlobs, not(hasItems(secondCommitFile.get())));

            blocking.countDown();

            for (StatelessCommitRef commitRef : commitRefs) {
                PlainActionFuture<Void> future = PlainActionFuture.newFuture();
                testHarness.commitService.addListenerForUploadedGeneration(testHarness.shardId, commitRef.getGeneration(), future);
                future.actionGet();
            }

            ArrayList<String> uploadedFiles = new ArrayList<>();
            uploadedFiles.addAll(uploadedBlobs);
            uploadedFiles.addAll(returnInternalFiles(testHarness, compoundCommitFiles));

            assertThat(
                "Expected that all commit files " + commitFiles + " have been uploaded " + uploadedFiles,
                uploadedFiles,
                hasItems(commitFiles.toArray(String[]::new))
            );

            assertThat(
                "Expected that all compound commit files " + compoundCommitFiles + " have been uploaded " + uploadedFiles,
                uploadedFiles,
                hasItems(compoundCommitFiles.toArray(String[]::new))
            );
        }
    }

    public void testUploadFileOfUnregisteredShardThrowsExceptions() throws IOException {
        try (var testHarness = createNode((n, r) -> r.run(), (n, r) -> r.run())) {
            List<StatelessCommitRef> refs = generateIndexCommits(testHarness, 1);

            testHarness.commitService.unregister(testHarness.shardId);
            expectThrows(AlreadyClosedException.class, () -> testHarness.commitService.onCommitCreation(refs.get(0)));
        }
    }

    public void testMapIsPrunedOnIndexDelete() throws IOException {
        try (var testHarness = createNode((n, r) -> r.run(), (n, r) -> r.run())) {
            List<StatelessCommitRef> refs = generateIndexCommits(testHarness, 2, true);
            StatelessCommitRef firstCommit = refs.get(0);
            StatelessCommitRef secondCommit = refs.get(1);

            testHarness.commitService.onCommitCreation(firstCommit);

            PlainActionFuture<Void> future = PlainActionFuture.newFuture();
            testHarness.commitService.addListenerForUploadedGeneration(testHarness.shardId, firstCommit.getGeneration(), future);
            future.actionGet();

            testHarness.commitService.onCommitCreation(secondCommit);

            PlainActionFuture<Void> future2 = PlainActionFuture.newFuture();
            testHarness.commitService.addListenerForUploadedGeneration(testHarness.shardId, firstCommit.getGeneration(), future2);
            future2.actionGet();

            Set<String> files = testHarness.commitService.getFileToBlobFile(testHarness.shardId).keySet();

            assertThat(
                "Expected that all first commit files " + firstCommit.getCommitFiles() + " to be in file map " + files,
                files,
                hasItems(firstCommit.getCommitFiles().toArray(String[]::new))
            );
            assertThat(
                "Expected that all second commit files " + secondCommit.getCommitFiles() + " to be in file map " + files,
                files,
                hasItems(secondCommit.getCommitFiles().toArray(String[]::new))
            );

            testHarness.commitService.markCommitDeleted(testHarness.shardId, firstCommit.getCommitFiles());

            List<String> expectedDeletedFiles = firstCommit.getCommitFiles()
                .stream()
                .filter(f -> secondCommit.getCommitFiles().contains(f) == false)
                .toList();

            Set<String> filesAfterDelete = testHarness.commitService.getFileToBlobFile(testHarness.shardId).keySet();

            assertThat(
                "Expected that all first commit only files " + expectedDeletedFiles + " to be deleted from file map " + filesAfterDelete,
                filesAfterDelete,
                not(hasItems(expectedDeletedFiles.toArray(String[]::new)))
            );
            assertThat(
                "Expected that all second commit files " + secondCommit.getCommitFiles() + " to be in file map " + files,
                files,
                hasItems(secondCommit.getCommitFiles().toArray(String[]::new))
            );
        }
    }

    public void testRecoveredCommitIsNotUploadedAgain() throws Exception {
        Set<String> uploadedBlobs = Collections.newSetFromMap(new ConcurrentHashMap<>());
        try (var testHarness = createNode(fileCapture(uploadedBlobs), fileCapture(uploadedBlobs))) {

            StatelessCommitRef commitRef = generateIndexCommits(testHarness, 1).get(0);

            List<String> commitFiles = commitRef.getCommitFiles()
                .stream()
                .filter(file -> file.startsWith(IndexFileNames.SEGMENTS) == false)
                .toList();

            testHarness.commitService.onCommitCreation(commitRef);
            PlainActionFuture<Void> future = PlainActionFuture.newFuture();
            testHarness.commitService.addListenerForUploadedGeneration(testHarness.shardId, commitRef.getGeneration(), future);
            future.actionGet();

            ArrayList<String> uploadedFiles = new ArrayList<>();
            uploadedFiles.addAll(uploadedBlobs);
            uploadedFiles.addAll(returnInternalFiles(testHarness, List.of(StatelessCompoundCommit.NAME + commitRef.getGeneration())));

            assertThat(
                "Expected that all commit files " + commitFiles + " have been uploaded " + uploadedFiles,
                uploadedFiles,
                hasItems(commitFiles.toArray(String[]::new))
            );

            assertThat(
                "Expected that the compound commit file "
                    + StatelessCompoundCommit.NAME
                    + commitRef.getGeneration()
                    + " has been uploaded "
                    + uploadedFiles,
                uploadedFiles,
                hasItems(StatelessCompoundCommit.NAME + commitRef.getGeneration())
            );

            uploadedBlobs.clear();

            StatelessCompoundCommit commit = ObjectStoreService.findSearchShardFiles(
                testHarness.objectStoreService.getBlobContainer(testHarness.shardId, primaryTerm)
            );

            testHarness.commitService.unregister(testHarness.shardId);

            testHarness.commitService.register(testHarness.shardId);
            testHarness.commitService.markRecoveredCommit(testHarness.shardId, commit);

            testHarness.commitService.onCommitCreation(commitRef);
            PlainActionFuture<Void> future2 = PlainActionFuture.newFuture();
            testHarness.commitService.addListenerForUploadedGeneration(testHarness.shardId, commitRef.getGeneration(), future2);
            future2.actionGet();

            assertThat(uploadedBlobs, empty());
        }
    }

    public void testWaitForGenerationFailsForClosedShard() throws IOException {
        try (var testHarness = createNode((n, r) -> r.run(), (n, r) -> r.run())) {
            ShardId unregisteredShardId = new ShardId("index", "uuid", 0);
            expectThrows(
                AlreadyClosedException.class,
                () -> testHarness.commitService.addListenerForUploadedGeneration(unregisteredShardId, 1, PlainActionFuture.newFuture())
            );
            PlainActionFuture<Void> failedFuture = PlainActionFuture.newFuture();
            testHarness.commitService.addListenerForUploadedGeneration(testHarness.shardId, 1, failedFuture);

            testHarness.commitService.unregister(testHarness.shardId);
            expectThrows(AlreadyClosedException.class, failedFuture::actionGet);
        }
    }

    private ArrayList<String> returnInternalFiles(FakeStatelessNode testHarness, List<String> compoundCommitFiles) throws IOException {
        ArrayList<String> filesOnObjectStore = new ArrayList<>();
        for (String commitFile : compoundCommitFiles) {
            BlobContainer blobContainer = testHarness.objectStoreService.getBlobContainer(testHarness.shardId, primaryTerm);
            try (InputStream inputStream = blobContainer.readBlob(commitFile)) {
                long fileLength = blobContainer.listBlobs().get(commitFile).length();
                StatelessCompoundCommit compoundCommit = StatelessCompoundCommit.readFromStore(
                    new InputStreamStreamInput(inputStream),
                    fileLength
                );
                compoundCommit.commitFiles()
                    .entrySet()
                    .stream()
                    .filter(e -> e.getValue().blobName().equals(commitFile))
                    .forEach(e -> filesOnObjectStore.add(e.getKey()));

            }
        }
        return filesOnObjectStore;
    }

    private static CheckedBiConsumer<String, CheckedRunnable<IOException>, IOException> fileCapture(Set<String> uploadedFiles) {
        return (s, r) -> {
            r.run();
            uploadedFiles.add(s);
        };
    }

    private FakeStatelessNode createNode(
        CheckedBiConsumer<String, CheckedRunnable<IOException>, IOException> commitFileConsumer,
        CheckedBiConsumer<String, CheckedRunnable<IOException>, IOException> compoundCommitFileConsumer
    ) throws IOException {
        return new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry()) {
            @Override
            public BlobContainer wrapBlobContainer(BlobPath path, BlobContainer innerContainer) {

                class WrappedBlobContainer extends FilterBlobContainer {
                    WrappedBlobContainer(BlobContainer delegate) {
                        super(delegate);
                    }

                    @Override
                    protected BlobContainer wrapChild(BlobContainer child) {
                        return new WrappedBlobContainer(child);
                    }

                    @Override
                    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
                        throws IOException {
                        assertFalse(blobName, blobName.startsWith(IndexFileNames.SEGMENTS));
                        commitFileConsumer.accept(blobName, () -> super.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists));
                    }

                    @Override
                    public void writeMetadataBlob(
                        String blobName,
                        boolean failIfAlreadyExists,
                        boolean atomic,
                        CheckedConsumer<OutputStream, IOException> writer
                    ) throws IOException {
                        assertTrue(blobName, blobName.startsWith(StatelessCompoundCommit.NAME));
                        compoundCommitFileConsumer.accept(
                            blobName,
                            () -> super.writeMetadataBlob(blobName, failIfAlreadyExists, atomic, writer)
                        );
                    }

                    @Override
                    public void writeBlobAtomic(String blobName, BytesReference bytes, boolean failIfAlreadyExists) {
                        throw new AssertionError("should not be called");
                    }
                }

                return new WrappedBlobContainer(innerContainer);
            }
        };
    }

    private List<StatelessCommitRef> generateIndexCommits(FakeStatelessNode testHarness, int commitsNumber) throws IOException {
        return generateIndexCommits(testHarness, commitsNumber, false);
    }

    private List<StatelessCommitRef> generateIndexCommits(FakeStatelessNode testHarness, int commitsNumber, boolean merge)
        throws IOException {
        List<StatelessCommitRef> commits = new ArrayList<>(commitsNumber);
        Set<String> previousCommit = new HashSet<>();

        final var indexWriterConfig = new IndexWriterConfig(new KeywordAnalyzer());
        indexWriterConfig.setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE);
        String deleteId = randomAlphaOfLength(10);

        try (var indexWriter = new IndexWriter(testHarness.indexingStore.directory(), indexWriterConfig)) {
            for (int i = 0; i < commitsNumber; i++) {
                LuceneDocument document = new LuceneDocument();
                document.add(new KeywordField("field0", "term", Field.Store.YES));
                indexWriter.addDocument(document.getFields());
                if (randomBoolean()) {
                    final ParsedDocument tombstone = ParsedDocument.deleteTombstone(deleteId);
                    LuceneDocument delete = tombstone.docs().get(0);
                    NumericDocValuesField field = Lucene.newSoftDeletesField();
                    delete.add(field);
                    indexWriter.softUpdateDocument(EngineTestCase.newUid(deleteId), delete.getFields(), Lucene.newSoftDeletesField());
                }
                indexWriter.commit();
                if (merge) {
                    indexWriter.forceMerge(1, true);
                }
                try (var indexReader = DirectoryReader.open(indexWriter)) {
                    IndexCommit indexCommit = indexReader.getIndexCommit();
                    Set<String> commitFiles = new HashSet<>(indexCommit.getFileNames());
                    Set<String> additionalFiles = Sets.difference(commitFiles, previousCommit);
                    previousCommit = commitFiles;

                    StatelessCommitRef statelessCommitRef = new StatelessCommitRef(
                        testHarness.shardId,
                        new Engine.IndexCommitRef(indexCommit, () -> {}),
                        commitFiles,
                        additionalFiles,
                        primaryTerm,
                        0
                    );
                    commits.add(statelessCommitRef);
                }
            }
        }

        return commits;
    }
}
