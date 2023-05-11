/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.stateless.commits;

import co.elastic.elasticsearch.stateless.ObjectStoreService;
import co.elastic.elasticsearch.stateless.lucene.StatelessCommitRef;
import co.elastic.elasticsearch.stateless.test.FakeStatelessNode;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItems;
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
        Set<String> uploadedFiles = Collections.newSetFromMap(new ConcurrentHashMap<>());
        Map<Long, Collection<String>> requiredFiles = new HashMap<>();
        try (var testHarness = createNode(fileCapture(uploadedFiles), (compoundCommitFile, runnable) -> {
            long generation = Long.parseLong(compoundCommitFile.split(StatelessCompoundCommit.NAME)[1]);
            Collection<String> required = requiredFiles.get(generation);
            assertThat(
                "Expected that all required files "
                    + required
                    + " for generation ["
                    + generation
                    + "] to have been uploaded "
                    + uploadedFiles,
                uploadedFiles,
                hasItems(required.toArray(String[]::new))
            );

            uploadedFiles.add(compoundCommitFile);
            runnable.run();
        })) {

            List<StatelessCommitRef> commitRefs = generateIndexCommits(testHarness, randomIntBetween(3, 8));
            commitRefs.forEach(
                statelessCommitRef -> requiredFiles.put(
                    statelessCommitRef.getGeneration(),
                    statelessCommitRef.getCommitFiles()
                        .stream()
                        .filter(file -> file.startsWith(IndexFileNames.SEGMENTS) == false)
                        .collect(Collectors.toList())
                )
            );
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
                testHarness.commitService.addOrNotify(testHarness.shardId, commitRef.getGeneration(), future);
                future.actionGet();
            }

            assertThat(
                "Expected that all commit files " + commitFiles + " have been uploaded " + uploadedFiles,
                uploadedFiles,
                hasItems(commitFiles.toArray(String[]::new))
            );

            // Must be assert busy as the generation listener could have been triggered by a newer commit finishing
            assertBusy(
                () -> assertThat(
                    "Expected that all compound commit files " + compoundCommitFiles + " have been uploaded " + uploadedFiles,
                    uploadedFiles,
                    hasItems(compoundCommitFiles.toArray(String[]::new))
                )
            );
        }
    }

    public void testCommitUploadIncludesRetries() throws Exception {
        Set<String> uploadedFiles = Collections.newSetFromMap(new ConcurrentHashMap<>());
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
                    uploadedFiles.add(fileName);
                }
            }

        }, fileCapture(uploadedFiles))) {

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
                testHarness.commitService.addOrNotify(testHarness.shardId, commitRef.getGeneration(), future);
                future.actionGet();
            }

            assertThat(
                "Expected that all commit files " + commitFiles + " have been uploaded " + uploadedFiles,
                uploadedFiles,
                hasItems(commitFiles.toArray(String[]::new))
            );

            // Must be assert busy as the generation listener could have been triggered by a newer commit finishing
            assertBusy(
                () -> assertThat(
                    "Expected that all compound commit files " + compoundCommitFiles + " have been uploaded " + uploadedFiles,
                    uploadedFiles,
                    hasItems(compoundCommitFiles.toArray(String[]::new))
                )
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
            testHarness.commitService.addOrNotify(testHarness.shardId, firstCommit.getGeneration(), future);
            future.actionGet();

            testHarness.commitService.onCommitCreation(secondCommit);

            PlainActionFuture<Void> future2 = PlainActionFuture.newFuture();
            testHarness.commitService.addOrNotify(testHarness.shardId, firstCommit.getGeneration(), future2);
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
        Set<String> uploadedFiles = Collections.newSetFromMap(new ConcurrentHashMap<>());
        try (var testHarness = createNode(fileCapture(uploadedFiles), fileCapture(uploadedFiles))) {

            StatelessCommitRef commitRef = generateIndexCommits(testHarness, 1).get(0);

            List<String> commitFiles = commitRef.getCommitFiles()
                .stream()
                .filter(file -> file.startsWith(IndexFileNames.SEGMENTS) == false)
                .toList();

            testHarness.commitService.onCommitCreation(commitRef);
            PlainActionFuture<Void> future = PlainActionFuture.newFuture();
            testHarness.commitService.addOrNotify(testHarness.shardId, commitRef.getGeneration(), future);
            future.actionGet();

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

            uploadedFiles.clear();

            StatelessCompoundCommit commit = ObjectStoreService.findSearchShardFiles(
                testHarness.objectStoreService.getBlobContainer(testHarness.shardId, primaryTerm)
            );

            testHarness.commitService.unregister(testHarness.shardId);

            testHarness.commitService.register(testHarness.shardId);
            testHarness.commitService.markRecoveredCommit(testHarness.shardId, commit);

            testHarness.commitService.onCommitCreation(commitRef);
            PlainActionFuture<Void> future2 = PlainActionFuture.newFuture();
            testHarness.commitService.addOrNotify(testHarness.shardId, commitRef.getGeneration(), future2);
            future2.actionGet();

            assertThat(uploadedFiles, empty());
        }
    }

    public void testWaitForGenerationFailsForClosedShard() throws IOException {
        try (var testHarness = createNode((n, r) -> r.run(), (n, r) -> r.run())) {
            ShardId unregisteredShardId = new ShardId("index", "uuid", 0);
            expectThrows(
                AlreadyClosedException.class,
                () -> testHarness.commitService.addOrNotify(unregisteredShardId, 1, PlainActionFuture.newFuture())
            );
            PlainActionFuture<Void> failedFuture = PlainActionFuture.newFuture();
            testHarness.commitService.addOrNotify(testHarness.shardId, 1, failedFuture);

            testHarness.commitService.unregister(testHarness.shardId);
            expectThrows(AlreadyClosedException.class, failedFuture::actionGet);
        }
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
        try (var indexWriter = new IndexWriter(testHarness.indexingStore.directory(), indexWriterConfig)) {
            for (int i = 0; i < commitsNumber; i++) {
                indexWriter.addDocument(List.of());
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
                        primaryTerm
                    );
                    commits.add(statelessCommitRef);
                }
            }
        }

        return commits;
    }
}
