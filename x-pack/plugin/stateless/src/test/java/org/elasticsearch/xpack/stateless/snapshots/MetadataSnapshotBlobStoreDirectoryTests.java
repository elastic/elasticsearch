/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.fs.FsBlobContainer;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.commits.BlobFile;
import org.elasticsearch.xpack.stateless.commits.BlobLocation;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;
import org.elasticsearch.xpack.stateless.test.FakeStatelessNode;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

@LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class MetadataSnapshotBlobStoreDirectoryTests extends ESTestCase {

    private static final Logger logger = LogManager.getLogger(MetadataSnapshotBlobStoreDirectoryTests.class);

    public void testMetadataSnapshotMatchesLocalDirectory() throws IOException {
        // Create a real Lucene index
        try (Directory localDir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(localDir, new IndexWriterConfig())) {
                int numDocs = between(1, 50);
                for (int i = 0; i < numDocs; i++) {
                    Document doc = new Document();
                    doc.add(new StringField("id", Integer.toString(i), Field.Store.YES));
                    doc.add(new StringField("value", randomAlphaOfLength(10), Field.Store.YES));
                    writer.addDocument(doc);
                }
                writer.commit();
            }

            // Load the expected metadata from the local directory
            final Store.MetadataSnapshot expected;
            try (var reader = DirectoryReader.open(localDir)) {
                expected = Store.MetadataSnapshot.loadFromIndexCommit(
                    reader.getIndexCommit(),
                    localDir,
                    LogManager.getLogger(MetadataSnapshotBlobStoreDirectoryTests.class)
                );
            }
            assertFalse("expected non-empty metadata", expected.fileMetadataMap().isEmpty());

            // Write all index files into a blob container, simulating blob store
            final var blobContainer = createBlobContainer();
            final long primaryTerm = randomLongBetween(1, 100);
            final Map<String, BlobLocation> blobLocations = new HashMap<>();

            // Write each file as a separate blob with a unique generation to simulate a commit referring multiple blobs
            long fileGeneration = 1;
            for (String fileName : localDir.listAll()) {
                final long fileLength = localDir.fileLength(fileName);
                if (fileLength == 0) {
                    assertEmptyFileIsWriteLockOrExtraFile(fileName);
                    continue;
                }
                final String fileBlobName = "stateless_commit_" + fileGeneration;
                try (
                    var indexInput = localDir.openInput(fileName, IOContext.READONCE);
                    var inputStream = new InputStreamIndexInput(indexInput, fileLength)
                ) {
                    blobContainer.writeBlob(OperationPurpose.SNAPSHOT_DATA, fileBlobName, inputStream, fileLength, false);
                    blobLocations.put(
                        fileName,
                        new BlobLocation(
                            new BlobFile(fileBlobName, new PrimaryTermAndGeneration(primaryTerm, fileGeneration)),
                            0,
                            fileLength
                        )
                    );
                }
                fileGeneration++;
            }

            // Load metadata via BlobStoreReadOnlyDirectory
            final var shardId = new ShardId(new Index(randomIdentifier(), randomUUID()), 0);
            final Store.MetadataSnapshot actual;
            try (var blobDir = new MetadataSnapshotBlobStoreDirectory(blobLocations, (s, pt) -> blobContainer, shardId)) {
                actual = Store.MetadataSnapshot.loadFromIndexCommit(null, blobDir, logger);
            }

            // Verify they match
            assertStoreMetadataMatches(actual, expected);
        }
    }

    /**
     * Tests that files written at a non-zero offset within the blob are read correctly.
     */
    public void testMetadataSnapshotWithBlobOffset() throws IOException {
        try (Directory localDir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(localDir, new IndexWriterConfig())) {
                Document doc = new Document();
                doc.add(new StringField("id", "1", Field.Store.YES));
                writer.addDocument(doc);
                writer.commit();
            }

            final Store.MetadataSnapshot expected;
            try (var reader = DirectoryReader.open(localDir)) {
                expected = Store.MetadataSnapshot.loadFromIndexCommit(
                    reader.getIndexCommit(),
                    localDir,
                    LogManager.getLogger(MetadataSnapshotBlobStoreDirectoryTests.class)
                );
            }

            // Write files at a non-zero offset within a single blob (simulating compound commit)
            final var blobContainer = createBlobContainer();
            final long primaryTerm = randomNonNegativeLong();
            final long generation = randomNonNegativeLong();
            final String blobName = "stateless_commit_" + generation;
            final Map<String, BlobLocation> blobLocations = new HashMap<>();

            // First write some padding bytes, then each file sequentially
            final int padding = between(1, 100);
            byte[] paddingBytes = randomByteArrayOfLength(padding);

            // Build the full blob: padding + file1 + file2 + ...
            var baos = new ByteArrayOutputStream();
            baos.write(paddingBytes);
            final var blobFile = new BlobFile(blobName, new PrimaryTermAndGeneration(primaryTerm, generation));
            for (String fileName : localDir.listAll()) {
                long fileLength = localDir.fileLength(fileName);
                if (fileLength == 0) {
                    assertEmptyFileIsWriteLockOrExtraFile(fileName);
                    continue;
                }
                long offset = baos.size();
                try (
                    var indexInput = localDir.openInput(fileName, IOContext.READONCE);
                    var inputStream = new InputStreamIndexInput(indexInput, fileLength)
                ) {
                    inputStream.transferTo(baos);
                }
                blobLocations.put(fileName, new BlobLocation(blobFile, offset, fileLength));
            }

            // Write the compound blob
            blobContainer.writeBlob(
                OperationPurpose.SNAPSHOT_DATA,
                blobName,
                new ByteArrayInputStream(baos.toByteArray()),
                baos.size(),
                false
            );

            final var shardId = new ShardId(new Index(randomIdentifier(), randomUUID()), 0);
            final Store.MetadataSnapshot actual;
            try (var blobDir = new MetadataSnapshotBlobStoreDirectory(blobLocations, (s, pt) -> blobContainer, shardId)) {
                actual = Store.MetadataSnapshot.loadFromIndexCommit(null, blobDir, logger);
            }

            assertStoreMetadataMatches(actual, expected);
        }
    }

    public void testMetadataSnapshotWithFakeStatelessNode() throws IOException {
        try (
            var testHarness = new FakeStatelessNode(
                this::newEnvironment,
                this::newNodeEnvironment,
                xContentRegistry(),
                randomLongBetween(1, 100),
                TestProjectResolvers.DEFAULT_PROJECT_ONLY
            )
        ) {
            final int iterations = between(5, 20);
            for (int i = 0; i < iterations; i++) {
                final var commitRefs = testHarness.generateIndexCommits(between(1, 20), randomBoolean(), randomBoolean(), generation -> {});
                commitRefs.forEach(testHarness.commitService::onCommitCreation);
                final var currentCommit = commitRefs.getLast().getIndexCommit();
                testHarness.commitService.ensureMaxGenerationToUploadForFlush(testHarness.shardId, currentCommit.getGeneration());
                safeAwait(
                    (ActionListener<Void> listener) -> testHarness.commitService.addListenerForUploadedGeneration(
                        testHarness.shardId,
                        currentCommit.getGeneration(),
                        listener
                    )
                );

                final Store.MetadataSnapshot expected = testHarness.indexingStore.getMetadata(currentCommit);

                final var blobLocations = currentCommit.getFileNames()
                    .stream()
                    .collect(
                        Collectors.toUnmodifiableMap(
                            Function.identity(),
                            fileName -> Objects.requireNonNull(
                                testHarness.commitService.getBlobLocation(testHarness.shardId, fileName),
                                fileName
                            )
                        )
                    );
                final Store.MetadataSnapshot actual;
                try (
                    var blobDir = new MetadataSnapshotBlobStoreDirectory(
                        blobLocations,
                        testHarness.objectStoreService::getProjectBlobContainer,
                        testHarness.shardId
                    )
                ) {
                    actual = Store.MetadataSnapshot.loadFromIndexCommit(null, blobDir, logger);
                }

                assertStoreMetadataMatches(actual, expected);
            }
        }
    }

    private static void assertStoreMetadataMatches(Store.MetadataSnapshot actual, Store.MetadataSnapshot expected) {
        assertThat(actual.numDocs(), equalTo(expected.numDocs()));
        assertThat(actual.commitUserData(), equalTo(expected.commitUserData()));
        assertThat(actual.fileMetadataMap().keySet(), equalTo(expected.fileMetadataMap().keySet()));
        for (var entry : expected.fileMetadataMap().entrySet()) {
            var expectedMeta = entry.getValue();
            var actualMeta = actual.get(entry.getKey());
            assertNotNull("missing metadata for file " + entry.getKey(), actualMeta);
            assertThat(actualMeta.name(), equalTo(expectedMeta.name()));
            assertThat(actualMeta.length(), equalTo(expectedMeta.length()));
            assertThat(actualMeta.checksum(), equalTo(expectedMeta.checksum()));
            assertThat(actualMeta.writtenBy(), equalTo(expectedMeta.writtenBy()));
            assertThat(actualMeta.hash(), equalTo(expectedMeta.hash()));
            assertThat(actualMeta.writerUuid(), equalTo(expectedMeta.writerUuid()));
        }
    }

    private static FsBlobContainer createBlobContainer() throws IOException {
        final var blobStorePath = PathUtils.get(createTempDir().toString());
        return new FsBlobContainer(new FsBlobStore(randomIntBetween(1, 8) * 1024, blobStorePath, false), BlobPath.EMPTY, blobStorePath);
    }

    private static void assertEmptyFileIsWriteLockOrExtraFile(final String fileName) {
        assertThat(
            "File with zero length should be a write lock or an ExtraFS",
            fileName,
            anyOf(equalTo(IndexWriter.WRITE_LOCK_NAME), startsWith("extra"))
        );
    }
}
