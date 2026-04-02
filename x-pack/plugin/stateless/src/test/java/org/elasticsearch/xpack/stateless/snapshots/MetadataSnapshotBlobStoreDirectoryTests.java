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

package org.elasticsearch.xpack.stateless.snapshots;

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class MetadataSnapshotBlobStoreDirectoryTests extends ESTestCase {

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
                long offset = baos.size();
                long fileLength = localDir.fileLength(fileName);
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
                actual = Store.MetadataSnapshot.loadFromIndexCommit(
                    null,
                    blobDir,
                    LogManager.getLogger(MetadataSnapshotBlobStoreDirectoryTests.class)
                );
            }

            assertThat(actual.numDocs(), equalTo(expected.numDocs()));
            assertThat(actual.fileMetadataMap().keySet(), equalTo(expected.fileMetadataMap().keySet()));
            for (var entry : expected.fileMetadataMap().entrySet()) {
                assertThat(actual.get(entry.getKey()).checksum(), equalTo(entry.getValue().checksum()));
                assertThat(actual.get(entry.getKey()).hash(), equalTo(entry.getValue().hash()));
            }
        }
    }

    private static FsBlobContainer createBlobContainer() throws IOException {
        final var blobStorePath = PathUtils.get(createTempDir().toString());
        return new FsBlobContainer(new FsBlobStore(randomIntBetween(1, 8) * 1024, blobStorePath, false), BlobPath.EMPTY, blobStorePath);
    }
}
