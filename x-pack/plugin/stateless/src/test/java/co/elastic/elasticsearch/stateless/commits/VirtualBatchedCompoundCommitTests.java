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

import co.elastic.elasticsearch.stateless.lucene.StatelessCommitRef;
import co.elastic.elasticsearch.stateless.test.FakeStatelessNode;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoDeletionPolicy;
import org.apache.lucene.store.IOContext;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongConsumer;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class VirtualBatchedCompoundCommitTests extends ESTestCase {

    public void testWrittenBatchedCompoundCommitCanBeRead() throws Exception {
        Map<String, BlobLocation> uploadedBlobLocations = new HashMap<>();
        Map<String, BytesReference> batchedCompoundCommitBlobs = new HashMap<>();

        var primaryTerm = 1;
        try (var fakeNode = createFakeNode(primaryTerm)) {
            var numberOfBatches = randomIntBetween(2, 4);
            for (int batchNumber = 1; batchNumber <= numberOfBatches; batchNumber++) {

                var numberOfNewCommits = randomIntBetween(1, 4);
                var indexCommits = generateIndexCommits(fakeNode, numberOfNewCommits);

                long firstCommitGeneration = indexCommits.get(0).getGeneration();
                var virtualBatchedCompoundCommit = new VirtualBatchedCompoundCommit(
                    fakeNode.shardId,
                    "node-id",
                    primaryTerm,
                    firstCommitGeneration,
                    uploadedBlobLocations::get
                );
                for (StatelessCommitRef statelessCommitRef : indexCommits) {
                    virtualBatchedCompoundCommit.appendCommit(statelessCommitRef);
                }

                try (BytesStreamOutput output = new BytesStreamOutput()) {
                    var batchedCompoundCommit = virtualBatchedCompoundCommit.writeToStore(output);
                    virtualBatchedCompoundCommit.close();

                    var serializedBatchedCompoundCommit = output.bytes();
                    batchedCompoundCommitBlobs.put(virtualBatchedCompoundCommit.getBlobName(), serializedBatchedCompoundCommit);

                    var deserializedBatchedCompoundCommit = BatchedCompoundCommit.readFromStore(
                        virtualBatchedCompoundCommit.getBlobName(),
                        output.size(),
                        (blobName, offset, length) -> serializedBatchedCompoundCommit.slice((int) offset, (int) length).streamInput()
                    );
                    assertEquals(batchedCompoundCommit, deserializedBatchedCompoundCommit);

                    // Ensure that the contents written into the blob store are the same as the local files
                    for (StatelessCompoundCommit compoundCommit : deserializedBatchedCompoundCommit.compoundCommits()) {
                        // Update uploaded blob locations that can be used in the next batched compound commits
                        uploadedBlobLocations.putAll(compoundCommit.commitFiles());

                        for (Map.Entry<String, BlobLocation> commitFileBlobLocation : compoundCommit.commitFiles().entrySet()) {
                            var fileName = commitFileBlobLocation.getKey();
                            var blobLocation = commitFileBlobLocation.getValue();

                            // TODO: compare checksums instead
                            byte[] batchedCompoundCommitFileContents = readFileFromBlob(batchedCompoundCommitBlobs, blobLocation);
                            byte[] localFileContents = readLocalFile(fakeNode, fileName);
                            assertArrayEquals(batchedCompoundCommitFileContents, localFileContents);
                        }
                    }
                }
            }
        }
    }

    public void testAccumulatedCommitsAreReleasedOnceVirtualBatchedCompoundCommitIsClosed() throws Exception {
        var primaryTerm = 1;
        try (var fakeNode = createFakeNode(primaryTerm)) {
            var numberOfCommits = randomIntBetween(2, 4);
            List<Long> closedCommitRefGenerations = new ArrayList<>();
            var commits = generateIndexCommits(fakeNode, numberOfCommits, closedCommitRefGenerations::add);

            long firstCommitGeneration = commits.get(0).getGeneration();
            var virtualBatchedCompoundCommit = new VirtualBatchedCompoundCommit(
                fakeNode.shardId,
                "node-id",
                primaryTerm,
                firstCommitGeneration,
                (fileName) -> {
                    throw new AssertionError("Unexpected call");
                }
            );

            for (StatelessCommitRef commit : commits) {
                virtualBatchedCompoundCommit.appendCommit(commit);
            }

            assertThat(closedCommitRefGenerations, is(empty()));
            virtualBatchedCompoundCommit.incRef();

            virtualBatchedCompoundCommit.close();
            // There's still an outstanding ref (i.e. it's reading the local files)
            assertThat(closedCommitRefGenerations, is(empty()));

            virtualBatchedCompoundCommit.decRef();
            assertThat(closedCommitRefGenerations, hasSize(numberOfCommits));
            for (StatelessCommitRef commit : commits) {
                assertThat(closedCommitRefGenerations, hasItem(commit.getGeneration()));
            }
        }
    }

    // TODO: move this method to FakeStatelessNode
    private List<StatelessCommitRef> generateIndexCommits(FakeStatelessNode testHarness, int commitsNumber) throws IOException {
        return generateIndexCommits(testHarness, commitsNumber, generation -> {});
    }

    private List<StatelessCommitRef> generateIndexCommits(FakeStatelessNode testHarness, int commitsNumber, LongConsumer onCommitClosed)
        throws IOException {
        List<StatelessCommitRef> commits = new ArrayList<>(commitsNumber);
        Set<String> previousCommit;

        final var indexWriterConfig = new IndexWriterConfig(new KeywordAnalyzer());
        indexWriterConfig.setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE);
        String deleteId = randomAlphaOfLength(10);

        try (var indexWriter = new IndexWriter(testHarness.indexingStore.directory(), indexWriterConfig)) {
            try (var indexReader = DirectoryReader.open(indexWriter)) {
                IndexCommit indexCommit = indexReader.getIndexCommit();
                previousCommit = new HashSet<>(indexCommit.getFileNames());
            }
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
                try (var indexReader = DirectoryReader.open(indexWriter)) {
                    IndexCommit indexCommit = indexReader.getIndexCommit();
                    Set<String> commitFiles = new HashSet<>(indexCommit.getFileNames());
                    Set<String> additionalFiles = Sets.difference(commitFiles, previousCommit);
                    previousCommit = commitFiles;

                    StatelessCommitRef statelessCommitRef = new StatelessCommitRef(
                        testHarness.shardId,
                        new Engine.IndexCommitRef(indexCommit, () -> onCommitClosed.accept(indexCommit.getGeneration())),
                        commitFiles,
                        additionalFiles,
                        1,
                        0
                    );
                    commits.add(statelessCommitRef);
                }
            }
        }

        return commits;
    }

    private static byte[] readLocalFile(FakeStatelessNode testHarness, String fileName) throws IOException {
        try (var localIndexInput = testHarness.indexingDirectory.openInput(fileName, IOContext.READONCE)) {
            int length = (int) localIndexInput.length();
            var data = new byte[length];
            localIndexInput.readBytes(data, 0, length);
            return data;
        }
    }

    private static byte[] readFileFromBlob(Map<String, BytesReference> blobs, BlobLocation blobLocation) throws IOException {
        int fileLength = (int) blobLocation.fileLength();
        int offsetWithinBatchedCompoundCommitFile = (int) blobLocation.offset();
        var compoundCommitFileContents = new byte[fileLength];
        var batchedCompoundCommitBlob = blobs.get(blobLocation.blobName());
        var fileContent = batchedCompoundCommitBlob.slice(offsetWithinBatchedCompoundCommitFile, fileLength);
        try (var input = fileContent.streamInput()) {
            input.read(compoundCommitFileContents, 0, compoundCommitFileContents.length);
            return compoundCommitFileContents;
        }
    }

    private FakeStatelessNode createFakeNode(long primaryTerm) throws IOException {
        return new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), primaryTerm);
    }

    private static void assertEquals(BatchedCompoundCommit batchedCompoundCommit, BatchedCompoundCommit deserializedBatchedCompoundCommit) {
        assertNotSame(deserializedBatchedCompoundCommit, batchedCompoundCommit);
        assertThat(deserializedBatchedCompoundCommit, equalTo(batchedCompoundCommit));
    }
}
