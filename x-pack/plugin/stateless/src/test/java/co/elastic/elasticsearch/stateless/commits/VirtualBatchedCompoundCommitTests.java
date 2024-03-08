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

package co.elastic.elasticsearch.stateless.commits;

import co.elastic.elasticsearch.stateless.lucene.StatelessCommitRef;
import co.elastic.elasticsearch.stateless.test.FakeStatelessNode;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.function.BiConsumer;

import static org.hamcrest.Matchers.containsString;
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
                var indexCommits = fakeNode.generateIndexCommits(numberOfNewCommits);

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
            var commits = fakeNode.generateIndexCommits(numberOfCommits, false, closedCommitRefGenerations::add);

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

    public void testGetVirtualBatchedCompoundCommitBytesByRange() throws Exception {
        var primaryTerm = 1;
        try (var fakeNode = createFakeNode(primaryTerm)) {
            List<Long> closedCommitRefGenerations = new ArrayList<>();
            var commits = fakeNode.generateIndexCommits(randomIntBetween(1, 4), randomBoolean(), closedCommitRefGenerations::add);
            var virtualBatchedCompoundCommit = new VirtualBatchedCompoundCommit(
                fakeNode.shardId,
                "node-id",
                primaryTerm,
                commits.get(0).getGeneration(),
                (fileName) -> {
                    throw new AssertionError("Unexpected call");
                }
            );
            for (StatelessCommitRef statelessCommitRef : commits) {
                virtualBatchedCompoundCommit.appendCommit(statelessCommitRef);
            }

            try (BytesStreamOutput output = new BytesStreamOutput()) {
                virtualBatchedCompoundCommit.writeToStore(output);
                var serializedBatchedCompoundCommit = output.bytes();

                BiConsumer<Long, Long> assertBytesRange = (offset, bytesToRead) -> {
                    var serializedBatchedCompoundCommitBytesRef = new BytesRef(
                        serializedBatchedCompoundCommit.toBytesRef().bytes,
                        offset.intValue(),
                        bytesToRead.intValue()
                    );
                    var bytesStreamOutput = new BytesStreamOutput(bytesToRead.intValue());
                    try {
                        virtualBatchedCompoundCommit.getBytesByRange(offset, bytesToRead, bytesStreamOutput);
                    } catch (IOException e) {
                        assert false : "Unexpected IOException: " + e.getMessage();
                    }
                    assertArrayEquals(
                        BytesRef.deepCopyOf(serializedBatchedCompoundCommitBytesRef).bytes,
                        BytesRef.deepCopyOf(bytesStreamOutput.bytes().toBytesRef()).bytes
                    );
                };

                // Read all vBCC
                assertBytesRange.accept(0L, (long) serializedBatchedCompoundCommit.length());

                // Edge cases
                assertBytesRange.accept(0L, 0L);
                assertBytesRange.accept(0L, 1L); // first byte
                assertBytesRange.accept((long) serializedBatchedCompoundCommit.length() - 1, 0L);
                assertBytesRange.accept((long) serializedBatchedCompoundCommit.length() - 1, 1L); // last byte
                assertBytesRange.accept((long) serializedBatchedCompoundCommit.length(), 0L);

                // Read first header
                long firstHeaderSize = virtualBatchedCompoundCommit.getPendingCompoundCommits().stream().findFirst().get().getHeaderSize();
                assertBytesRange.accept(0L, firstHeaderSize);

                // Read a random file
                StatelessCommitRef randomCommit = commits.get(randomIntBetween(0, commits.size() - 1));
                List<String> randomCommitFiles = randomCommit.getCommitFiles().stream().toList();
                String randomFile = randomCommitFiles.get(randomIntBetween(0, randomCommitFiles.size() - 1));
                BlobLocation randomBlobLocation = virtualBatchedCompoundCommit.getBlobLocation(randomFile);
                assertBytesRange.accept(randomBlobLocation.offset(), randomBlobLocation.fileLength());

                // Random range
                long randomOffset = randomLongBetween(0, serializedBatchedCompoundCommit.length() - 1);
                long randomBytesToRead = randomLongBetween(0, serializedBatchedCompoundCommit.length() - randomOffset);
                assertBytesRange.accept(randomOffset, randomBytesToRead);

                // Close vBCC and expect an exception when trying to read from it
                virtualBatchedCompoundCommit.close();
                VirtualBatchedCompoundCommit.BatchedCompoundCommitAlreadyUploaded exception = expectThrows(
                    VirtualBatchedCompoundCommit.BatchedCompoundCommitAlreadyUploaded.class,
                    () -> virtualBatchedCompoundCommit.getBytesByRange(
                        0L,
                        (long) serializedBatchedCompoundCommit.length(),
                        new BytesStreamOutput(serializedBatchedCompoundCommit.length())
                    )
                );
                assertThat(exception.getMessage(), containsString(fakeNode.shardId.toString()));
            }
        }
    }

    public void testGetVirtualBatchedCompoundCommitBytesByRangeWithConcurrentAppends() throws Exception {
        var primaryTerm = 1;
        try (var fakeNode = createFakeNode(primaryTerm)) {
            List<Long> closedCommitRefGenerations = new ArrayList<>();
            var commits = fakeNode.generateIndexCommits(randomIntBetween(4, 20), randomBoolean(), closedCommitRefGenerations::add);
            var virtualBatchedCompoundCommit = new VirtualBatchedCompoundCommit(
                fakeNode.shardId,
                "node-id",
                primaryTerm,
                commits.get(0).getGeneration(),
                (fileName) -> {
                    throw new AssertionError("Unexpected call");
                }
            );

            if (randomBoolean()) {
                StatelessCommitRef firstCommit = commits.get(0);
                virtualBatchedCompoundCommit.appendCommit(firstCommit);
                commits.remove(0);
            }

            final Semaphore appendBlock = new Semaphore(1);
            Thread appendThread = new Thread(() -> {
                try {
                    for (StatelessCommitRef statelessCommitRef : commits) {
                        appendBlock.acquire(); // wait on the slow validator thread to reach the point that it calls getBytesByRange
                        virtualBatchedCompoundCommit.appendCommit(statelessCommitRef);
                    }
                } catch (Exception e) {
                    assert false : "Unexpected exception: " + e.getMessage();
                }
            }, "TEST-appendThread");
            appendThread.start();

            while (appendThread.isAlive()) {
                if (virtualBatchedCompoundCommit.getPendingCompoundCommits().size() > 0) {
                    try (BytesStreamOutput output = new BytesStreamOutput()) {
                        virtualBatchedCompoundCommit.writeToStore(output);
                        var serializedBatchedCompoundCommit = output.bytes();
                        Long randomOffset = randomLongBetween(0, serializedBatchedCompoundCommit.length() - 1);
                        Long randomBytesToRead = randomLongBetween(0, serializedBatchedCompoundCommit.length() - randomOffset);
                        var serializedBatchedCompoundCommitBytesRef = new BytesRef(
                            serializedBatchedCompoundCommit.toBytesRef().bytes,
                            randomOffset.intValue(),
                            randomBytesToRead.intValue()
                        );
                        var bytesStreamOutput = new BytesStreamOutput(randomBytesToRead.intValue());
                        appendBlock.release();
                        virtualBatchedCompoundCommit.getBytesByRange(randomOffset, randomBytesToRead, bytesStreamOutput);
                        assertArrayEquals(
                            BytesRef.deepCopyOf(serializedBatchedCompoundCommitBytesRef).bytes,
                            BytesRef.deepCopyOf(bytesStreamOutput.bytes().toBytesRef()).bytes
                        );
                    } catch (Exception e) {
                        assert false : "Unexpected exception: " + e.getMessage();
                    }
                }
            }

            virtualBatchedCompoundCommit.close();
        }
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
