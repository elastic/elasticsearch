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

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.lucene.store.BytesReferenceIndexInput;
import org.elasticsearch.core.Streams;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.function.BiConsumer;

import static org.hamcrest.Matchers.anyOf;
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
                    uploadedBlobLocations::get,
                    ESTestCase::randomNonNegativeLong,
                    fakeNode.sharedCacheService.getRegionSize(),
                    randomIntBetween(0, fakeNode.sharedCacheService.getRegionSize())
                );
                for (StatelessCommitRef statelessCommitRef : indexCommits) {
                    assertTrue(virtualBatchedCompoundCommit.appendCommit(statelessCommitRef, randomBoolean()));
                }
                virtualBatchedCompoundCommit.freeze();

                try (BytesStreamOutput output = new BytesStreamOutput()) {
                    assertTrue(virtualBatchedCompoundCommit.isFrozen());
                    try (var frozenInputStream = virtualBatchedCompoundCommit.getFrozenInputStreamForUpload()) {
                        Streams.copy(frozenInputStream, output, false);
                    }
                    var batchedCompoundCommit = virtualBatchedCompoundCommit.getFrozenBatchedCompoundCommit();
                    virtualBatchedCompoundCommit.close();

                    var serializedBatchedCompoundCommit = output.bytes();
                    batchedCompoundCommitBlobs.put(virtualBatchedCompoundCommit.getBlobName(), serializedBatchedCompoundCommit);

                    var deserializedBatchedCompoundCommit = BatchedCompoundCommit.readFromStore(
                        virtualBatchedCompoundCommit.getBlobName(),
                        output.size(),
                        (blobName, offset, length) -> serializedBatchedCompoundCommit.slice((int) offset, (int) length).streamInput(),
                        true
                    );
                    assertEquals(batchedCompoundCommit, deserializedBatchedCompoundCommit);

                    // Ensure that the contents written into the blob store are the same as the local files
                    for (StatelessCompoundCommit compoundCommit : deserializedBatchedCompoundCommit.compoundCommits()) {
                        // Update uploaded blob locations that can be used in the next batched compound commits
                        uploadedBlobLocations.putAll(compoundCommit.commitFiles());

                        Map<String, BlobLocation> commitFiles = compoundCommit.commitFiles();
                        // Make sure that all internal files are on the same blob
                        Set<String> internalFiles = compoundCommit.getInternalFiles();
                        internalFiles.forEach(f -> assertEquals(virtualBatchedCompoundCommit.getBlobName(), commitFiles.get(f).blobName()));
                        // Check that internalFiles are sorted according to the file size and name
                        assertThat(
                            internalFiles.stream().sorted(Comparator.comparingLong(e -> commitFiles.get(e).offset())).toList(),
                            equalTo(
                                internalFiles.stream()
                                    .sorted(Comparator.<String>comparingLong(e -> commitFiles.get(e).fileLength()).thenComparing(it -> it))
                                    .toList()
                            )
                        );

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
            var commits = fakeNode.generateIndexCommits(numberOfCommits, false, true, closedCommitRefGenerations::add);

            long firstCommitGeneration = commits.get(0).getGeneration();
            var virtualBatchedCompoundCommit = new VirtualBatchedCompoundCommit(
                fakeNode.shardId,
                "node-id",
                primaryTerm,
                firstCommitGeneration,
                (fileName) -> {
                    throw new AssertionError("Unexpected call");
                },
                ESTestCase::randomNonNegativeLong,
                fakeNode.sharedCacheService.getRegionSize(),
                randomIntBetween(0, fakeNode.sharedCacheService.getRegionSize())
            );

            for (StatelessCommitRef commit : commits) {
                assertTrue(virtualBatchedCompoundCommit.appendCommit(commit, randomBoolean()));
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
            var commits = fakeNode.generateIndexCommits(randomIntBetween(1, 4), randomBoolean(), true, closedCommitRefGenerations::add);
            var virtualBatchedCompoundCommit = new VirtualBatchedCompoundCommit(
                fakeNode.shardId,
                "node-id",
                primaryTerm,
                commits.get(0).getGeneration(),
                (fileName) -> {
                    throw new AssertionError("Unexpected call");
                },
                ESTestCase::randomNonNegativeLong,
                fakeNode.sharedCacheService.getRegionSize(),
                randomIntBetween(0, fakeNode.sharedCacheService.getRegionSize())
            );
            for (StatelessCommitRef statelessCommitRef : commits) {
                assertTrue(virtualBatchedCompoundCommit.appendCommit(statelessCommitRef, randomBoolean()));
            }
            virtualBatchedCompoundCommit.freeze();

            try (BytesStreamOutput output = new BytesStreamOutput()) {
                assertTrue(virtualBatchedCompoundCommit.isFrozen());
                try (var frozenInputStream = virtualBatchedCompoundCommit.getFrozenInputStreamForUpload()) {
                    Streams.copy(frozenInputStream, output, false);
                }
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
                var firstCC = virtualBatchedCompoundCommit.getPendingCompoundCommits().getFirst();
                long firstCCHeaderSize = firstCC.getHeaderSize();
                assertBytesRange.accept(0L, firstCCHeaderSize);

                // Read first StatelessCompoundCommit (without header)
                long firstCCWithoutHeaderSize = firstCC.getStatelessCompoundCommit().sizeInBytes() - firstCCHeaderSize;
                assertBytesRange.accept(firstCCHeaderSize, firstCCWithoutHeaderSize);

                // Read first padding
                long firstCCPaddingSize = firstCC.getSizeInBytes() - firstCCHeaderSize - firstCCWithoutHeaderSize;
                assertBytesRange.accept(firstCCHeaderSize + firstCCWithoutHeaderSize, firstCCPaddingSize);
                assert firstCC.getSizeInBytes() == firstCCHeaderSize + firstCCWithoutHeaderSize + firstCCPaddingSize
                    : "the compound commit size "
                        + firstCC.getSizeInBytes()
                        + " is not equal to the sum of its parts of header "
                        + firstCCHeaderSize
                        + ", commit "
                        + firstCCWithoutHeaderSize
                        + " and padding "
                        + firstCCPaddingSize;

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
                ResourceNotFoundException exception = expectThrows(
                    ResourceNotFoundException.class,
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
            var commits = fakeNode.generateIndexCommits(randomIntBetween(4, 20), randomBoolean(), true, closedCommitRefGenerations::add);
            var virtualBatchedCompoundCommit = new VirtualBatchedCompoundCommit(
                fakeNode.shardId,
                "node-id",
                primaryTerm,
                commits.get(0).getGeneration(),
                (fileName) -> {
                    throw new AssertionError("Unexpected call");
                },
                ESTestCase::randomNonNegativeLong,
                fakeNode.sharedCacheService.getRegionSize(),
                randomIntBetween(0, fakeNode.sharedCacheService.getRegionSize())
            );

            if (randomBoolean()) {
                StatelessCommitRef firstCommit = commits.get(0);
                assertTrue(virtualBatchedCompoundCommit.appendCommit(firstCommit, randomBoolean()));
                commits.remove(0);
            }

            final Semaphore appendBlock = new Semaphore(1);
            Thread appendThread = new Thread(() -> {
                try {
                    for (StatelessCommitRef statelessCommitRef : commits) {
                        appendBlock.acquire(); // wait on the slow validator thread to reach the point that it calls getBytesByRange
                        assertTrue(virtualBatchedCompoundCommit.appendCommit(statelessCommitRef, randomBoolean()));
                    }
                } catch (Exception e) {
                    assert false : "Unexpected exception: " + e.getMessage();
                }
            }, "TEST-appendThread");
            appendThread.start();

            while (appendThread.isAlive()) {
                if (virtualBatchedCompoundCommit.getPendingCompoundCommits().size() > 0) {
                    try (BytesStreamOutput output = new BytesStreamOutput()) {
                        // Workaround to serialize VBCC without freezing for testing
                        try (var vbccInputStream = virtualBatchedCompoundCommit.getInputStreamForUpload()) {
                            Streams.copy(vbccInputStream, output, false);
                        }

                        var serializedBatchedCompoundCommit = output.bytes();
                        int randomOffset = randomIntBetween(0, serializedBatchedCompoundCommit.length() - 1);
                        int randomBytesToRead = randomIntBetween(0, serializedBatchedCompoundCommit.length() - randomOffset);
                        var serializedBatchedCompoundCommitBytesRef = new BytesRef(
                            serializedBatchedCompoundCommit.toBytesRef().bytes,
                            randomOffset,
                            randomBytesToRead
                        );
                        var bytesStreamOutput = new BytesStreamOutput(randomBytesToRead);
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

    public void testFreeze() throws IOException {
        var primaryTerm = 1;
        try (var fakeNode = createFakeNode(primaryTerm)) {
            var numberOfCommits = randomIntBetween(2, 4);
            var commits = fakeNode.generateIndexCommits(numberOfCommits);

            long firstCommitGeneration = commits.get(0).getGeneration();
            var virtualBatchedCompoundCommit = new VirtualBatchedCompoundCommit(
                fakeNode.shardId,
                "node-id",
                primaryTerm,
                firstCommitGeneration,
                (fileName) -> {
                    throw new AssertionError("Unexpected call");
                },
                ESTestCase::randomNonNegativeLong,
                fakeNode.sharedCacheService.getRegionSize(),
                randomIntBetween(0, fakeNode.sharedCacheService.getRegionSize())
            );

            for (StatelessCommitRef commit : commits) {
                assertTrue(virtualBatchedCompoundCommit.appendCommit(commit, randomBoolean()));
            }

            try (BytesStreamOutput output = new BytesStreamOutput()) {
                assertTrue(virtualBatchedCompoundCommit.freeze());
                assertTrue(virtualBatchedCompoundCommit.isFrozen());
                if (randomBoolean()) { // extra freeze is a no-op
                    assertFalse(virtualBatchedCompoundCommit.freeze());
                }
                try (var frozenInputStream = virtualBatchedCompoundCommit.getFrozenInputStreamForUpload()) {
                    Streams.copy(frozenInputStream, output, false);
                }
            }

            final StatelessCommitRef newCommitRef = fakeNode.generateIndexCommits(1).get(0);
            assertFalse(virtualBatchedCompoundCommit.appendCommit(newCommitRef, randomBoolean()));
        }
    }

    public void testReplicatedContent() throws IOException {
        var primaryTerm = 1;
        try (var fakeNode = createFakeNode(primaryTerm)) {
            var numberOfNewCommits = randomIntBetween(1, 10);
            var commits = fakeNode.generateIndexCommits(numberOfNewCommits);
            var virtualBatchedCompoundCommit = new VirtualBatchedCompoundCommit(
                fakeNode.shardId,
                "node-id",
                primaryTerm,
                commits.getFirst().getGeneration(),
                (fileName) -> {
                    throw new AssertionError("Unexpected call");
                },
                ESTestCase::randomNonNegativeLong,
                fakeNode.sharedCacheService.getRegionSize(),
                randomIntBetween(0, fakeNode.sharedCacheService.getRegionSize())
            );

            for (StatelessCommitRef commit : commits) {
                assertTrue(virtualBatchedCompoundCommit.appendCommit(commit, true));
            }

            var batchedCompoundCommitBlobs = new HashMap<String, BytesReference>();

            try (BytesStreamOutput output = new BytesStreamOutput()) {
                try (var vbccInputStream = virtualBatchedCompoundCommit.getInputStreamForUpload()) {
                    Streams.copy(vbccInputStream, output, false);
                }
                batchedCompoundCommitBlobs.put(virtualBatchedCompoundCommit.getBlobName(), output.bytes());

                long lastCommitPosition = 0;
                for (var commit : virtualBatchedCompoundCommit.getPendingCompoundCommits()) {
                    // check replicated content
                    try (var vbccIndexInput = new BytesReferenceIndexInput("test", output.bytes())) {
                        long consumedReplicatedRangeSize = 0;
                        var replicatedRanges = commit.getStatelessCompoundCommit().internalFilesReplicatedRanges();
                        for (var replicatedRange : replicatedRanges.replicatedRanges()) {
                            // range represents header or footer
                            vbccIndexInput.seek(lastCommitPosition + commit.getHeaderSize() + consumedReplicatedRangeSize);
                            assertThat(
                                CodecUtil.readBEInt(vbccIndexInput),
                                anyOf(equalTo(CodecUtil.CODEC_MAGIC), equalTo(CodecUtil.FOOTER_MAGIC))
                            );
                            // range is the same as original content
                            byte[] replicatedBytes = readBytes(
                                vbccIndexInput,
                                lastCommitPosition + commit.getHeaderSize() + consumedReplicatedRangeSize,
                                replicatedRange.length()
                            );
                            byte[] originalBytes = readBytes(
                                vbccIndexInput,
                                lastCommitPosition + commit.getHeaderSize() + replicatedRanges.dataSizeInBytes() + replicatedRange
                                    .position(),
                                replicatedRange.length()
                            );
                            assertArrayEquals("Replicated range is not same as original content", originalBytes, replicatedBytes);
                            consumedReplicatedRangeSize += replicatedRange.length();
                        }
                    }

                    // check files
                    for (var commitFileBlobLocation : commit.getStatelessCompoundCommit().commitFiles().entrySet()) {
                        var fileName = commitFileBlobLocation.getKey();
                        var blobLocation = commitFileBlobLocation.getValue();

                        byte[] batchedCompoundCommitFileContents = readFileFromBlob(batchedCompoundCommitBlobs, blobLocation);
                        byte[] localFileContents = readLocalFile(fakeNode, fileName);
                        assertArrayEquals(batchedCompoundCommitFileContents, localFileContents);
                    }
                    lastCommitPosition += commit.getSizeInBytes();
                }
            }
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

    private static byte[] readBytes(BytesReferenceIndexInput input, long position, int length) throws IOException {
        var bytes = new byte[length];
        input.seek(position);
        input.readBytes(bytes, 0, length);
        return bytes;
    }

    private FakeStatelessNode createFakeNode(long primaryTerm) throws IOException {
        return new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), primaryTerm);
    }

    private static void assertEquals(BatchedCompoundCommit batchedCompoundCommit, BatchedCompoundCommit deserializedBatchedCompoundCommit) {
        assertNotSame(deserializedBatchedCompoundCommit, batchedCompoundCommit);
        assertThat(deserializedBatchedCompoundCommit, equalTo(batchedCompoundCommit));
    }

}
