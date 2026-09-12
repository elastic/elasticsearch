/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.FilterStreamInput;
import org.elasticsearch.common.lucene.store.BytesReferenceIndexInput;
import org.elasticsearch.core.Streams;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;
import org.elasticsearch.xpack.stateless.lucene.StatelessCommitRef;
import org.elasticsearch.xpack.stateless.test.FakeStatelessNode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

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
                    assertTrue(virtualBatchedCompoundCommit.appendCommit(statelessCommitRef, randomBoolean(), null));
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

                    BatchedCompoundCommit deserializedBatchedCompoundCommit = deserializeBatchedCompoundCommit(
                        virtualBatchedCompoundCommit.getBlobName(),
                        output
                    );
                    assertEquals(batchedCompoundCommit, deserializedBatchedCompoundCommit);
                    assertEquals(output.size(), batchedCompoundCommit.calculateBccBlobLength());

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

    // generate a commit, corrupt some files, and expect a CorruptIndexException while uploading it
    public void testVirtualBatchedCompoundCommitVerifiesIndexFileIntegrity() throws IOException {
        var primaryTerm = 1;
        try (var fakeNode = createFakeNode(primaryTerm)) {
            // create commit and install in VBCC
            var commit = fakeNode.generateIndexCommits(1).getFirst();

            var virtualBatchedCompoundCommit = new VirtualBatchedCompoundCommit(
                fakeNode.shardId,
                "node-id",
                primaryTerm,
                commit.getGeneration(),
                (fileName) -> {
                    throw new AssertionError("Unexpected call");
                },
                ESTestCase::randomNonNegativeLong,
                fakeNode.sharedCacheService.getRegionSize(),
                randomIntBetween(0, fakeNode.sharedCacheService.getRegionSize())
            );

            assertTrue(virtualBatchedCompoundCommit.appendCommit(commit, randomBoolean(), null));

            virtualBatchedCompoundCommit.freeze();

            // corrupt a random subset of commit files
            var fsPath = fakeNode.indexingShardPath.resolveIndex();
            for (var toCorrupt : randomNonEmptySubsetOf(commit.getCommitFiles())) {
                var targetPath = fsPath.resolve(toCorrupt);

                var targetFile = FileChannel.open(targetPath, StandardOpenOption.READ, StandardOpenOption.WRITE);
                long position = randomLongBetween(0, targetFile.size() - 1);
                logger.info("corrupting {} at {}", targetPath, position);

                var buf = ByteBuffer.allocate(1);
                assertEquals(1, targetFile.read(buf, position));
                buf.flip();

                byte b = buf.get();
                buf.clear();

                buf.put((byte) ~b);
                buf.flip();
                assertEquals(1, targetFile.write(buf, position));

                targetFile.close();
            }

            // expect CorruptIndexException when the VBCC upload stream is consumed.
            // At least AWS SDKv1 doesn't read to EOF or close the input stream until after it has finished writing the object
            // (as a way to make reads retryable), so we emulate that here (ES-10931)
            final int commitSize = (int) virtualBatchedCompoundCommit.getTotalSizeInBytes();
            try (var frozenInputStream = virtualBatchedCompoundCommit.getFrozenInputStreamForUpload()) {
                var output = new byte[commitSize];
                assertThrows(CorruptIndexException.class, () -> Streams.readFully(frozenInputStream, output, 0, output.length));
            }

            // verify that corruption is also detected when reading the VBCC in chunks (as for concurrent multipart uploads)
            final int chunkSize = randomIntBetween(100, 1000);
            assertThrows(CorruptIndexException.class, () -> {
                for (int i = 0; i < commitSize; i += chunkSize) {
                    int len = Math.min(chunkSize, commitSize - i);
                    var output = new byte[len];
                    try (var chunkStream = virtualBatchedCompoundCommit.getFrozenInputStreamForUpload(i, len)) {
                        Streams.readFully(chunkStream, output, 0, len);
                    }
                }
            });
            virtualBatchedCompoundCommit.close();
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
                assertTrue(virtualBatchedCompoundCommit.appendCommit(commit, randomBoolean(), null));
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
                assertTrue(virtualBatchedCompoundCommit.appendCommit(statelessCommitRef, randomBoolean(), null));
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
                long firstCCHeaderSize = VirtualBatchedCompoundCommitTestUtils.getHeaderSize(firstCC);
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
                assertTrue(virtualBatchedCompoundCommit.appendCommit(firstCommit, randomBoolean(), null));
                commits.remove(0);
            }

            final Semaphore appendBlock = new Semaphore(1);
            Thread appendThread = new Thread(() -> {
                try {
                    for (StatelessCommitRef statelessCommitRef : commits) {
                        appendBlock.acquire(); // wait on the slow validator thread to reach the point that it calls getBytesByRange
                        assertTrue(virtualBatchedCompoundCommit.appendCommit(statelessCommitRef, randomBoolean(), null));
                    }
                } catch (Exception e) {
                    assert false : "Unexpected exception: " + e.getMessage();
                }
            }, "TEST-appendThread");
            appendThread.start();

            record VbccRangeRead(int offset, int length, BytesRef expected) {}
            var performedReads = new ArrayList<VbccRangeRead>();

            while (appendThread.isAlive()) {
                if (virtualBatchedCompoundCommit.getPendingCompoundCommits().size() > 0) {
                    // Workaround to serialize VBCC without freezing for testing
                    try (
                        var vbccInputStream = VirtualBatchedCompoundCommitTestUtils.getInputStreamForUpload(virtualBatchedCompoundCommit)
                    ) {
                        // At this point a commit is concurrently appended to this vbcc by `appendThread`.
                        // Appending a commit first changes `internalDataReadersByOffset` and then advances `currentOffset`.
                        // `vbcc#getInputStreamForUpload()` returns a stream that contains all files in `internalDataReadersByOffset`
                        // but some of the files are not ready to be read at this point because `currentOffset` is not advanced yet.
                        // Doing that causes an assertion failure in `getBytesByRange()`.
                        // By clamping the commit to `getTotalSizeInBytes()` we ensure we only read files once they are fully appended.
                        // Note that this is not a problem in production code since 1) commit appends and freeze/uploads are synchronized
                        // in the caller, 2) callers also clamp the vbcc stream using `getTotalSizeInBytes()`.
                        // This test is still valuable because we want to verify that concurrent appends do not impact
                        // reading previously appended commit data (the race explained above only impacts currently appended commit files).
                        var serializedBatchedCompoundCommit = vbccInputStream.readNBytes(
                            (int) virtualBatchedCompoundCommit.getTotalSizeInBytes()
                        );
                        int randomOffset = randomIntBetween(0, serializedBatchedCompoundCommit.length - 1);
                        int randomBytesToRead = randomIntBetween(0, serializedBatchedCompoundCommit.length - randomOffset);
                        var serializedBatchedCompoundCommitBytesRef = new BytesRef(
                            serializedBatchedCompoundCommit,
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
                        performedReads.add(
                            new VbccRangeRead(randomOffset, randomBytesToRead, BytesRef.deepCopyOf(serializedBatchedCompoundCommitBytesRef))
                        );
                    } catch (Exception e) {
                        assert false : "Unexpected exception: " + e.getMessage();
                    }
                }
            }

            virtualBatchedCompoundCommit.freeze();
            var bytesToUpload = virtualBatchedCompoundCommit.getFrozenInputStreamForUpload().readAllBytes();
            for (var read : performedReads) {
                var actualRangeData = new BytesRef(bytesToUpload, read.offset, read.length);
                assertTrue(actualRangeData.bytesEquals(read.expected));
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
                assertTrue(virtualBatchedCompoundCommit.appendCommit(commit, randomBoolean(), null));
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
            assertFalse(virtualBatchedCompoundCommit.appendCommit(newCommitRef, randomBoolean(), null));
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
                assertTrue(virtualBatchedCompoundCommit.appendCommit(commit, true, null));
            }

            var batchedCompoundCommitBlobs = new HashMap<String, BytesReference>();

            try (BytesStreamOutput output = new BytesStreamOutput()) {
                try (var vbccInputStream = VirtualBatchedCompoundCommitTestUtils.getInputStreamForUpload(virtualBatchedCompoundCommit)) {
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
                            vbccIndexInput.seek(
                                lastCommitPosition + VirtualBatchedCompoundCommitTestUtils.getHeaderSize(commit)
                                    + consumedReplicatedRangeSize
                            );
                            assertThat(
                                CodecUtil.readBEInt(vbccIndexInput),
                                anyOf(equalTo(CodecUtil.CODEC_MAGIC), equalTo(CodecUtil.FOOTER_MAGIC))
                            );
                            // range is the same as original content
                            byte[] replicatedBytes = readBytes(
                                vbccIndexInput,
                                lastCommitPosition + VirtualBatchedCompoundCommitTestUtils.getHeaderSize(commit)
                                    + consumedReplicatedRangeSize,
                                replicatedRange.length()
                            );
                            byte[] originalBytes = readBytes(
                                vbccIndexInput,
                                lastCommitPosition + VirtualBatchedCompoundCommitTestUtils.getHeaderSize(commit) + replicatedRanges
                                    .dataSizeInBytes() + replicatedRange.position(),
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

    /**
     * Verifies {@code InternalHeaderReader} re-materialization: ranged reads over header regions are byte-identical to the uploaded blob,
     * sub-ranges mid-header are correct, and a blob reassembled from chunked reads
     * round-trips through {@code deserializeBatchedCompoundCommit}.
     */
    public void testHeaderBytesAreRematerializedConsistently() throws Exception {
        var primaryTerm = 1;
        try (var fakeNode = createFakeNode(primaryTerm)) {
            var commits = fakeNode.generateIndexCommits(randomIntBetween(2, 4));
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
            for (StatelessCommitRef statelessCommitRef : commits) {
                assertTrue(virtualBatchedCompoundCommit.appendCommit(statelessCommitRef, randomBoolean(), null));
            }
            virtualBatchedCompoundCommit.freeze();

            try (BytesStreamOutput output = new BytesStreamOutput()) {
                try (var frozenInputStream = virtualBatchedCompoundCommit.getFrozenInputStreamForUpload()) {
                    Streams.copy(frozenInputStream, output, false);
                }
                var serializedBatchedCompoundCommit = output.bytes();

                // Reassemble the whole blob from ranged reads of random sizes, as a search node's chunk requests would
                try (BytesStreamOutput reassembled = new BytesStreamOutput()) {
                    long offset = 0;
                    while (offset < serializedBatchedCompoundCommit.length()) {
                        long chunkSize = Math.min(randomLongBetween(1, 128 * 1024), serializedBatchedCompoundCommit.length() - offset);
                        virtualBatchedCompoundCommit.getBytesByRange(offset, chunkSize, reassembled);
                        offset += chunkSize;
                    }
                    assertArrayEquals(BytesReference.toBytes(serializedBatchedCompoundCommit), BytesReference.toBytes(reassembled.bytes()));
                    assertEquals(
                        virtualBatchedCompoundCommit.getFrozenBatchedCompoundCommit(),
                        deserializeBatchedCompoundCommit(virtualBatchedCompoundCommit.getBlobName(), reassembled)
                    );
                }

                long ccOffset = 0;
                for (var pendingCompoundCommit : virtualBatchedCompoundCommit.getPendingCompoundCommits()) {
                    long headerSize = VirtualBatchedCompoundCommitTestUtils.getHeaderSize(pendingCompoundCommit);
                    byte[] expectedHeader = BytesReference.toBytes(
                        serializedBatchedCompoundCommit.slice(Math.toIntExact(ccOffset), Math.toIntExact(headerSize))
                    );
                    int readCount = randomIntBetween(2, 4);
                    for (int i = 0; i < readCount; i++) {
                        try (BytesStreamOutput headerOutput = new BytesStreamOutput()) {
                            virtualBatchedCompoundCommit.getBytesByRange(ccOffset, headerSize, headerOutput);
                            assertArrayEquals(expectedHeader, BytesReference.toBytes(headerOutput.bytes()));
                        }
                    }

                    long subOffset = randomLongBetween(0, headerSize - 1);
                    long subLength = randomLongBetween(0, headerSize - subOffset);
                    try (BytesStreamOutput subRangeOutput = new BytesStreamOutput()) {
                        virtualBatchedCompoundCommit.getBytesByRange(ccOffset + subOffset, subLength, subRangeOutput);
                        assertArrayEquals(
                            BytesReference.toBytes(
                                serializedBatchedCompoundCommit.slice(Math.toIntExact(ccOffset + subOffset), Math.toIntExact(subLength))
                            ),
                            BytesReference.toBytes(subRangeOutput.bytes())
                        );
                    }
                    ccOffset += pendingCompoundCommit.getSizeInBytes();
                }
            }
            virtualBatchedCompoundCommit.close();
        }
    }

    public static BatchedCompoundCommit deserializeBatchedCompoundCommit(String blobName, BytesStreamOutput output) throws IOException {
        if (randomBoolean()) {
            return BatchedCompoundCommit.readFromStore(
                blobName,
                output.size(),
                (ignored, offset, length) -> output.bytes().slice((int) offset, (int) length).streamInput(),
                true
            );
        } else {
            var bytesRead = new AtomicInteger(0);
            var bccIterator = BatchedCompoundCommit.readFromStoreIncrementally(
                blobName,
                output.size(),
                (ignored, offset, length) -> new FilterStreamInput(output.bytes().slice((int) offset, (int) length).streamInput()) {
                    @Override
                    public byte readByte() throws IOException {
                        bytesRead.incrementAndGet();
                        return super.readByte();
                    }

                    @Override
                    public void readBytes(byte[] b, int offset, int len) throws IOException {
                        bytesRead.addAndGet(len);
                        super.readBytes(b, offset, len);
                    }
                },
                true
            );
            assertThat(bytesRead.get(), equalTo(0));
            List<StatelessCompoundCommit> compoundCommits = new ArrayList<>();
            PrimaryTermAndGeneration bccTermAndGen = null;
            var lastObservedBytesRead = 0;
            while (bccIterator.hasNext()) {
                // The read is actually triggered once the #next element is requested
                assertThat(bytesRead.get(), equalTo(lastObservedBytesRead));
                var compoundCommit = bccIterator.next();
                assertThat(bytesRead.get(), is(greaterThan(lastObservedBytesRead)));
                lastObservedBytesRead = bytesRead.get();
                if (bccTermAndGen == null) {
                    bccTermAndGen = compoundCommit.primaryTermAndGeneration();
                }
                compoundCommits.add(compoundCommit);
            }
            return new BatchedCompoundCommit(bccTermAndGen, compoundCommits);
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

    /**
     * The concurrent multipart upload path in {@code AzureBlobStore} builds a blob from one
     * {@link VirtualBatchedCompoundCommit#getFrozenInputStreamForUpload(long, long)} call per part. Concatenating
     * those parts must reproduce the blob byte for byte.
     * <p>
     * Part sizes below deliberately include values larger than the compound commit header. With small chunks part 0
     * always ends inside the header, which cannot observe a bug in where the header slice is placed.
     */
    public void testRangedUploadStreamTilesBlobExactly() throws Exception {
        final long primaryTerm = 1;
        try (var fakeNode = createFakeNode(primaryTerm)) {
            var vbcc = frozenVbccWithCommits(fakeNode, primaryTerm);
            try {
                final byte[] expected = wholeUploadBlob(vbcc);
                assertThat((long) expected.length, equalTo(vbcc.getTotalSizeInBytes()));
                assertOpensWithCommitHeader(expected);

                for (long partSize : uploadPartSizesToExercise(expected.length)) {
                    var reassembled = new byte[expected.length];
                    int written = 0;
                    for (long[] range : multiPartRanges(expected.length, partSize)) {
                        byte[] part = readUploadPart(vbcc, range[0], range[1]);
                        assertThat(
                            "part at offset " + range[0] + " has wrong length for partSize " + partSize,
                            (long) part.length,
                            equalTo(range[1])
                        );
                        System.arraycopy(part, 0, reassembled, written, part.length);
                        written += part.length;
                    }
                    assertThat(written, equalTo(expected.length));
                    assertOpensWithCommitHeader(reassembled);
                    assertArrayEquals("tiling mismatch at partSize " + partSize, expected, reassembled);
                }
            } finally {
                vbcc.close();
            }
        }
    }

    /**
     * When staging a part times out, the provided input stream is closed without having been read to EOF (see the
     * cancellation handlers in {@code AzureBlobStore#stageBlock}). {@code RetryableAction} then re-runs the whole
     * upload, asking the provider for the same ranges again.
     * <p>
     * Re-reading a range after an abandoned read must yield identical bytes. The corruption this guards against
     * produced a part 0 holding {@code source[headerSize .. partSize + headerSize)} rather than
     * {@code source[0 .. partSize)}: the commit header absent and the payload shifted forward into its place, while
     * later parts stayed correctly positioned. Total blob length was unchanged, so neither a length nor a footer
     * check detected it.
     */
    public void testUploadPartIsIdenticalWhenReReadAfterAbandonedUpload() throws Exception {
        final long primaryTerm = 1;
        try (var fakeNode = createFakeNode(primaryTerm)) {
            var vbcc = frozenVbccWithCommits(fakeNode, primaryTerm);
            try {
                final byte[] expected = wholeUploadBlob(vbcc);

                // Sweep part sizes so that part 0 ends inside the commit header in some cases and beyond it in
                // others. The production failure had a 100MB part 0 ending far past the header; a part that stops
                // inside the header cannot observe a misplaced header at all.
                for (long partSize : uploadPartSizesToExercise(expected.length)) {
                    final var ranges = multiPartRanges(expected.length, partSize);
                    final long firstPartLength = ranges.get(0)[1];

                    // Abandon part 0 part-way through, one or more times, as cancelled stage attempts do.
                    for (int attempt = 0; attempt < randomIntBetween(1, 3); attempt++) {
                        try (var stream = vbcc.getFrozenInputStreamForUpload(0, firstPartLength)) {
                            var discarded = new byte[Math.toIntExact(Math.max(1L, firstPartLength / 3L))];
                            Streams.readFully(stream, discarded, 0, discarded.length);
                            // closed here without reaching EOF
                        }
                    }

                    // Retry the upload in full; every part must match the pristine blob.
                    var reassembled = new byte[expected.length];
                    int written = 0;
                    for (long[] range : ranges) {
                        byte[] part = readUploadPart(vbcc, range[0], range[1]);
                        System.arraycopy(part, 0, reassembled, written, part.length);
                        written += part.length;
                    }
                    assertThat(written, equalTo(expected.length));

                    // Check the header first: it gives a far clearer failure than a whole-array diff.
                    assertOpensWithCommitHeader(reassembled);
                    assertArrayEquals(
                        "retried part 0 differs from the original upload at partSize " + partSize,
                        Arrays.copyOfRange(expected, 0, Math.toIntExact(firstPartLength)),
                        Arrays.copyOfRange(reassembled, 0, Math.toIntExact(firstPartLength))
                    );
                    assertArrayEquals("retried blob differs at partSize " + partSize, expected, reassembled);

                    try (BytesStreamOutput roundTrip = new BytesStreamOutput()) {
                        roundTrip.writeBytes(reassembled);
                        assertEquals(
                            vbcc.getFrozenBatchedCompoundCommit(),
                            deserializeBatchedCompoundCommit(vbcc.getBlobName(), roundTrip)
                        );
                    }
                }
            } finally {
                vbcc.close();
            }
        }
    }

    /**
     * Parts are staged concurrently, so several provider streams are open against one frozen VBCC at once. Reads all
     * parts in parallel, released together from a barrier, and requires exact tiling.
     */
    public void testConcurrentUploadPartReadsDoNotInterfere() throws Exception {
        final long primaryTerm = 1;
        try (var fakeNode = createFakeNode(primaryTerm)) {
            var vbcc = frozenVbccWithCommits(fakeNode, primaryTerm);
            try {
                final byte[] expected = wholeUploadBlob(vbcc);
                final long partSize = Math.max(4096L, expected.length / randomIntBetween(3, 6));
                final var ranges = multiPartRanges(expected.length, partSize);

                final var parts = new byte[ranges.size()][];
                final var barrier = new CyclicBarrier(ranges.size());
                final var done = new CountDownLatch(ranges.size());
                final var failures = new ArrayList<Throwable>();

                for (int i = 0; i < ranges.size(); i++) {
                    final int index = i;
                    new Thread(() -> {
                        try {
                            barrier.await(30, TimeUnit.SECONDS);
                            parts[index] = readUploadPart(vbcc, ranges.get(index)[0], ranges.get(index)[1]);
                        } catch (Throwable t) {
                            synchronized (failures) {
                                failures.add(t);
                            }
                        } finally {
                            done.countDown();
                        }
                    }, "vbcc-upload-part-" + index).start();
                }
                assertTrue("concurrent part reads timed out", done.await(60, TimeUnit.SECONDS));
                synchronized (failures) {
                    assertThat("concurrent part reads threw " + failures, failures, empty());
                }

                var reassembled = new byte[expected.length];
                int written = 0;
                for (byte[] part : parts) {
                    System.arraycopy(part, 0, reassembled, written, part.length);
                    written += part.length;
                }
                assertOpensWithCommitHeader(reassembled);
                assertArrayEquals(expected, reassembled);
            } finally {
                vbcc.close();
            }
        }
    }

    /**
     * {@code InternalHeaderReader} measures its header size once, in its constructor, but re-serializes the header on
     * every read. The blob layout is derived from the measured value while the uploaded bytes come from the
     * re-serialization, so the two disagreeing would shift everything that follows the header.
     */
    public void testUploadHeaderRematerializesAtStableLength() throws Exception {
        final long primaryTerm = 1;
        try (var fakeNode = createFakeNode(primaryTerm)) {
            var vbcc = frozenVbccWithCommits(fakeNode, primaryTerm);
            try {
                final byte[] expected = wholeUploadBlob(vbcc);
                for (int i = 0; i < 5; i++) {
                    assertArrayEquals("whole-blob read " + i + " differs", expected, wholeUploadBlob(vbcc));
                }
                final long prefix = Math.min(expected.length, 4096L);
                final byte[] first = readUploadPart(vbcc, 0, prefix);
                for (int i = 0; i < 5; i++) {
                    assertArrayEquals("header prefix read " + i + " differs", first, readUploadPart(vbcc, 0, prefix));
                }
                assertArrayEquals(Arrays.copyOfRange(expected, 0, Math.toIntExact(prefix)), first);
            } finally {
                vbcc.close();
            }
        }
    }

    private VirtualBatchedCompoundCommit frozenVbccWithCommits(FakeStatelessNode fakeNode, long primaryTerm) throws Exception {
        var commits = fakeNode.generateIndexCommits(randomIntBetween(2, 4));
        var vbcc = new VirtualBatchedCompoundCommit(
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
        for (StatelessCommitRef statelessCommitRef : commits) {
            assertTrue(vbcc.appendCommit(statelessCommitRef, randomBoolean(), null));
        }
        vbcc.freeze();
        return vbcc;
    }

    /** Reads the whole blob through the non-ranged upload stream, the ground truth for the ranged reads. */
    private static byte[] wholeUploadBlob(VirtualBatchedCompoundCommit vbcc) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            try (var stream = vbcc.getFrozenInputStreamForUpload()) {
                Streams.copy(stream, output, false);
            }
            return BytesReference.toBytes(output.bytes());
        }
    }

    /** Reads one multipart range through the provider the concurrent multipart upload path uses. */
    private static byte[] readUploadPart(VirtualBatchedCompoundCommit vbcc, long offset, long length) throws IOException {
        var buffer = new byte[Math.toIntExact(length)];
        try (var stream = vbcc.getFrozenInputStreamForUpload(offset, length)) {
            Streams.readFully(stream, buffer, 0, buffer.length);
        }
        return buffer;
    }

    /** A batched compound commit blob must open with the stateless commit codec header. */
    private static void assertOpensWithCommitHeader(byte[] blob) throws IOException {
        CodecUtil.checkHeader(
            new BytesReferenceIndexInput("blob", new BytesArray(blob)),
            StatelessCompoundCommit.SHARD_COMMIT_CODEC,
            StatelessCompoundCommit.VERSION_WITH_COMMIT_FILES,
            StatelessCompoundCommit.CURRENT_VERSION
        );
    }

    /**
     * Part sizes worth exercising for a blob of the given size. Deliberately spans sizes smaller than the compound
     * commit header and sizes larger than it, so part 0 ends inside the header in some runs and past it in others.
     */
    private static long[] uploadPartSizesToExercise(long totalSize) {
        var sizes = new ArrayList<Long>();
        for (long candidate : new long[] { 512L, 4096L, 16L * 1024L, 64L * 1024L, totalSize / 4L, totalSize / 2L + 1L, totalSize }) {
            if (candidate > 0 && candidate <= totalSize && sizes.contains(candidate) == false) {
                sizes.add(candidate);
            }
        }
        assertThat(sizes, not(empty()));
        return sizes.stream().mapToLong(Long::longValue).toArray();
    }

    /** Part boundaries as {@code AzureBlobStore} computes them: equal parts with the remainder in the last. */
    private static List<long[]> multiPartRanges(long totalSize, long partSize) {
        var ranges = new ArrayList<long[]>();
        for (long offset = 0; offset < totalSize; offset += partSize) {
            ranges.add(new long[] { offset, Math.min(partSize, totalSize - offset) });
        }
        return ranges;
    }

}
