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

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.fs.FsBlobContainer;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.SnapshotIndexCommit;
import org.elasticsearch.repositories.SnapshotShardContext;
import org.elasticsearch.snapshots.AbortedSnapshotException;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.commits.BlobFile;
import org.elasticsearch.xpack.stateless.commits.BlobLocation;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class StatelessSnapshotShardContextTests extends ESTestCase {

    public void testBlobStoreFileReader() throws IOException {
        final var shardId = new ShardId(new Index(randomIdentifier(), randomUUID()), between(0, 5));

        final var blobContainer = createBlobContainer();

        final var blobLength = between(500, 5000);
        final var bytes = randomByteArrayOfLength(blobLength);
        final long generation = 42L;
        final String blobName = "stateless_commit_" + generation;
        blobContainer.writeBlob(
            randomFrom(OperationPurpose.values()),
            blobName,
            new ByteArrayInputStream(bytes),
            blobLength,
            randomBoolean()
        );
        final var commitRefReleased = new AtomicBoolean();
        final var snapshotIndexCommit = new SnapshotIndexCommit(new Engine.IndexCommitRef(null, () -> commitRefReleased.set(true)));
        assertThat(snapshotIndexCommit.refCount(), equalTo(1));

        final var fileOffset = between(0, 50);
        final var fileLength = blobLength - 50;
        final List<CloseTrackingInputStream> allOpenedStreams = new ArrayList<>();

        // Sometimes abort the read halfway through
        final var abortPosition = between(0, 3) == 3 ? randomIntBetween(fileOffset, blobLength) : Integer.MAX_VALUE;
        final var snapshotShardContext = new StatelessSnapshotShardContext(
            shardId,
            new SnapshotId(randomIdentifier(), randomUUID()),
            new IndexId(shardId.getIndexName(), randomUUID()),
            randomIdentifier(),
            IndexShardSnapshotStatus.newInitializing(ShardGeneration.newGeneration(), randomLongBetween(1, Long.MAX_VALUE)),
            IndexVersion.current(),
            randomNonNegativeLong(),
            snapshotIndexCommit,
            new Store.MetadataSnapshot(Map.of(), Map.of(), randomNonNegativeLong()),
            Map.of("file", new BlobLocation(new BlobFile(blobName, new PrimaryTermAndGeneration(1L, generation)), fileOffset, fileLength)),
            (s, g) -> new FilterBlobContainer(blobContainer) {
                @Override
                protected BlobContainer wrapChild(BlobContainer child) {
                    throw new AssertionError("should not obtain child");
                }

                @Override
                public InputStream readBlob(OperationPurpose purpose, String blobName, long position, long length) throws IOException {
                    assertThat(purpose, equalTo(OperationPurpose.SNAPSHOT_DATA));
                    final var inputStream = new CloseTrackingInputStream(
                        super.readBlob(purpose, blobName, position, length),
                        position >= abortPosition
                    );
                    allOpenedStreams.add(inputStream);
                    return inputStream;
                }
            },
            new PlainActionFuture<>()
        );

        final var closeEachInputStream = rarely();
        final var numberOfParts = between(0, 10);
        try (var fileReader = snapshotShardContext.fileReader("file", mock(StoreFileMetadata.class))) {
            assertThat(snapshotIndexCommit.refCount(), equalTo(2));
            if (numberOfParts != 0) {
                final var fileContentRead = readBlobContent(fileReader, numberOfParts, fileLength, closeEachInputStream);
                if (abortPosition == Integer.MAX_VALUE) { // when not aborted
                    assertThat(fileContentRead.length, equalTo(fileLength));
                }
                assertArrayEquals(Arrays.copyOfRange(bytes, fileOffset, fileOffset + fileContentRead.length), fileContentRead);
            }
        }
        assertFalse(commitRefReleased.get());
        assertThat(snapshotIndexCommit.refCount(), equalTo(1));

        for (CloseTrackingInputStream inputStream : allOpenedStreams) {
            assertTrue("Expected all opened input streams to be closed", inputStream.isClosed());
        }
    }

    public void testFileReaderThrowsWhenSnapshotAborted() throws IOException {
        final var commitRefReleased = new AtomicBoolean();
        final var snapshotIndexCommit = new SnapshotIndexCommit(new Engine.IndexCommitRef(null, () -> commitRefReleased.set(true)));
        final var snapshotStatus = IndexShardSnapshotStatus.newInitializing(ShardGeneration.newGeneration(), randomNonNegativeLong());
        snapshotStatus.addAbortListener(new ActionListener<>() {
            @Override
            public void onResponse(IndexShardSnapshotStatus.AbortStatus abortStatus) {
                assertThat(abortStatus, is(IndexShardSnapshotStatus.AbortStatus.ABORTED));
                snapshotIndexCommit.onAbort();
            }

            @Override
            public void onFailure(Exception e) {
                fail(e);
            }
        });
        snapshotStatus.abortIfNotCompleted("test abort", listener -> listener.onResponse(null));
        assertTrue(commitRefReleased.get());

        final var shardId = new ShardId(new Index(randomIndexName(), randomUUID()), between(0, 5));
        final long generation = 42L;
        final String blobName = "stateless_commit_" + generation;
        final var context = new StatelessSnapshotShardContext(
            shardId,
            new SnapshotId(randomSnapshotName(), randomUUID()),
            new IndexId(shardId.getIndexName(), randomUUID()),
            randomIdentifier(),
            snapshotStatus,
            IndexVersion.current(),
            randomNonNegativeLong(),
            snapshotIndexCommit,
            new Store.MetadataSnapshot(Map.of(), Map.of(), randomNonNegativeLong()),
            Map.of("file", new BlobLocation(new BlobFile(blobName, new PrimaryTermAndGeneration(1L, generation)), 0, 100)),
            (s, g) -> mock(BlobContainer.class),
            new PlainActionFuture<>()
        );

        expectThrows(AbortedSnapshotException.class, () -> context.fileReader("file", mock(StoreFileMetadata.class)));
    }

    public void testVerify() throws IOException {
        final Directory dir = newDirectory();
        final IndexOutput output = dir.createOutput("foo.bar", IOContext.DEFAULT);
        final int iters = scaledRandomIntBetween(10, 100);
        long totalLength = 16; // header + footer
        for (int i = 0; i < iters; i++) {
            BytesRef bytesRef = new BytesRef(TestUtil.randomRealisticUnicodeString(random(), 10, 1024));
            output.writeBytes(bytesRef.bytes, bytesRef.offset, bytesRef.length);
            totalLength += bytesRef.length;
        }
        CodecUtil.writeFooter(output);
        output.close();

        final long generation = randomLongBetween(1, 42);
        final String blobName = "stateless_commit_" + generation;
        final long badGeneration = randomLongBetween(50, 100);
        final String badBlobName = "stateless_commit_" + badGeneration;
        final FsBlobContainer blobContainer = createBlobContainer();

        try (
            var indexInput = dir.openInput("foo.bar", IOContext.DEFAULT);
            var indexInputStream = new InputStreamIndexInput(indexInput, totalLength)
        ) {
            assertThat(indexInput.length(), equalTo(totalLength));
            blobContainer.writeBlob(randomFrom(OperationPurpose.values()), blobName, indexInputStream, totalLength, randomBoolean());
        } finally {
            IOUtils.close(dir);
        }

        // Write a bad blob with a single bit flipped
        try (var in = blobContainer.readBlob(randomFrom(OperationPurpose.values()), blobName, 0, totalLength)) {
            final var content = in.readAllBytes();
            final int brokenPosition = randomInt(content.length - 1);
            content[brokenPosition] = (byte) (content[brokenPosition] ^ (byte) (1 << between(0, 7)));
            blobContainer.writeBlob(
                randomFrom(OperationPurpose.values()),
                badBlobName,
                new ByteArrayInputStream(content),
                totalLength,
                randomBoolean()
            );
        }

        final var shardId = new ShardId(new Index(randomIdentifier(), randomUUID()), between(0, 5));
        final var snapshotShardContext = new StatelessSnapshotShardContext(
            shardId,
            new SnapshotId(randomIdentifier(), randomUUID()),
            new IndexId(shardId.getIndexName(), randomUUID()),
            randomIdentifier(),
            IndexShardSnapshotStatus.newInitializing(ShardGeneration.newGeneration(), randomLongBetween(1, Long.MAX_VALUE)),
            IndexVersion.current(),
            randomNonNegativeLong(),
            null,
            new Store.MetadataSnapshot(Map.of(), Map.of(), randomNonNegativeLong()),
            Map.of(
                "file",
                new BlobLocation(new BlobFile(blobName, new PrimaryTermAndGeneration(1L, generation)), 0, totalLength),
                "bad_file",
                new BlobLocation(new BlobFile(badBlobName, new PrimaryTermAndGeneration(1L, badGeneration)), 0, totalLength)
            ),
            (s, g) -> blobContainer,
            new PlainActionFuture<>()
        );

        final var numberOfParts = between(1, 10);
        try (var fileReader = snapshotShardContext.fileReader("file", mock(StoreFileMetadata.class))) {
            readBlobContent(fileReader, numberOfParts, (int) totalLength, randomBoolean());
            fileReader.verify();
        }

        try (var fileReader = snapshotShardContext.fileReader("bad_file", mock(StoreFileMetadata.class))) {
            readBlobContent(fileReader, numberOfParts, (int) totalLength, randomBoolean());
            expectThrows(CorruptIndexException.class, fileReader::verify);
        }
    }

    public void testResetWithoutMark() throws IOException {
        final var setup = createMarkResetTestSetup();
        try (var fileReader = setup.context.fileReader("file", mock(StoreFileMetadata.class))) {
            final var stream = fileReader.openInput(setup.fileLength);
            assertTrue(stream.markSupported());

            final int firstReadSize = between(1, setup.fileLength - 1);
            byte[] firstBytes = stream.readNBytes(firstReadSize);
            assertArrayEquals(Arrays.copyOfRange(setup.expectedContent, 0, firstReadSize), firstBytes);

            stream.reset();

            byte[] allBytes = readMixedBytes(stream, setup.fileLength);
            assertArrayEquals(setup.expectedContent, allBytes);

            fileReader.verify();
        }

        for (CloseTrackingInputStream s : setup.allOpenedStreams) {
            assertTrue("Expected all opened input streams to be closed", s.isClosed());
        }
    }

    public void testMarkAndReset() throws IOException {
        final var setup = createMarkResetTestSetup();
        try (var fileReader = setup.context.fileReader("file", mock(StoreFileMetadata.class))) {
            final var stream = fileReader.openInput(setup.fileLength);

            final int markPosition = between(1, setup.fileLength - 2);
            stream.readNBytes(markPosition);
            // Randomize the readLimit argument since is ignored by our implementation, i.e. reset is always allowed regardless of
            // how many bytes have been read after mark.
            stream.mark(randomInt());

            final int extraRead = between(1, setup.fileLength - markPosition);
            stream.readNBytes(extraRead);

            stream.reset();

            byte[] remaining = readMixedBytes(stream, setup.fileLength - markPosition);
            assertArrayEquals(Arrays.copyOfRange(setup.expectedContent, markPosition, setup.fileLength), remaining);

            fileReader.verify();
        }

        for (CloseTrackingInputStream s : setup.allOpenedStreams) {
            assertTrue("Expected all opened input streams to be closed", s.isClosed());
        }
    }

    public void testMultipleResets() throws IOException {
        final var setup = createMarkResetTestSetup();
        try (var fileReader = setup.context.fileReader("file", mock(StoreFileMetadata.class))) {
            final var stream = fileReader.openInput(setup.fileLength);

            int markPos = 0;
            final int rounds = between(2, 10);
            for (int i = 0; i < rounds; i++) {
                final int remaining = setup.fileLength - markPos;
                if (remaining <= 1) {
                    break;
                }
                final int bytesToRead = between(1, remaining - 1);
                readMixedBytes(stream, bytesToRead);

                if (randomBoolean()) {
                    final int newMarkPos = between(markPos, markPos + bytesToRead);
                    stream.reset();
                    readMixedBytes(stream, newMarkPos - markPos);
                    // Randomize the readLimit argument since is ignored by our implementation, i.e. reset is always allowed regardless of
                    // how many bytes have been read after mark.
                    stream.mark(randomInt());
                    markPos = newMarkPos;
                } else {
                    stream.reset();
                }
            }

            byte[] content = readMixedBytes(stream, setup.fileLength - markPos);
            assertArrayEquals(Arrays.copyOfRange(setup.expectedContent, markPos, setup.fileLength), content);

            fileReader.verify();
        }

        for (CloseTrackingInputStream s : setup.allOpenedStreams) {
            assertTrue("Expected all opened input streams to be closed", s.isClosed());
        }
    }

    private record MarkResetTestSetup(
        byte[] expectedContent,
        int fileLength,
        StatelessSnapshotShardContext context,
        List<CloseTrackingInputStream> allOpenedStreams
    ) {}

    private MarkResetTestSetup createMarkResetTestSetup() throws IOException {
        final Directory dir = newDirectory();
        final IndexOutput output = dir.createOutput("test.file", IOContext.DEFAULT);
        final byte[] content = randomByteArrayOfLength(between(100, 1000));
        output.writeBytes(content, content.length);
        CodecUtil.writeFooter(output);
        output.close();

        final long totalLength;
        try (var indexInput = dir.openInput("test.file", IOContext.DEFAULT)) {
            totalLength = indexInput.length();
        }

        final long generation = 42L;
        final String blobName = "stateless_commit_" + generation;
        final int blobPadding = between(0, 50);
        final FsBlobContainer blobContainer = createBlobContainer();

        final byte[] expectedContent;
        try (
            var indexInput = dir.openInput("test.file", IOContext.DEFAULT);
            var indexInputStream = new InputStreamIndexInput(indexInput, totalLength)
        ) {
            final byte[] fileContent = indexInputStream.readAllBytes();
            // Add padding bytes randomly so that the file is sometimes in the middle of a blob
            final byte[] padding = randomByteArrayOfLength(blobPadding);
            final int blobTrailing = between(0, 50);
            final byte[] trailing = randomByteArrayOfLength(blobTrailing);
            final byte[] blobContent = new byte[blobPadding + fileContent.length + blobTrailing];
            System.arraycopy(padding, 0, blobContent, 0, blobPadding);
            System.arraycopy(fileContent, 0, blobContent, blobPadding, fileContent.length);
            System.arraycopy(trailing, 0, blobContent, blobPadding + fileContent.length, blobTrailing);
            blobContainer.writeBlob(
                OperationPurpose.SNAPSHOT_DATA,
                blobName,
                new ByteArrayInputStream(blobContent),
                blobContent.length,
                false
            );
            expectedContent = fileContent;
        } finally {
            IOUtils.close(dir);
        }

        final var shardId = new ShardId(new Index(randomIdentifier(), randomUUID()), 0);
        final List<CloseTrackingInputStream> allOpenedStreams = new ArrayList<>();
        final var context = new StatelessSnapshotShardContext(
            shardId,
            new SnapshotId(randomIdentifier(), randomUUID()),
            new IndexId(shardId.getIndexName(), randomUUID()),
            randomIdentifier(),
            IndexShardSnapshotStatus.newInitializing(ShardGeneration.newGeneration(), randomLongBetween(1, Long.MAX_VALUE)),
            IndexVersion.current(),
            randomNonNegativeLong(),
            null,
            new Store.MetadataSnapshot(Map.of(), Map.of(), randomNonNegativeLong()),
            Map.of(
                "file",
                new BlobLocation(new BlobFile(blobName, new PrimaryTermAndGeneration(1L, generation)), blobPadding, totalLength)
            ),
            (s, g) -> new FilterBlobContainer(blobContainer) {
                @Override
                protected BlobContainer wrapChild(BlobContainer child) {
                    throw new AssertionError("should not obtain child");
                }

                @Override
                public InputStream readBlob(OperationPurpose purpose, String blobName, long position, long length) throws IOException {
                    var inputStream = new CloseTrackingInputStream(super.readBlob(purpose, blobName, position, length), false);
                    allOpenedStreams.add(inputStream);
                    return inputStream;
                }
            },
            new PlainActionFuture<>()
        );

        return new MarkResetTestSetup(expectedContent, (int) totalLength, context, allOpenedStreams);
    }

    private static byte[] readMixedBytes(InputStream stream, int count) throws IOException {
        final byte[] result = new byte[count];
        int pos = 0;
        while (pos < count) {
            if (randomBoolean()) {
                final int b = stream.read();
                assertNotEquals("unexpected end of stream at position " + pos, -1, b);
                result[pos++] = (byte) b;
            } else {
                final int len = between(1, count - pos);
                final int n = stream.read(result, pos, len);
                assertNotEquals("unexpected end of stream at position " + pos, -1, n);
                pos += n;
            }
        }
        return result;
    }

    private static byte[] readBlobContent(
        SnapshotShardContext.FileReader fileReader,
        int numberOfParts,
        int fileLength,
        boolean closeEachInputStream
    ) throws IOException {
        final var fileContentRead = new byte[fileLength];
        final var partLength = fileLength / numberOfParts;
        int bytesRead = 0;
        try {
            for (int i = 0; i < numberOfParts; i++) {
                var limit = partLength;
                if (i == numberOfParts - 1) {
                    limit = Math.max(limit, fileLength - bytesRead);
                }
                final var inputStream = fileReader.openInput(limit);
                inputStream.readNBytes(fileContentRead, bytesRead, limit);
                bytesRead += limit;
                if (closeEachInputStream) {
                    inputStream.close();
                }
            }
        } catch (AbortedSnapshotException e) {
            // Sometimes the read is aborted halfway through, return what have been read so far
            return Arrays.copyOfRange(fileContentRead, 0, bytesRead);
        }
        return fileContentRead;
    }

    private static FsBlobContainer createBlobContainer() throws IOException {
        final var blobStorePath = PathUtils.get(createTempDir().toString());
        final var blobContainer = new FsBlobContainer(
            new FsBlobStore(randomIntBetween(1, 8) * 1024, blobStorePath, false),
            BlobPath.EMPTY,
            blobStorePath
        );
        return blobContainer;
    }

    static class CloseTrackingInputStream extends FilterInputStream {
        private final boolean shouldAbort;
        private boolean closed = false;

        CloseTrackingInputStream(InputStream in, boolean shouldAbort) {
            super(in);
            this.shouldAbort = shouldAbort;
        }

        @Override
        public int read() throws IOException {
            if (shouldAbort) {
                throw new AbortedSnapshotException();
            }
            return super.read();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (shouldAbort) {
                throw new AbortedSnapshotException();
            }
            return super.read(b, off, len);
        }

        @Override
        public void close() throws IOException {
            super.close();
            closed = true;
        }

        public boolean isClosed() {
            return closed;
        }
    }

}
