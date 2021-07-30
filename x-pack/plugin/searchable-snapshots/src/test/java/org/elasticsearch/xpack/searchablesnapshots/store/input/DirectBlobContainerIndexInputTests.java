/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.searchablesnapshots.store.input;

import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.lucene.store.ESIndexInputTestCase;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.xpack.searchablesnapshots.cache.blob.CachedBlob;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.ByteRange;
import org.elasticsearch.xpack.searchablesnapshots.store.IndexInputStats;
import org.elasticsearch.xpack.searchablesnapshots.store.SearchableSnapshotDirectory;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.searchablesnapshots.AbstractSearchableSnapshotsTestCase.randomChecksumBytes;
import static org.elasticsearch.xpack.searchablesnapshots.AbstractSearchableSnapshotsTestCase.randomIOContext;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsUtils.toIntBytes;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DirectBlobContainerIndexInputTests extends ESIndexInputTestCase {

    private DirectBlobContainerIndexInput createIndexInput(Tuple<String, byte[]> bytes) throws IOException {
        final byte[] input = bytes.v2();
        return createIndexInput(
            input,
            randomBoolean() ? input.length : randomIntBetween(1, input.length),
            randomIntBetween(1, 1000),
            bytes.v1(),
            () -> {}
        );
    }

    private DirectBlobContainerIndexInput createIndexInput(
        final byte[] input,
        long partSize,
        long minimumReadSize,
        String checksum,
        Runnable onReadBlob
    ) throws IOException {
        final String fileName = randomAlphaOfLength(5) + randomFileExtension();
        final FileInfo fileInfo = new FileInfo(
            randomAlphaOfLength(5),
            new StoreFileMetadata(fileName, input.length, checksum, Version.LATEST.toString()),
            partSize == input.length
                ? randomFrom(
                    new ByteSizeValue(partSize, ByteSizeUnit.BYTES),
                    new ByteSizeValue(randomLongBetween(partSize, Long.MAX_VALUE), ByteSizeUnit.BYTES),
                    ByteSizeValue.ZERO,
                    new ByteSizeValue(-1, ByteSizeUnit.BYTES),
                    null
                )
                : new ByteSizeValue(partSize, ByteSizeUnit.BYTES)
        );

        final BlobContainer blobContainer = mock(BlobContainer.class);
        when(blobContainer.readBlob(anyString(), anyLong(), anyInt())).thenAnswer(invocationOnMock -> {
            String name = (String) invocationOnMock.getArguments()[0];
            long position = (long) invocationOnMock.getArguments()[1];
            long length = (long) invocationOnMock.getArguments()[2];
            assertThat(
                "Reading [" + length + "] bytes from [" + name + "] at [" + position + "] exceeds part size [" + partSize + "]",
                position + length,
                lessThanOrEqualTo(partSize)
            );

            onReadBlob.run();

            final InputStream stream;
            if (fileInfo.numberOfParts() == 1) {
                assertThat("Unexpected blob name [" + name + "]", name, equalTo(fileInfo.name()));
                stream = new ByteArrayInputStream(input, toIntBytes(position), toIntBytes(length));

            } else {
                assertThat("Unexpected blob name [" + name + "]", name, allOf(startsWith(fileInfo.name()), containsString(".part")));

                int partNumber = Integer.parseInt(name.substring(name.indexOf(".part") + ".part".length()));
                assertThat(
                    "Unexpected part number [" + partNumber + "] for [" + name + "]",
                    partNumber,
                    allOf(greaterThanOrEqualTo(0), lessThan(fileInfo.numberOfParts()))
                );

                stream = new ByteArrayInputStream(input, toIntBytes(partNumber * partSize + position), toIntBytes(length));
            }

            if (randomBoolean()) {
                return stream;
            } else {
                // sometimes serve less bytes than expected, in agreement with InputStream{@link #read(byte[], int, int)} javadoc
                return new FilterInputStream(stream) {
                    @Override
                    public int read(byte[] b, int off, int len) throws IOException {
                        return super.read(b, off, randomIntBetween(1, len));
                    }
                };
            }
        });

        final SearchableSnapshotDirectory directory = mock(SearchableSnapshotDirectory.class);
        when(directory.getCachedBlob(anyString(), any(ByteRange.class))).thenReturn(CachedBlob.CACHE_NOT_READY);
        when(directory.blobContainer()).thenReturn(blobContainer);

        final DirectBlobContainerIndexInput indexInput = new DirectBlobContainerIndexInput(
            fileName,
            directory,
            fileInfo,
            randomIOContext(),
            new IndexInputStats(1L, fileInfo.length(), fileInfo.length(), fileInfo.length(), () -> 0L),
            minimumReadSize
        );
        assertEquals(input.length, indexInput.length());
        return indexInput;
    }

    public void testRandomReads() throws IOException {
        for (int i = 0; i < 100; i++) {
            final Tuple<String, byte[]> bytes = randomChecksumBytes(randomIntBetween(1, 1000));
            final byte[] input = bytes.v2();

            final DirectBlobContainerIndexInput indexInput = createIndexInput(bytes);
            assertEquals(input.length, indexInput.length());
            assertEquals(0, indexInput.getFilePointer());
            byte[] output = randomReadAndSlice(indexInput, input.length);
            assertArrayEquals(input, output);
        }
    }

    public void testRandomOverflow() throws IOException {
        for (int i = 0; i < 100; i++) {
            final Tuple<String, byte[]> bytes = randomChecksumBytes(randomIntBetween(1, 1000));
            final byte[] input = bytes.v2();

            final DirectBlobContainerIndexInput indexInput = createIndexInput(bytes);
            int firstReadLen = randomIntBetween(0, input.length - 1);
            randomReadAndSlice(indexInput, firstReadLen);
            int bytesLeft = input.length - firstReadLen;
            int secondReadLen = bytesLeft + randomIntBetween(1, 100);
            expectThrows(EOFException.class, () -> indexInput.readBytes(new byte[secondReadLen], 0, secondReadLen));
        }
    }

    public void testSeekOverflow() throws IOException {
        for (int i = 0; i < 100; i++) {
            final Tuple<String, byte[]> bytes = randomChecksumBytes(randomIntBetween(1, 1000));
            final byte[] input = bytes.v2();

            final DirectBlobContainerIndexInput indexInput = createIndexInput(bytes);
            int firstReadLen = randomIntBetween(0, input.length - 1);
            randomReadAndSlice(indexInput, firstReadLen);
            expectThrows(IOException.class, () -> {
                switch (randomIntBetween(0, 2)) {
                    case 0:
                        indexInput.seek(Integer.MAX_VALUE + 4L);
                        break;
                    case 1:
                        indexInput.seek(-randomIntBetween(1, 10));
                        break;
                    default:
                        int seek = input.length + randomIntBetween(1, 100);
                        indexInput.seek(seek);
                        break;
                }
            });
        }
    }

    public void testSequentialReadsShareInputStreamFromBlobStore() throws IOException {
        for (int i = 0; i < 100; i++) {
            final Tuple<String, byte[]> bytes = randomChecksumBytes(randomIntBetween(1, 1000));
            final byte[] input = bytes.v2();

            final int minimumReadSize = randomIntBetween(1, 1000);
            final int partSize = randomBoolean() ? input.length : randomIntBetween(1, input.length);
            final String checksum = bytes.v1();

            final AtomicInteger readBlobCount = new AtomicInteger();
            final BufferedIndexInput indexInput = createIndexInput(
                input,
                partSize,
                minimumReadSize,
                checksum,
                readBlobCount::incrementAndGet
            );

            assertEquals(input.length, indexInput.length());

            final int readStart = randomIntBetween(0, input.length);
            final int readEnd = randomIntBetween(readStart, input.length);
            final int readLen = readEnd - readStart;

            indexInput.seek(readStart);

            // Straightforward sequential reading from `indexInput` (no cloning, slicing or seeking)
            final byte[] output = new byte[readLen];
            int readPos = readStart;
            while (readPos < readEnd) {
                if (randomBoolean()) {
                    output[readPos++ - readStart] = indexInput.readByte();
                } else {
                    int len = randomIntBetween(1, readEnd - readPos);
                    indexInput.readBytes(output, readPos - readStart, len);
                    readPos += len;
                }
            }
            assertEquals(readEnd, readPos);
            assertEquals(readEnd, indexInput.getFilePointer());

            final byte[] expected = new byte[readLen];
            System.arraycopy(input, readStart, expected, 0, readLen);
            assertArrayEquals(expected, output);

            // compute the maximum expected number of ranges read from the blob store
            final int firstPart = readStart / partSize;
            final int bufferedEnd = readEnd + indexInput.getBufferSize() - 1;
            final int lastPart = (bufferedEnd - 1) / partSize; // may overshoot a part due to buffering but not due to readahead

            final int expectedRanges;
            if (firstPart == lastPart) {
                final int bufferedBytes = bufferedEnd - readStart;
                expectedRanges = (bufferedBytes + minimumReadSize - 1) / minimumReadSize; // ceil(bufferedBytes/minimumReadSize)
            } else {
                // read was split across parts; each part involves at least one range

                final int bytesInFirstPart = (firstPart + 1) * partSize - readStart;
                // ceil(bytesInFirstPart/minimumReadSize)
                final int rangesInFirstPart = (bytesInFirstPart + minimumReadSize - 1) / minimumReadSize;

                final int bytesInLastPart = bufferedEnd - lastPart * partSize;
                // ceil(bytesInLastPart/minimumReadSize)
                final int rangesInLastPart = (bytesInLastPart + minimumReadSize - 1) / minimumReadSize;

                // ceil(partSize/minimumReadSize);
                final int rangesInMiddleParts = (partSize + minimumReadSize - 1) / minimumReadSize;
                final int middlePartCount = lastPart - firstPart - 1;

                expectedRanges = rangesInFirstPart + rangesInLastPart + rangesInMiddleParts * middlePartCount;
            }

            assertThat(
                "data was read in ranges of no less than " + minimumReadSize + " where possible",
                readBlobCount.get(),
                lessThanOrEqualTo(expectedRanges)
            );
        }
    }

}
