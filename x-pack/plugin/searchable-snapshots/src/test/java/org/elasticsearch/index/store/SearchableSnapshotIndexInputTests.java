/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.store;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SearchableSnapshotIndexInputTests extends ESTestCase {

    private SearchableSnapshotIndexInput createIndexInput(final byte[] input) throws IOException {
        final long partSize = (long) (randomBoolean() ? input.length : randomIntBetween(1, input.length));
        final FileInfo fileInfo = new FileInfo(randomAlphaOfLength(5),
            new StoreFileMetaData("test", (long) input.length, "_checksum", Version.LATEST),
            new ByteSizeValue(partSize, ByteSizeUnit.BYTES));

        final BlobContainer blobContainer = mock(BlobContainer.class);
        when(blobContainer.readBlob(anyString(), anyLong(), anyInt()))
            .thenAnswer(invocationOnMock -> {
                String name = (String) invocationOnMock.getArguments()[0];
                long position = (long) invocationOnMock.getArguments()[1];
                int length = (int) invocationOnMock.getArguments()[2];
                assertThat("Reading [" + length + "] bytes from [" + name + "] at [" + position + "] exceeds part size [" + partSize + "]",
                    position + length, lessThanOrEqualTo(partSize));

                if (fileInfo.numberOfParts() == 1L) {
                    assertThat("Unexpected blob name [" + name + "]", name, equalTo(fileInfo.name()));
                    return new ByteArrayInputStream(input, Math.toIntExact(position), length);

                } else {
                    assertThat("Unexpected blob name [" + name + "]", name, allOf(startsWith(fileInfo.name()), containsString(".part")));

                    long partNumber = Long.parseLong(name.substring(name.indexOf(".part") + ".part".length()));
                    assertThat("Unexpected part number [" + partNumber + "] for [" + name + "]", partNumber,
                        allOf(greaterThanOrEqualTo(0L), lessThan(fileInfo.numberOfParts())));

                    return new ByteArrayInputStream(input, Math.toIntExact(partNumber * partSize + position), length);
                }
            });
        return new SearchableSnapshotIndexInput(blobContainer, fileInfo);
    }

    public void testRandomReads() throws IOException {
        for (int i = 0; i < 100; i++) {
            byte[] input = randomUnicodeOfLength(randomIntBetween(1, 1000)).getBytes(StandardCharsets.UTF_8);
            IndexInput indexInput = createIndexInput(input);
            assertEquals(input.length, indexInput.length());
            assertEquals(0, indexInput.getFilePointer());
            byte[] output = randomReadAndSlice(indexInput, input.length);
            assertArrayEquals(input, output);
        }
    }

    public void testRandomOverflow() throws IOException {
        for (int i = 0; i < 100; i++) {
            byte[] input = randomUnicodeOfLength(randomIntBetween(1, 1000)).getBytes(StandardCharsets.UTF_8);
            IndexInput indexInput = createIndexInput(input);
            int firstReadLen = randomIntBetween(0, input.length - 1);
            randomReadAndSlice(indexInput, firstReadLen);
            int bytesLeft = input.length - firstReadLen;
            int secondReadLen = bytesLeft + randomIntBetween(1, 100);
            expectThrows(EOFException.class, () -> indexInput.readBytes(new byte[secondReadLen], 0, secondReadLen));
        }
    }

    public void testSeekOverflow() throws IOException {
        for (int i = 0; i < 100; i++) {
            byte[] input = randomUnicodeOfLength(randomIntBetween(1, 1000)).getBytes(StandardCharsets.UTF_8);
            IndexInput indexInput = createIndexInput(input);
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

    private byte[] randomReadAndSlice(IndexInput indexInput, int length) throws IOException {
        int readPos = (int) indexInput.getFilePointer();
        byte[] output = new byte[length];
        while (readPos < length) {
            switch (randomIntBetween(0, 4)) {
                case 0:
                    // Read by one byte at a time
                    output[readPos++] = indexInput.readByte();
                    break;
                case 1:
                    // Read several bytes into target
                    int len = randomIntBetween(1, length - readPos);
                    indexInput.readBytes(output, readPos, len);
                    readPos += len;
                    break;
                case 2:
                    // Read several bytes into 0-offset target
                    len = randomIntBetween(1, length - readPos);
                    byte[] temp = new byte[len];
                    indexInput.readBytes(temp, 0, len);
                    System.arraycopy(temp, 0, output, readPos, len);
                    readPos += len;
                    break;
                case 3:
                    // Read using slice
                    len = randomIntBetween(1, length - readPos);
                    IndexInput slice = indexInput.slice("slice (" + readPos + ", " + len + ") of " + indexInput.toString(), readPos, len);
                    temp = randomReadAndSlice(slice, len);
                    // assert that position in the original input didn't change
                    assertEquals(readPos, indexInput.getFilePointer());
                    System.arraycopy(temp, 0, output, readPos, len);
                    readPos += len;
                    indexInput.seek(readPos);
                    assertEquals(readPos, indexInput.getFilePointer());
                    break;
                case 4:
                    // Seek at a random position and read a single byte,
                    // then seek back to original position
                    final int lastReadPos = readPos;
                    readPos = randomIntBetween(0, length - 1);
                    indexInput.seek(readPos);
                    assertEquals(readPos, indexInput.getFilePointer());
                    randomReadAndSlice(indexInput, 1);
                    readPos = lastReadPos;
                    indexInput.seek(readPos);
                    assertEquals(readPos, indexInput.getFilePointer());
                    break;
                default:
                    fail();
            }
            assertEquals(readPos, indexInput.getFilePointer());
        }
        return output;
    }
}
