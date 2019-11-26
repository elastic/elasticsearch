/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.store;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.test.ESTestCase;

import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class SearchableSnapshotIndexInputTests extends ESTestCase {

    private SearchableSnapshotIndexInput createIndexInput(final byte[] input) {
        StoreFileMetaData metadata = new StoreFileMetaData("test", (long) input.length, "_checksum", Version.LATEST);
        long partSize = (long) (randomBoolean() ? input.length : randomIntBetween(1, input.length));
        FileInfo fileInfo = new FileInfo(randomAlphaOfLength(5), metadata, new ByteSizeValue(partSize, ByteSizeUnit.BYTES));

        BlobBytesReader reader = (name, from, len, buffer, offset) -> {
            if (fileInfo.numberOfParts() == 1L) {
                if (name.equals(fileInfo.name()) == false || name.contains(".part")) {
                    throw new IOException("Unexpected part name " + name);
                }
                System.arraycopy(input, Math.toIntExact(from), buffer, offset, len);
            } else {
                if (name.startsWith(fileInfo.name()) == false || name.contains(".part") == false) {
                    throw new IOException("Unexpected part name " + name);
                }
                long partNumber = Long.parseLong(name.substring(name.indexOf(".part") + ".part".length()));
                if (partNumber < 0 || partNumber >= fileInfo.numberOfParts()) {
                    throw new IOException("Unexpected part number " + name);
                }
                System.arraycopy(input, Math.toIntExact(partNumber * partSize + from), buffer, offset, len);
            }
        };
        return new SearchableSnapshotIndexInput(reader, fileInfo);
    }

    public void testRandomReads() throws IOException {
        for (int i = 0; i < 100; i++) {
            byte[] input = randomUnicodeOfLength(randomIntBetween(1, 1000)).getBytes(StandardCharsets.UTF_8);
            SearchableSnapshotIndexInput indexInput = createIndexInput(input);
            assertEquals(input.length, indexInput.length());
            assertEquals(0, indexInput.getFilePointer());
            byte[] output = randomReadAndSlice(indexInput, input.length);
            assertArrayEquals(input, output);
        }
    }

    public void testRandomOverflow() throws IOException {
        for (int i = 0; i < 100; i++) {
            byte[] input = randomUnicodeOfLength(randomIntBetween(1, 1000)).getBytes(StandardCharsets.UTF_8);
            SearchableSnapshotIndexInput indexInput = createIndexInput(input);
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
            SearchableSnapshotIndexInput indexInput = createIndexInput(input);
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
            switch (randomIntBetween(0, 3)) {
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
                default:
                    fail();
            }
            assertEquals(readPos, indexInput.getFilePointer());
        }
        return output;
    }
}
