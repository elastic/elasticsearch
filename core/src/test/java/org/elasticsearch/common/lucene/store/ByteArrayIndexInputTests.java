/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.lucene.store;

import java.nio.charset.StandardCharsets;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class ByteArrayIndexInputTests extends ESTestCase {

    @Test
    public void testRandomReads() throws IOException {
        for (int i = 0; i < 100; i++) {
            byte[] input = randomUnicodeOfLength(randomIntBetween(1, 1000)).getBytes(StandardCharsets.UTF_8);
            ByteArrayIndexInput indexInput = new ByteArrayIndexInput("test", input);
            assertEquals(input.length, indexInput.length());
            assertEquals(0, indexInput.getFilePointer());
            byte[] output = randomReadAndSlice(indexInput, input.length);
            assertArrayEquals(input, output);
        }
    }

    @Test
    public void testRandomOverflow() throws IOException {
        for (int i = 0; i < 100; i++) {
            byte[] input = randomUnicodeOfLength(randomIntBetween(1, 1000)).getBytes(StandardCharsets.UTF_8);
            ByteArrayIndexInput indexInput = new ByteArrayIndexInput("test", input);
            int firstReadLen = randomIntBetween(0, input.length - 1);
            randomReadAndSlice(indexInput, firstReadLen);
            int bytesLeft = input.length - firstReadLen;
            try {
                // read using int size
                int secondReadLen = bytesLeft + randomIntBetween(1, 100);
                indexInput.readBytes(new byte[secondReadLen], 0, secondReadLen);
                fail();
            } catch (IOException ex) {
                assertThat(ex.getMessage(), containsString("EOF"));
            }
        }
    }

    @Test
    public void testSeekOverflow() throws IOException {
        for (int i = 0; i < 100; i++) {
            byte[] input = randomUnicodeOfLength(randomIntBetween(1, 1000)).getBytes(StandardCharsets.UTF_8);
            ByteArrayIndexInput indexInput = new ByteArrayIndexInput("test", input);
            int firstReadLen = randomIntBetween(0, input.length - 1);
            randomReadAndSlice(indexInput, firstReadLen);
            try {
                switch (randomIntBetween(0, 2)) {
                    case 0:
                        indexInput.seek(Integer.MAX_VALUE + 4L);
                        break;
                    case 1:
                        indexInput.seek(-randomIntBetween(1, 10));
                        break;
                    case 2:
                        int seek = input.length + randomIntBetween(1, 100);
                        indexInput.seek(seek);
                        break;
                    default:
                        fail();
                }
                fail();
            } catch (IOException ex) {
                assertThat(ex.getMessage(), containsString("EOF"));
            } catch (IllegalArgumentException ex) {
                assertThat(ex.getMessage(), containsString("negative position"));
            }
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
            assertEquals((long) readPos, indexInput.getFilePointer());
        }
        return output;
    }
}

