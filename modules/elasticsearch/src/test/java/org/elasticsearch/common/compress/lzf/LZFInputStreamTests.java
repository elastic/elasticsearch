/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.common.compress.lzf;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.*;
import java.security.SecureRandom;
import java.util.Random;

/**
 * @author kimchy (shay.banon)
 */
public class LZFInputStreamTests {

    private static int BUFFER_SIZE = LZFChunk.MAX_CHUNK_LEN * 64;
    private byte[] nonEncodableBytesToWrite = new byte[BUFFER_SIZE];
    private byte[] bytesToWrite = new byte[BUFFER_SIZE];
    private ByteArrayOutputStream nonCompressed;
    private ByteArrayOutputStream compressed;

    @BeforeClass
    public void setUp() throws Exception {
        SecureRandom.getInstance("SHA1PRNG").nextBytes(nonEncodableBytesToWrite);
        String phrase = "all work and no play make Jack a dull boy";
        byte[] bytes = phrase.getBytes();
        int cursor = 0;
        while (cursor <= bytesToWrite.length) {
            System.arraycopy(bytes, 0, bytesToWrite, cursor, (bytes.length + cursor < bytesToWrite.length) ? bytes.length : bytesToWrite.length - cursor);
            cursor += bytes.length;
        }
        nonCompressed = new ByteArrayOutputStream();
        OutputStream os = new LZFOutputStream(nonCompressed);
        os.write(nonEncodableBytesToWrite);
        os.close();

        compressed = new ByteArrayOutputStream();
        os = new LZFOutputStream(compressed);
        os.write(bytesToWrite);
        os.close();
    }

    @Test
    public void testDecompressNonEncodableReadByte() throws IOException {
        doDecompressReadBlock(nonCompressed.toByteArray(), nonEncodableBytesToWrite);
    }

    @Test
    public void testDecompressNonEncodableReadBlock() throws IOException {
        doDecompressReadBlock(nonCompressed.toByteArray(), nonEncodableBytesToWrite);
    }

    @Test
    public void testDecompressEncodableReadByte() throws IOException {
        doDecompressReadBlock(compressed.toByteArray(), bytesToWrite);
    }

    @Test
    public void testDecompressEncodableReadBlock() throws IOException {
        doDecompressReadBlock(compressed.toByteArray(), bytesToWrite);
    }

    public void doDecompressNonEncodableReadByte(byte[] bytes, byte[] reference) throws IOException {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        int outputBytes = 0;
        InputStream is = new LZFInputStream(bis);
        int val;
        while ((val = is.read()) != -1) {
            byte testVal = (byte) (val & 255);
            Assert.assertTrue(testVal == reference[outputBytes]);
            outputBytes++;
        }
        Assert.assertTrue(outputBytes == reference.length);
    }

    @Test void testIncremental() throws IOException {
        // first need to compress something...
        String[] words = new String[]{"what", "ever", "some", "other", "words", "too"};
        StringBuilder sb = new StringBuilder(258000);
        Random rnd = new Random(123);
        while (sb.length() < 256000) {
            int i = (rnd.nextInt() & 31);
            if (i < words.length) {
                sb.append(words[i]);
            } else {
                sb.append(i);
            }
        }
        byte[] uncomp = sb.toString().getBytes("UTF-8");
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        LZFOutputStream lzOut = new LZFOutputStream(bytes);
        lzOut.write(uncomp);
        lzOut.close();
        byte[] comp = bytes.toByteArray();

        // read back, in chunks
        bytes = new ByteArrayOutputStream(uncomp.length);
        byte[] buffer = new byte[64];
        LZFInputStream lzIn = new LZFInputStream(new ByteArrayInputStream(comp));

        while (true) {
            int len = 1 + ((rnd.nextInt() & 0x7FFFFFFF) % buffer.length);
            int offset = buffer.length - len;

            int count = lzIn.read(buffer, offset, len);
            if (count < 0) {
                break;
            }
            if (count > len) {
                Assert.fail("Requested " + len + " bytes (offset " + offset + ", array length " + buffer.length + "), got " + count);
            }
            bytes.write(buffer, offset, count);
        }
        byte[] result = bytes.toByteArray();
        Assert.assertEquals(result.length, uncomp.length);
        Assert.assertEquals(result, uncomp);
    }


    private void doDecompressReadBlock(byte[] bytes, byte[] reference) throws IOException {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        int outputBytes = 0;
        InputStream is = new LZFInputStream(bis);
        int val;
        byte[] buffer = new byte[65536 + 23];
        while ((val = is.read(buffer)) != -1) {
            for (int i = 0; i < val; i++) {
                byte testVal = buffer[i];
                Assert.assertTrue(testVal == reference[outputBytes]);
                outputBytes++;
            }
        }
        Assert.assertTrue(outputBytes == reference.length);
    }
}
