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

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.security.SecureRandom;

/**
 * @author kimchy (shay.banon)
 */
public class LZFOutputStreamTests {

    private static int BUFFER_SIZE = LZFChunk.MAX_CHUNK_LEN * 64;
    private byte[] nonEncodableBytesToWrite = new byte[BUFFER_SIZE];
    private byte[] bytesToWrite = new byte[BUFFER_SIZE];

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
    }

    @Test
    public void testUnencodable() throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        OutputStream os = new LZFOutputStream(bos);
        os.write(nonEncodableBytesToWrite);
        os.close();
        Assert.assertTrue(bos.toByteArray().length > nonEncodableBytesToWrite.length);
    }

    @Test
    public void testStreaming() throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        OutputStream os = new LZFOutputStream(bos);
        os.write(bytesToWrite);
        os.close();
        Assert.assertTrue(bos.toByteArray().length > 10);
        Assert.assertTrue(bos.toByteArray().length < bytesToWrite.length * .5);
    }

    @Test
    public void testSingleByte() throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        OutputStream os = new LZFOutputStream(bos);
        for (int idx = 0; idx < BUFFER_SIZE; idx++) {
            os.write(bytesToWrite[idx]);
            if (idx % 1023 == 0) {
                os.flush();
            }
        }
        os.close();
        Assert.assertTrue(bos.toByteArray().length > 10);
        Assert.assertTrue(bos.toByteArray().length < bytesToWrite.length * .5);
    }

    @Test
    public void testPartialBuffer() throws Exception {
        int offset = 255;
        int len = 1 << 17;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        OutputStream os = new LZFOutputStream(bos);
        os.write(bytesToWrite, offset, len);
        os.close();
        Assert.assertTrue(bos.toByteArray().length > 10);
        Assert.assertTrue(bos.toByteArray().length < bytesToWrite.length * .5);
    }
}
