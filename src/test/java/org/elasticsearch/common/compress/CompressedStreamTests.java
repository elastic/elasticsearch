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

package org.elasticsearch.common.compress;

import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.test.ElasticsearchTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Random;

/**
 * Test streaming compression (e.g. used for recovery)
 */
public class CompressedStreamTests extends ElasticsearchTestCase {

    public void testRandom() throws IOException {
        Random r = getRandom();
        for (int i = 0; i < 100; i++) {
            byte bytes[] = new byte[TestUtil.nextInt(r, 1, 100000)];
            r.nextBytes(bytes);
            doTest("lzf", bytes);
        }
    }
    
    public void testLineDocs() throws IOException {
        Random r = getRandom();
        LineFileDocs lineFileDocs = new LineFileDocs(r);
        for (int i = 0; i < 100; i++) {
            int numDocs = TestUtil.nextInt(r, 1, 200);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            for (int j = 0; j < numDocs; j++) {
                String s = lineFileDocs.nextDoc().get("body");
                bos.write(s.getBytes(StandardCharsets.UTF_8));
            }
            doTest("lzf", bos.toByteArray());
        }
        lineFileDocs.close();
    }
    
    public void testRepetitionsL() throws IOException {
        Random r = getRandom();
        for (int i = 0; i < 200; i++) {
            int numLongs = TestUtil.nextInt(r, 1, 10000);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            long theValue = r.nextLong();
            for (int j = 0; j < numLongs; j++) {
                if (r.nextInt(10) == 0) {
                    theValue = r.nextLong();
                }
                bos.write((byte) (theValue >>> 56));
                bos.write((byte) (theValue >>> 48));
                bos.write((byte) (theValue >>> 40));
                bos.write((byte) (theValue >>> 32));
                bos.write((byte) (theValue >>> 24));
                bos.write((byte) (theValue >>> 16));
                bos.write((byte) (theValue >>> 8));
                bos.write((byte) theValue);
            }
            doTest("lzf", bos.toByteArray());
        }
    }
    
    public void testRepetitionsI() throws IOException {
        Random r = getRandom();
        for (int i = 0; i < 200; i++) {
            int numInts = TestUtil.nextInt(r, 1, 20000);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            int theValue = r.nextInt();
            for (int j = 0; j < numInts; j++) {
                if (r.nextInt(10) == 0) {
                    theValue = r.nextInt();
                }
                bos.write((byte) (theValue >>> 24));
                bos.write((byte) (theValue >>> 16));
                bos.write((byte) (theValue >>> 8));
                bos.write((byte) theValue);
            }
            doTest("lzf", bos.toByteArray());
        }
    }
    
    public void testRepetitionsS() throws IOException {
        Random r = getRandom();
        for (int i = 0; i < 200; i++) {
            int numShorts = TestUtil.nextInt(r, 1, 40000);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            short theValue = (short) r.nextInt(65535);
            for (int j = 0; j < numShorts; j++) {
                if (r.nextInt(10) == 0) {
                    theValue = (short) r.nextInt(65535);
                }
                bos.write((byte) (theValue >>> 8));
                bos.write((byte) theValue);
            }
            doTest("lzf", bos.toByteArray());
        }
    }
    
    private void doTest(String compressor, byte bytes[]) throws IOException {
        CompressorFactory.configure(ImmutableSettings.settingsBuilder().put("compress.default.type", compressor).build());           
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        StreamInput rawIn = new ByteBufferStreamInput(bb);
        Compressor c = CompressorFactory.defaultCompressor();
        
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        OutputStreamStreamOutput rawOs = new OutputStreamStreamOutput(bos);
        StreamOutput os = c.streamOutput(rawOs);
        
        Random r = getRandom();
        int bufferSize = r.nextBoolean() ? 65535 : TestUtil.nextInt(getRandom(), 1, 70000);
        byte buffer[] = new byte[bufferSize];
        int len;
        while ((len = rawIn.read(buffer)) != -1) {
            os.write(buffer, 0, len);
        }
        os.close();
        rawIn.close();
        
        // now we have compressed byte array
        
        byte compressed[] = bos.toByteArray();
        ByteBuffer bb2 = ByteBuffer.wrap(compressed);
        StreamInput compressedIn = new ByteBufferStreamInput(bb2);
        StreamInput in = c.streamInput(compressedIn);
        
        ByteArrayOutputStream uncompressedOut = new ByteArrayOutputStream();
        while ((len = in.read(buffer)) != -1) {
            uncompressedOut.write(buffer, 0, len);
        }
        uncompressedOut.close();
        
        assertArrayEquals(bytes, uncompressedOut.toByteArray());
    }
}
