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
import java.util.Random;

/**
 * Test streaming compression (e.g. used for recovery)
 */
public class CompressedStreamTests extends ElasticsearchTestCase {

    public void testRandom() throws IOException {
        String compressor = "lzf";
        CompressorFactory.configure(ImmutableSettings.settingsBuilder().put("compress.default.type", compressor).build());
        Random r = getRandom();
        for (int i = 0; i < 2000; i++) {
            byte bytes[] = new byte[TestUtil.nextInt(r, 1, 10000)];
            r.nextBytes(bytes);
            
            ByteBuffer bb = ByteBuffer.wrap(bytes);
            StreamInput rawIn = new ByteBufferStreamInput(bb);
            Compressor c = CompressorFactory.defaultCompressor();
           
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            OutputStreamStreamOutput rawOs = new OutputStreamStreamOutput(bos);
            StreamOutput os = c.streamOutput(rawOs);
            
            byte buffer[] = new byte[1024]; //arbitrary
            int len;
            while ((len = rawIn.read(buffer)) != -1) {
              os.write(buffer, 0, len);
            }
            os.close();
            
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
}
