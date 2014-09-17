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

package org.elasticsearch.common.compress.lzf;

import com.ning.compress.lzf.ChunkDecoder;
import com.ning.compress.lzf.ChunkEncoder;
import com.ning.compress.lzf.LZFChunk;
import com.ning.compress.lzf.util.ChunkDecoderFactory;
import com.ning.compress.lzf.util.ChunkEncoderFactory;
import org.elasticsearch.test.ElasticsearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

/**
 * Test an extremely rare corruption produced by the pure java impl of ChunkEncoder.
 */
public class CorruptedCompressorTests extends ElasticsearchTestCase {

    public void testCorruption() throws IOException {
        // this test generates a hash collision: [0,1,153,64] hashes the same as [1,153,64,64]
        // and then leverages the bug s/inPos/0/ to corrupt the array
        // the first array is used to insert a reference from this hash to offset 6
        // and then the hash table is reused and still thinks that there is such a hash at position 6
        // and at position 7, it finds a sequence with the same hash
        // so it inserts a buggy reference
        byte[] b1 = new byte[] {0,1,2,3,4,(byte)153,64,64,64,9,9,9,9,9,9,9,9,9,9};
        byte[] b2 = new byte[] {1,(byte)153,0,0,0,0,(byte)153,64,64,64,0,0,0,0,0,0,0,0,0,0,0,0,0,0};
        ChunkEncoder encoder = ChunkEncoderFactory.safeInstance();
        ChunkDecoder decoder = ChunkDecoderFactory.safeInstance();
        check(encoder, decoder, b1, 0, b1.length);
        final int off = 6;
        check(encoder, decoder, b2, off, b2.length - off);
    }

    private void check(ChunkEncoder encoder, ChunkDecoder decoder, byte[] bytes, int offset, int length) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        byte[] expected = new byte[length];
        byte[] buffer = new byte[LZFChunk.MAX_CHUNK_LEN];
        byte[] output = new byte[length];
        System.arraycopy(bytes, offset, expected, 0, length);
        encoder.encodeAndWriteChunk(bytes, offset, length, outputStream);
        System.out.println(Arrays.toString(Arrays.copyOf(outputStream.toByteArray(), 20)));
        InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        assertEquals(decoder.decodeChunk(inputStream, buffer, output), length);

        System.out.println(Arrays.toString(Arrays.copyOf(output, 20)));
        assertArrayEquals(expected, output);
    }
}
