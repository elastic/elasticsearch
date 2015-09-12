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

import com.ning.compress.BufferRecycler;
import com.ning.compress.lzf.ChunkDecoder;
import com.ning.compress.lzf.LZFChunk;
import org.elasticsearch.common.compress.CompressedStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 */
public class LZFCompressedStreamInput extends CompressedStreamInput {

    private final BufferRecycler recycler;

    private final ChunkDecoder decoder;

    // scratch area buffer
    private byte[] inputBuffer;

    public LZFCompressedStreamInput(StreamInput in, ChunkDecoder decoder) throws IOException {
        super(in);
        this.recycler = BufferRecycler.instance();
        this.decoder = decoder;

        this.uncompressed = recycler.allocDecodeBuffer(LZFChunk.MAX_CHUNK_LEN);
        this.inputBuffer = recycler.allocInputBuffer(LZFChunk.MAX_CHUNK_LEN);
    }

    @Override
    public void readHeader(StreamInput in) throws IOException {
        // nothing to do here, each chunk has a header
    }

    @Override
    public int uncompress(StreamInput in, byte[] out) throws IOException {
        return decoder.decodeChunk(in, inputBuffer, out);
    }

    @Override
    protected void doClose() throws IOException {
        byte[] buf = inputBuffer;
        if (buf != null) {
            inputBuffer = null;
            recycler.releaseInputBuffer(buf);
        }
        buf = uncompressed;
        if (buf != null) {
            uncompressed = null;
            recycler.releaseDecodeBuffer(uncompressed);
        }
    }
}
