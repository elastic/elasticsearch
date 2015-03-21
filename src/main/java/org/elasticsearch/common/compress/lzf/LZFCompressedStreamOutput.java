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
import com.ning.compress.lzf.ChunkEncoder;
import com.ning.compress.lzf.LZFChunk;
import com.ning.compress.lzf.util.ChunkEncoderFactory;
import org.elasticsearch.common.compress.CompressedStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class LZFCompressedStreamOutput extends CompressedStreamOutput<LZFCompressorContext> {

    private final BufferRecycler recycler;
    private final ChunkEncoder encoder;

    public LZFCompressedStreamOutput(StreamOutput out) throws IOException {
        super(out, LZFCompressorContext.INSTANCE);
        this.recycler = BufferRecycler.instance();
        this.uncompressed = this.recycler.allocOutputBuffer(LZFChunk.MAX_CHUNK_LEN);
        this.uncompressedLength = LZFChunk.MAX_CHUNK_LEN;
        this.encoder = ChunkEncoderFactory.safeInstance(recycler);
    }

    @Override
    public void writeHeader(StreamOutput out) throws IOException {
        // nothing to do here, each chunk has a header of its own
    }

    @Override
    protected void compress(byte[] data, int offset, int len, StreamOutput out) throws IOException {
        encoder.encodeAndWriteChunk(data, offset, len, out);
    }

    @Override
    protected void doClose() throws IOException {
        byte[] buf = uncompressed;
        if (buf != null) {
            uncompressed = null;
            recycler.releaseOutputBuffer(buf);
        }
        encoder.close();
    }
}
