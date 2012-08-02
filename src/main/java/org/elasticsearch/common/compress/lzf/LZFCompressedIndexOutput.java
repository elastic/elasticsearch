/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

import com.ning.compress.BufferRecycler;
import com.ning.compress.lzf.ChunkEncoder;
import com.ning.compress.lzf.LZFChunk;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.common.compress.CompressedIndexOutput;
import org.elasticsearch.common.lucene.store.OutputStreamIndexOutput;

import java.io.IOException;

/**
 */
public class LZFCompressedIndexOutput extends CompressedIndexOutput<LZFCompressorContext> {

    private final BufferRecycler recycler;
    private final ChunkEncoder encoder;

    public LZFCompressedIndexOutput(IndexOutput out) throws IOException {
        super(out, LZFCompressorContext.INSTANCE);
        this.recycler = BufferRecycler.instance();
        this.uncompressed = this.recycler.allocOutputBuffer(LZFChunk.MAX_CHUNK_LEN);
        this.uncompressedLength = LZFChunk.MAX_CHUNK_LEN;
        this.encoder = new ChunkEncoder(LZFChunk.MAX_CHUNK_LEN);
    }

    @Override
    protected void writeHeader(IndexOutput out) throws IOException {
        out.writeBytes(LZFCompressor.LUCENE_HEADER, LZFCompressor.LUCENE_HEADER.length);
    }

    @Override
    protected void compress(byte[] data, int offset, int len, IndexOutput out) throws IOException {
        encoder.encodeAndWriteChunk(data, offset, len, new OutputStreamIndexOutput(out));
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
