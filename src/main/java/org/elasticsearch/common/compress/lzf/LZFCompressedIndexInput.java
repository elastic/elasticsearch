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
import com.ning.compress.lzf.LZFChunk;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.common.compress.CompressedIndexInput;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;

import java.io.IOException;
import java.util.Arrays;

/**
 */
@Deprecated
public class LZFCompressedIndexInput extends CompressedIndexInput<LZFCompressorContext> {

    private final ChunkDecoder decoder;
    // scratch area buffer
    private byte[] inputBuffer;

    public LZFCompressedIndexInput(IndexInput in, ChunkDecoder decoder) throws IOException {
        super(in, LZFCompressorContext.INSTANCE);

        this.decoder = decoder;
        this.uncompressed = new byte[LZFChunk.MAX_CHUNK_LEN];
        this.uncompressedLength = LZFChunk.MAX_CHUNK_LEN;
        this.inputBuffer = new byte[LZFChunk.MAX_CHUNK_LEN];
    }

    @Override
    protected void readHeader(IndexInput in) throws IOException {
        byte[] header = new byte[LZFCompressor.LUCENE_HEADER.length];
        in.readBytes(header, 0, header.length, false);
        if (!Arrays.equals(header, LZFCompressor.LUCENE_HEADER)) {
            throw new IOException("wrong lzf compressed header [" + Arrays.toString(header) + "]");
        }
    }

    @Override
    protected int uncompress(IndexInput in, byte[] out) throws IOException {
        return decoder.decodeChunk(new InputStreamIndexInput(in, Long.MAX_VALUE), inputBuffer, out);
    }

    @Override
    protected void doClose() throws IOException {
        // nothing to do here...
    }

    @Override
    public IndexInput clone() {
        LZFCompressedIndexInput cloned = (LZFCompressedIndexInput) super.clone();
        cloned.inputBuffer = new byte[LZFChunk.MAX_CHUNK_LEN];
        return cloned;
    }

    @Override
    public IndexInput slice(String description, long offset, long length) throws IOException {
        return BufferedIndexInput.wrap(description, this, offset, length);
    }
}
