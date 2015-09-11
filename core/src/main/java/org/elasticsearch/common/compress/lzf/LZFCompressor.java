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
import com.ning.compress.lzf.util.ChunkDecoderFactory;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedIndexInput;
import org.elasticsearch.common.compress.Compressor;
import org.elasticsearch.common.compress.deflate.DeflateCompressor;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.Loggers;
import org.jboss.netty.buffer.ChannelBuffer;

import java.io.IOException;

/**
 * @deprecated Use {@link DeflateCompressor} instead
 */
@Deprecated
public class LZFCompressor implements Compressor {

    static final byte[] LUCENE_HEADER = {'L', 'Z', 'F', 0};

    private ChunkDecoder decoder;

    public LZFCompressor() {
        this.decoder = ChunkDecoderFactory.safeInstance();
        Loggers.getLogger(LZFCompressor.class).debug("using decoder[{}] ", this.decoder.getClass().getSimpleName());
    }

    @Override
    public boolean isCompressed(BytesReference bytes) {
        return bytes.length() >= 3 &&
                bytes.get(0) == LZFChunk.BYTE_Z &&
                bytes.get(1) == LZFChunk.BYTE_V &&
                (bytes.get(2) == LZFChunk.BLOCK_TYPE_COMPRESSED || bytes.get(2) == LZFChunk.BLOCK_TYPE_NON_COMPRESSED);
    }

    @Override
    public boolean isCompressed(ChannelBuffer buffer) {
        int offset = buffer.readerIndex();
        return buffer.readableBytes() >= 3 &&
                buffer.getByte(offset) == LZFChunk.BYTE_Z &&
                buffer.getByte(offset + 1) == LZFChunk.BYTE_V &&
                (buffer.getByte(offset + 2) == LZFChunk.BLOCK_TYPE_COMPRESSED || buffer.getByte(offset + 2) == LZFChunk.BLOCK_TYPE_NON_COMPRESSED);
    }

    @Override
    public boolean isCompressed(IndexInput in) throws IOException {
        long currentPointer = in.getFilePointer();
        // since we have some metdata before the first compressed header, we check on our specific header
        if (in.length() - currentPointer < (LUCENE_HEADER.length)) {
            return false;
        }
        for (int i = 0; i < LUCENE_HEADER.length; i++) {
            if (in.readByte() != LUCENE_HEADER[i]) {
                in.seek(currentPointer);
                return false;
            }
        }
        in.seek(currentPointer);
        return true;
    }

    @Override
    public StreamInput streamInput(StreamInput in) throws IOException {
        return new LZFCompressedStreamInput(in, decoder);
    }

    @Override
    public StreamOutput streamOutput(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("LZF is only here for back compat, no write support");
    }

    @Override
    public CompressedIndexInput indexInput(IndexInput in) throws IOException {
        return new LZFCompressedIndexInput(in, decoder);
    }
}
