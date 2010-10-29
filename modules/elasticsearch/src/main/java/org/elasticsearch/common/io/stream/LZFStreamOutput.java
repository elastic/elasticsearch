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

package org.elasticsearch.common.io.stream;

import org.elasticsearch.common.compress.lzf.ChunkEncoder;
import org.elasticsearch.common.compress.lzf.LZFChunk;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class LZFStreamOutput extends StreamOutput {

    private StreamOutput out;

    private final byte[] outputBuffer = new byte[LZFChunk.MAX_CHUNK_LEN];
    private final ChunkEncoder encoder = new ChunkEncoder(LZFChunk.MAX_CHUNK_LEN);

    private int position = 0;

    public LZFStreamOutput(StreamOutput out) {
        this.out = out;
    }

    @Override public void write(final int singleByte) throws IOException {
        if (position >= outputBuffer.length) {
            writeCompressedBlock();
        }
        outputBuffer[position++] = (byte) (singleByte & 0xff);
    }

    @Override public void writeByte(byte b) throws IOException {
        if (position >= outputBuffer.length) {
            writeCompressedBlock();
        }
        outputBuffer[position++] = b;
    }

    @Override public void writeBytes(byte[] b, int offset, int length) throws IOException {
        int inputCursor = offset;
        int remainingBytes = length;
        while (remainingBytes > 0) {
            if (position >= outputBuffer.length) {
                writeCompressedBlock();
            }
            int chunkLength = (remainingBytes > (outputBuffer.length - position)) ? outputBuffer.length - position : remainingBytes;
            System.arraycopy(b, inputCursor, outputBuffer, position, chunkLength);
            position += chunkLength;
            remainingBytes -= chunkLength;
            inputCursor += chunkLength;
        }
    }

    @Override public void flush() throws IOException {
        try {
            writeCompressedBlock();
        } finally {
            out.flush();
        }
    }

    @Override public void close() throws IOException {
        try {
            flush();
        } finally {
            out.close();
        }
    }

    @Override public void reset() throws IOException {
        this.position = 0;
        out.reset();
    }

    public void reset(StreamOutput out) throws IOException {
        this.out = out;
        reset();
    }

    public StreamOutput wrappedOut() {
        return this.out;
    }

    /**
     * Compress and write the current block to the OutputStream
     */
    private void writeCompressedBlock() throws IOException {
        if (position > 0) {
            encoder.encodeChunk(out, outputBuffer, 0, position);
            position = 0;
        }
    }
}
