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

import org.elasticsearch.common.compress.lzf.LZFChunk;
import org.elasticsearch.common.compress.lzf.LZFDecoder;

import java.io.EOFException;
import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class LZFStreamInput extends StreamInput {
    public static int EOF_FLAG = -1;

    /* the current buffer of compressed bytes */
    private final byte[] compressedBytes = new byte[LZFChunk.MAX_CHUNK_LEN];

    /* the buffer of uncompressed bytes from which */
    private final byte[] uncompressedBytes = new byte[LZFChunk.MAX_CHUNK_LEN];

    /* The current position (next char to output) in the uncompressed bytes buffer. */
    private int bufferPosition = 0;

    /* Length of the current uncompressed bytes buffer */
    private int bufferLength = 0;

    private StreamInput in;

    public LZFStreamInput() {
    }

    public LZFStreamInput(StreamInput in) throws IOException {
        this.in = in;
        // we need to read the first buffer here, since it might be a VOID message, and we need to at least read the LZF header
        readyBuffer();
    }

    @Override public int read() throws IOException {
        int returnValue = EOF_FLAG;
        readyBuffer();
        if (bufferPosition < bufferLength) {
            returnValue = (uncompressedBytes[bufferPosition++] & 255);
        }
        return returnValue;
    }

    @Override public int read(byte[] b, int off, int len) throws IOException {
        if (len == 0) {
            return 0;
        }
        int outputPos = off;
        readyBuffer();
        if (bufferLength == -1) {
            return -1;
        }

        while (outputPos < len && bufferPosition < bufferLength) {
            int chunkLength = Math.min(bufferLength - bufferPosition, len - outputPos);
            System.arraycopy(uncompressedBytes, bufferPosition, b, outputPos, chunkLength);
            outputPos += chunkLength;
            bufferPosition += chunkLength;
            readyBuffer();
        }
        return outputPos - off;
    }

    @Override public byte readByte() throws IOException {
        readyBuffer();
        if (bufferPosition < bufferLength) {
            return (uncompressedBytes[bufferPosition++]);
        }
        throw new EOFException();
    }

    @Override public void readBytes(byte[] b, int offset, int len) throws IOException {
        int result = read(b, offset, len);
        if (result < len) {
            throw new EOFException();
        }
    }

    @Override public void reset() throws IOException {
        this.bufferPosition = 0;
        this.bufferLength = 0;
        in.reset();
    }

    public void reset(StreamInput in) throws IOException {
        this.in = in;
        this.bufferPosition = 0;
        this.bufferLength = 0;
        // we need to read the first buffer here, since it might be a VOID message, and we need to at least read the LZF header
        readyBuffer();
    }

    /**
     * Expert!, resets to buffer start, without the need to decompress it again.
     */
    public void resetToBufferStart() {
        this.bufferPosition = 0;
    }

    @Override public void close() throws IOException {
        in.close();
    }

    /**
     * Fill the uncompressed bytes buffer by reading the underlying inputStream.
     *
     * @throws java.io.IOException
     */
    private void readyBuffer() throws IOException {
        if (bufferPosition >= bufferLength) {
            bufferLength = LZFDecoder.decompressChunk(in, compressedBytes, uncompressedBytes);
            bufferPosition = 0;
        }
    }
}
