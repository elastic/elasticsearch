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

import org.elasticsearch.common.compress.lzf.BufferRecycler;
import org.elasticsearch.common.compress.lzf.LZFChunk;
import org.elasticsearch.common.compress.lzf.LZFDecoder;

import java.io.EOFException;
import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class LZFStreamInput extends StreamInput {
    private final BufferRecycler _recycler;

    /**
     * stream to be decompressed
     */
    protected StreamInput inputStream;

    /**
     * Flag that indicates whether we force full reads (reading of as many
     * bytes as requested), or 'optimal' reads (up to as many as available,
     * but at least one). Default is false, meaning that 'optimal' read
     * is used.
     */
    protected boolean cfgFullReads = true; // ES: ALWAYS TRUE since we need to throw EOF when doing readBytes

    /* the current buffer of compressed bytes (from which to decode) */
    private byte[] _inputBuffer;

    /* the buffer of uncompressed bytes from which content is read */
    private byte[] _decodedBytes;

    /* The current position (next char to output) in the uncompressed bytes buffer. */
    private int bufferPosition = 0;

    /* Length of the current uncompressed bytes buffer */
    private int bufferLength = 0;

    // ES: added to support never closing just resetting
    private final boolean cached;

    public LZFStreamInput(StreamInput in, boolean cached) {
        super();
        this.cached = cached;
        if (cached) {
            _recycler = new BufferRecycler();
        } else {
            _recycler = BufferRecycler.instance();
        }
        inputStream = in;

        _inputBuffer = _recycler.allocInputBuffer(LZFChunk.MAX_CHUNK_LEN);
        _decodedBytes = _recycler.allocDecodeBuffer(LZFChunk.MAX_CHUNK_LEN);
    }

    @Override public int read() throws IOException {
        readyBuffer();
        if (bufferPosition < bufferLength) {
            return _decodedBytes[bufferPosition++] & 255;
        }
        return -1;
    }

    @Override public int read(byte[] buffer, int offset, int length) throws IOException {
        if (length < 1) {
            return 0;
        }
        readyBuffer();
        if (bufferLength < 0) {
            return -1;
        }
        // First let's read however much data we happen to have...
        int chunkLength = Math.min(bufferLength - bufferPosition, length);
        System.arraycopy(_decodedBytes, bufferPosition, buffer, offset, chunkLength);
        bufferPosition += chunkLength;

        if (chunkLength == length || !cfgFullReads) {
            return chunkLength;
        }
        // Need more data, then
        int totalRead = chunkLength;
        do {
            offset += chunkLength;
            readyBuffer();
            if (bufferLength == -1) {
                break;
            }
            chunkLength = Math.min(bufferLength - bufferPosition, (length - totalRead));
            System.arraycopy(_decodedBytes, bufferPosition, buffer, offset, chunkLength);
            bufferPosition += chunkLength;
            totalRead += chunkLength;
        } while (totalRead < length);

        return totalRead;
    }

    @Override public byte readByte() throws IOException {
        readyBuffer();
        if (bufferPosition < bufferLength) {
            return _decodedBytes[bufferPosition++];
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
        inputStream.reset();
    }

    public void reset(StreamInput in) throws IOException {
        this.inputStream = in;
        this.bufferPosition = 0;
        this.bufferLength = 0;
    }

    /**
     * Expert!, resets to buffer start, without the need to decompress it again.
     */
    public void resetToBufferStart() {
        this.bufferPosition = 0;
    }

    @Override public void close() throws IOException {
        if (cached) {
            reset();
            return;
        }
        bufferPosition = bufferLength = 0;
        byte[] buf = _inputBuffer;
        if (buf != null) {
            _inputBuffer = null;
            _recycler.releaseInputBuffer(buf);
        }
        buf = _decodedBytes;
        if (buf != null) {
            _decodedBytes = null;
            _recycler.releaseDecodeBuffer(buf);
        }
        inputStream.close();
    }

    /**
     * Fill the uncompressed bytes buffer by reading the underlying inputStream.
     *
     * @throws java.io.IOException
     */
    private void readyBuffer() throws IOException {
        if (bufferPosition >= bufferLength) {
            bufferLength = LZFDecoder.decompressChunk(inputStream, _inputBuffer, _decodedBytes);
            bufferPosition = 0;
        }
    }
}
