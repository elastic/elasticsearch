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

package org.elasticsearch.common.io.stream;

import org.elasticsearch.common.compress.lzf.BufferRecycler;
import org.elasticsearch.common.compress.lzf.ChunkEncoder;
import org.elasticsearch.common.compress.lzf.LZFChunk;

import java.io.IOException;

/**
 *
 */
public class LZFStreamOutput extends StreamOutput {

    private static int OUTPUT_BUFFER_SIZE = LZFChunk.MAX_CHUNK_LEN;

    private final ChunkEncoder _encoder;
    private final BufferRecycler _recycler;

    protected StreamOutput _outputStream;
    protected byte[] _outputBuffer;
    protected int _position = 0;


    /**
     * Configuration setting that governs whether basic 'flush()' should
     * first complete a block or not.
     * <p/>
     * Default value is 'true'
     *
     * @since 0.8
     */
    protected boolean _cfgFinishBlockOnFlush = true;

    private final boolean neverClose;

    public LZFStreamOutput(StreamOutput out, boolean neverClose) {
        this.neverClose = neverClose;
        _recycler = neverClose ? new BufferRecycler() : BufferRecycler.instance();
        _encoder = new ChunkEncoder(OUTPUT_BUFFER_SIZE, _recycler);
        _outputStream = out;
        _outputBuffer = _recycler.allocOutputBuffer(OUTPUT_BUFFER_SIZE);
    }

    @Override
    public void write(final int singleByte) throws IOException {
        if (_position >= _outputBuffer.length) {
            writeCompressedBlock();
        }
        _outputBuffer[_position++] = (byte) singleByte;
    }

    @Override
    public void writeByte(byte b) throws IOException {
        if (_position >= _outputBuffer.length) {
            writeCompressedBlock();
        }
        _outputBuffer[_position++] = b;
    }

    @Override
    public void writeBytes(byte[] buffer, int offset, int length) throws IOException {
        // ES, check if length is 0, and don't write in this case
        if (length == 0) {
            return;
        }
        final int BUFFER_LEN = _outputBuffer.length;

        // simple case first: buffering only (for trivially short writes)
        int free = BUFFER_LEN - _position;
        if (free >= length) {
            System.arraycopy(buffer, offset, _outputBuffer, _position, length);
            _position += length;
            return;
        }
        // otherwise, copy whatever we can, flush
        System.arraycopy(buffer, offset, _outputBuffer, _position, free);
        offset += free;
        length -= free;
        _position += free;
        writeCompressedBlock();

        // then write intermediate full block, if any, without copying:
        while (length >= BUFFER_LEN) {
            _encoder.encodeAndWriteChunk(buffer, offset, BUFFER_LEN, _outputStream);
            offset += BUFFER_LEN;
            length -= BUFFER_LEN;
        }

        // and finally, copy leftovers in buffer, if any
        if (length > 0) {
            System.arraycopy(buffer, offset, _outputBuffer, 0, length);
        }
        _position = length;
    }

    @Override
    public void flush() throws IOException {
        if (_cfgFinishBlockOnFlush && _position > 0) {
            writeCompressedBlock();
        }
        _outputStream.flush();
    }

    @Override
    public void close() throws IOException {
        if (_position > 0) {
            writeCompressedBlock();
        }
        if (neverClose) {
            // just reset here the LZF stream (not the underlying stream, since we might want to read from it)
            _position = 0;
            return;
        }
        _outputStream.flush();
        _encoder.close();
        byte[] buf = _outputBuffer;
        if (buf != null) {
            _outputBuffer = null;
            _recycler.releaseOutputBuffer(buf);
        }
        _outputStream.close();
    }

    @Override
    public void reset() throws IOException {
        _position = 0;
        _outputStream.reset();
    }

    public void reset(StreamOutput out) throws IOException {
        this._outputStream = out;
        reset();
    }

    public StreamOutput wrappedOut() {
        return this._outputStream;
    }

    /**
     * Compress and write the current block to the OutputStream
     */
    private void writeCompressedBlock() throws IOException {
        int left = _position;
        _position = 0;
        int offset = 0;

        do {
            int chunkLen = Math.min(LZFChunk.MAX_CHUNK_LEN, left);
            _encoder.encodeAndWriteChunk(_outputBuffer, offset, chunkLen, _outputStream);
            offset += chunkLen;
            left -= chunkLen;
        } while (left > 0);
    }
}
