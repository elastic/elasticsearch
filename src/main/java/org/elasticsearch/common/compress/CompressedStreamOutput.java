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

package org.elasticsearch.common.compress;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 */
public abstract class CompressedStreamOutput<T extends CompressorContext> extends StreamOutput {

    private final StreamOutput out;
    protected final T context;

    protected byte[] uncompressed;
    protected int uncompressedLength;
    private int position = 0;

    private boolean closed;

    public CompressedStreamOutput(StreamOutput out, T context) throws IOException {
        this.out = out;
        this.context = context;
        super.setVersion(out.getVersion());
        writeHeader(out);
    }

    @Override
    public StreamOutput setVersion(Version version) {
        out.setVersion(version);
        return super.setVersion(version);
    }

    @Override
    public void write(int b) throws IOException {
        if (position >= uncompressedLength) {
            flushBuffer();
        }
        uncompressed[position++] = (byte) b;
    }

    @Override
    public void writeByte(byte b) throws IOException {
        if (position >= uncompressedLength) {
            flushBuffer();
        }
        uncompressed[position++] = b;
    }

    @Override
    public void writeBytes(byte[] input, int offset, int length) throws IOException {
        // ES, check if length is 0, and don't write in this case
        if (length == 0) {
            return;
        }
        final int BUFFER_LEN = uncompressedLength;

        // simple case first: buffering only (for trivially short writes)
        int free = BUFFER_LEN - position;
        if (free >= length) {
            System.arraycopy(input, offset, uncompressed, position, length);
            position += length;
            return;
        }
        // fill partial input as much as possible and flush
        if (position > 0) {
            System.arraycopy(input, offset, uncompressed, position, free);
            position += free;
            flushBuffer();
            offset += free;
            length -= free;
        }

        // then write intermediate full block, if any, without copying:
        while (length >= BUFFER_LEN) {
            compress(input, offset, BUFFER_LEN, out);
            offset += BUFFER_LEN;
            length -= BUFFER_LEN;
        }

        // and finally, copy leftovers in input, if any
        if (length > 0) {
            System.arraycopy(input, offset, uncompressed, 0, length);
        }
        position = length;
    }

    @Override
    public void flush() throws IOException {
        flushBuffer();
        out.flush();
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            flushBuffer();
            closed = true;
            doClose();
            out.close();
        }
    }

    protected abstract void doClose() throws IOException;

    @Override
    public void reset() throws IOException {
        position = 0;
        out.reset();
    }

    private void flushBuffer() throws IOException {
        if (position > 0) {
            compress(uncompressed, 0, position, out);
            position = 0;
        }
    }

    protected abstract void writeHeader(StreamOutput out) throws IOException;

    /**
     * Compresses the data into the output
     */
    protected abstract void compress(byte[] data, int offset, int len, StreamOutput out) throws IOException;

}
