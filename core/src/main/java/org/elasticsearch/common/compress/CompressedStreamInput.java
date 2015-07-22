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
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.EOFException;
import java.io.IOException;

/**
 */
public abstract class CompressedStreamInput extends StreamInput {

    private final StreamInput in;

    private boolean closed;

    protected byte[] uncompressed;
    private int position = 0;
    private int valid = 0;

    public CompressedStreamInput(StreamInput in) throws IOException {
        this.in = in;
        super.setVersion(in.getVersion());
        readHeader(in);
    }

    @Override
    public void setVersion(Version version) {
        in.setVersion(version);
        super.setVersion(version);
    }

    /**
     * Method is overridden to report number of bytes that can now be read
     * from decoded data buffer, without reading bytes from the underlying
     * stream.
     * Never throws an exception; returns number of bytes available without
     * further reads from underlying source; -1 if stream has been closed, or
     * 0 if an actual read (and possible blocking) is needed to find out.
     */
    @Override
    public int available() throws IOException {
        // if closed, return -1;
        if (closed) {
            return -1;
        }
        int left = (valid - position);
        return (left <= 0) ? 0 : left;
    }

    @Override
    public int read() throws IOException {
        if (!readyBuffer()) {
            return -1;
        }
        return uncompressed[position++] & 255;
    }

    @Override
    public byte readByte() throws IOException {
        if (!readyBuffer()) {
            throw new EOFException();
        }
        return uncompressed[position++];
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
        return read(buffer, offset, length, false);
    }

    public int read(byte[] buffer, int offset, int length, boolean fullRead) throws IOException {
        if (length < 1) {
            return 0;
        }
        if (!readyBuffer()) {
            return -1;
        }
        // First let's read however much data we happen to have...
        int chunkLength = Math.min(valid - position, length);
        System.arraycopy(uncompressed, position, buffer, offset, chunkLength);
        position += chunkLength;

        if (chunkLength == length || !fullRead) {
            return chunkLength;
        }
        // Need more data, then
        int totalRead = chunkLength;
        do {
            offset += chunkLength;
            if (!readyBuffer()) {
                break;
            }
            chunkLength = Math.min(valid - position, (length - totalRead));
            System.arraycopy(uncompressed, position, buffer, offset, chunkLength);
            position += chunkLength;
            totalRead += chunkLength;
        } while (totalRead < length);

        return totalRead;
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        int result = read(b, offset, len, true /* we want to have full reads, thats the contract... */);
        if (result < len) {
            throw new EOFException();
        }
    }

    @Override
    public void reset() throws IOException {
        this.position = 0;
        this.valid = 0;
        in.reset();
    }

    @Override
    public void close() throws IOException {
        position = valid = 0;
        if (!closed) {
            closed = true;
            doClose();
            in.close();
        }
    }

    protected abstract void doClose() throws IOException;

    /**
     * Fill the uncompressed bytes buffer by reading the underlying inputStream.
     */
    protected boolean readyBuffer() throws IOException {
        if (position < valid) {
            return true;
        }
        if (closed) {
            return false;
        }
        valid = uncompress(in, uncompressed);
        if (valid < 0) {
            return false;
        }
        position = 0;
        return (position < valid);
    }

    protected abstract void readHeader(StreamInput in) throws IOException;

    /**
     * Uncompress the data into the out array, returning the size uncompressed
     */
    protected abstract int uncompress(StreamInput in, byte[] out) throws IOException;

}
