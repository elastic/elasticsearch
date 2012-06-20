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

package org.elasticsearch.common.compress;

import org.apache.lucene.store.IndexInput;

import java.io.EOFException;
import java.io.IOException;

/**
 */
public abstract class CompressedIndexInput extends IndexInput {

    private IndexInput in;

    private int version;
    private long uncompressedLength;
    private long[] offsets;

    private boolean closed;

    protected byte[] uncompressed;
    private int position = 0;
    private int valid = 0;
    private long headerLength;
    private int currentOffsetIdx;
    private long currentOffset;
    private long currentOffsetFilePointer;
    private long metaDataPosition;

    public CompressedIndexInput(IndexInput in) throws IOException {
        super("compressed(" + in.toString() + ")");
        this.in = in;
        readHeader(in);
        this.version = in.readInt();
        metaDataPosition = in.readLong();
        headerLength = in.getFilePointer();
        in.seek(metaDataPosition);
        this.uncompressedLength = in.readVLong();
        int size = in.readVInt();
        offsets = new long[size];
        for (int i = 0; i < offsets.length; i++) {
            offsets[i] = in.readVLong();
        }
        this.currentOffsetIdx = -1;
        this.currentOffset = 0;
        this.currentOffsetFilePointer = 0;
        in.seek(headerLength);
    }

    /**
     * Method is overridden to report number of bytes that can now be read
     * from decoded data buffer, without reading bytes from the underlying
     * stream.
     * Never throws an exception; returns number of bytes available without
     * further reads from underlying source; -1 if stream has been closed, or
     * 0 if an actual read (and possible blocking) is needed to find out.
     */
    public int available() throws IOException {
        // if closed, return -1;
        if (closed) {
            return -1;
        }
        int left = (valid - position);
        return (left <= 0) ? 0 : left;
    }

    @Override
    public byte readByte() throws IOException {
        if (!readyBuffer()) {
            throw new EOFException();
        }
        return uncompressed[position++];
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
    public long getFilePointer() {
        return currentOffsetFilePointer + position;
    }

    @Override
    public void seek(long pos) throws IOException {
        int idx = (int) (pos / uncompressed.length);
        if (idx >= offsets.length) {
            // set the next "readyBuffer" to EOF
            currentOffsetIdx = idx;
            position = 0;
            valid = 0;
            return;
        }

        // TODO: optimize so we won't have to readyBuffer on seek, can keep the position around, and set it on readyBuffer in this case
        long pointer = offsets[idx];
        if (pointer != currentOffset) {
            in.seek(pointer);
            position = 0;
            valid = 0;
            currentOffsetIdx = idx - 1; // we are going to increase it in readyBuffer...
            readyBuffer();
        }
        position = (int) (pos % uncompressed.length);
    }

    @Override
    public long length() {
        return uncompressedLength;
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

    protected boolean readyBuffer() throws IOException {
        if (position < valid) {
            return true;
        }
        if (closed) {
            return false;
        }
        // we reached the end...
        if (currentOffsetIdx + 1 >= offsets.length) {
            return false;
        }
        valid = uncompress(in, uncompressed);
        if (valid < 0) {
            return false;
        }
        currentOffsetIdx++;
        currentOffset = offsets[currentOffsetIdx];
        currentOffsetFilePointer = currentOffset - headerLength;
        position = 0;
        return (position < valid);
    }

    protected abstract void readHeader(IndexInput in) throws IOException;

    /**
     * Uncompress the data into the out array, returning the size uncompressed
     */
    protected abstract int uncompress(IndexInput in, byte[] out) throws IOException;

    @Override
    public Object clone() {
        CompressedIndexInput cloned = (CompressedIndexInput) super.clone();
        cloned.position = 0;
        cloned.valid = 0;
        cloned.in = (IndexInput) cloned.in.clone();
        return cloned;
    }
}
