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

import org.apache.lucene.store.IndexInput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;

import java.io.EOFException;
import java.io.IOException;

/**
 * @deprecated Used only for backward comp. to read old compressed files, since we now use codec based compression
 */
@Deprecated
public abstract class CompressedIndexInput<T extends CompressorContext> extends IndexInput {

    private IndexInput in;
    protected final T context;

    private int version;
    private long totalUncompressedLength;
    private LongArray offsets;

    private boolean closed;

    protected byte[] uncompressed;
    protected int uncompressedLength;
    private int position = 0;
    private int valid = 0;
    private int currentOffsetIdx;
    private long currentUncompressedChunkPointer;

    public CompressedIndexInput(IndexInput in, T context) throws IOException {
        super("compressed(" + in.toString() + ")");
        this.in = in;
        this.context = context;
        readHeader(in);
        this.version = in.readInt();
        long metaDataPosition = in.readLong();
        long headerLength = in.getFilePointer();
        in.seek(metaDataPosition);
        this.totalUncompressedLength = in.readVLong();
        int size = in.readVInt();
        offsets = BigArrays.NON_RECYCLING_INSTANCE.newLongArray(size);
        for (int i = 0; i < size; i++) {
            offsets.set(i, in.readVLong());
        }
        this.currentOffsetIdx = -1;
        this.currentUncompressedChunkPointer = 0;
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
        return currentUncompressedChunkPointer + position;
    }

    @Override
    public void seek(long pos) throws IOException {
        int idx = (int) (pos / uncompressedLength);
        if (idx >= offsets.size()) {
            // set the next "readyBuffer" to EOF
            currentOffsetIdx = idx;
            position = 0;
            valid = 0;
            return;
        }

        // TODO: optimize so we won't have to readyBuffer on seek, can keep the position around, and set it on readyBuffer in this case
        if (idx != currentOffsetIdx) {
            long pointer = offsets.get(idx);
            in.seek(pointer);
            position = 0;
            valid = 0;
            currentOffsetIdx = idx - 1; // we are going to increase it in readyBuffer...
            readyBuffer();
        }
        position = (int) (pos % uncompressedLength);
    }

    @Override
    public long length() {
        return totalUncompressedLength;
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
        if (currentOffsetIdx + 1 >= offsets.size()) {
            return false;
        }
        valid = uncompress(in, uncompressed);
        if (valid < 0) {
            return false;
        }
        currentOffsetIdx++;
        currentUncompressedChunkPointer = ((long) currentOffsetIdx) * uncompressedLength;
        position = 0;
        return (position < valid);
    }

    protected abstract void readHeader(IndexInput in) throws IOException;

    /**
     * Uncompress the data into the out array, returning the size uncompressed
     */
    protected abstract int uncompress(IndexInput in, byte[] out) throws IOException;

    @Override
    public IndexInput clone() {
        // we clone and we need to make sure we keep the same positions!
        CompressedIndexInput cloned = (CompressedIndexInput) super.clone();
        cloned.uncompressed = new byte[uncompressedLength];
        System.arraycopy(uncompressed, 0, cloned.uncompressed, 0, uncompressedLength);
        cloned.in = (IndexInput) cloned.in.clone();
        return cloned;
    }
}
