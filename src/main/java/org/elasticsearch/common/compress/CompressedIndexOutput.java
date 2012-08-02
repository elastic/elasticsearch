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

import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;

/**
 */
public abstract class CompressedIndexOutput<T extends CompressorContext> extends IndexOutput {

    final IndexOutput out;
    protected final T context;

    protected byte[] uncompressed;
    protected int uncompressedLength;
    private int position = 0;

    private long uncompressedPosition;

    private boolean closed;

    private final long metaDataPointer;
    // need to have a growing segment long array list here...
    private TLongArrayList offsets = new TLongArrayList();

    public CompressedIndexOutput(IndexOutput out, T context) throws IOException {
        this.out = out;
        this.context = context;
        writeHeader(out);
        out.writeInt(0); // version
        metaDataPointer = out.getFilePointer();
        out.writeLong(-1); // the pointer to the end of the file metadata
    }

    public IndexOutput underlying() {
        return this.out;
    }

    @Override
    public void writeByte(byte b) throws IOException {
        if (position >= uncompressedLength) {
            flushBuffer();
        }
        uncompressedPosition++;
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
            uncompressedPosition += length;
            return;
        }
        // fill partial input as much as possible and flush
        if (position > 0) {
            System.arraycopy(input, offset, uncompressed, position, free);
            position += free;
            uncompressedPosition += free;
            flushBuffer();
            offset += free;
            length -= free;
        }

        // then write intermediate full block, if any, without copying:
        while (length >= BUFFER_LEN) {
            offsets.add(out.getFilePointer());
            compress(input, offset, BUFFER_LEN, out);
            offset += BUFFER_LEN;
            length -= BUFFER_LEN;
            uncompressedPosition += BUFFER_LEN;
        }

        // and finally, copy leftovers in input, if any
        if (length > 0) {
            System.arraycopy(input, offset, uncompressed, 0, length);
        }
        position = length;
        uncompressedPosition += length;
    }

    @Override
    public void copyBytes(DataInput input, long length) throws IOException {
        final int BUFFER_LEN = uncompressedLength;

        // simple case first: buffering only (for trivially short writes)
        int free = BUFFER_LEN - position;
        if (free >= length) {
            input.readBytes(uncompressed, position, (int) length, false);
            position += length;
            uncompressedPosition += length;
            return;
        }
        // fill partial input as much as possible and flush
        if (position > 0) {
            input.readBytes(uncompressed, position, free, false);
            position += free;
            uncompressedPosition += free;
            flushBuffer();
            length -= free;
        }

        // then write intermediate full block, if any, without copying:

        // Note, if we supported flushing buffers not on "chunkSize", then
        // we could have flushed up to the rest of non compressed data in the input
        // and then copy compressed segments. This means though that we need to
        // store the compressed sizes of each segment on top of the offsets, and
        // CompressedIndexInput#seek would be more costly, since it can't do (pos / chunk)
        // to get the index...

        while (length >= BUFFER_LEN) {
            offsets.add(out.getFilePointer());
            input.readBytes(uncompressed, 0, BUFFER_LEN);
            compress(uncompressed, 0, BUFFER_LEN, out);
            length -= BUFFER_LEN;
            uncompressedPosition += BUFFER_LEN;
        }

        // and finally, copy leftovers in input, if any
        if (length > 0) {
            input.readBytes(uncompressed, 0, (int) length, false);
        }
        position = (int) length;
        uncompressedPosition += length;
    }

    @Override
    public void flush() throws IOException {
        // ignore flush, we always want to flush on actual block size
        //flushBuffer();
        out.flush();
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            flushBuffer();

            // write metadata, and update pointer
            long metaDataPointerValue = out.getFilePointer();
            // length uncompressed
            out.writeVLong(uncompressedPosition);
            // compressed pointers
            out.writeVInt(offsets.size());
            for (TLongIterator it = offsets.iterator(); it.hasNext(); ) {
                out.writeVLong(it.next());
            }
            out.seek(metaDataPointer);
            out.writeLong(metaDataPointerValue);

            closed = true;
            doClose();
            out.close();
        }
    }

    protected abstract void doClose() throws IOException;

    @Override
    public long getFilePointer() {
        return uncompressedPosition;
    }

    @Override
    public void seek(long pos) throws IOException {
        throw new IOException("seek not supported on compressed output");
    }

    @Override
    public long length() throws IOException {
        return uncompressedPosition;
    }

    private void flushBuffer() throws IOException {
        if (position > 0) {
            offsets.add(out.getFilePointer());
            compress(uncompressed, 0, position, out);
            position = 0;
        }
    }

    protected abstract void writeHeader(IndexOutput out) throws IOException;

    /**
     * Compresses the data into the output
     */
    protected abstract void compress(byte[] data, int offset, int len, IndexOutput out) throws IOException;
}
