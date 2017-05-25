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

package org.elasticsearch.transport.nio;

import com.carrotsearch.hppc.ObjectArrayDeque;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class CompositeNetworkBuffer extends BytesReference {

    private final ObjectArrayDeque<ByteBufferReference> references;
    private int[] offsets;
    private int length;
    private int writeIndex;
    private int readIndex;

    public CompositeNetworkBuffer() {
        this.references = new ObjectArrayDeque<>(8);
        this.offsets = new int[0];
        this.length = 0;
    }

    public CompositeNetworkBuffer(ByteBufferReference... newReferences) {
        this.references = new ObjectArrayDeque<>(Math.max(8, newReferences.length));
        this.offsets = new int[0];
        this.length = 0;

        addBuffers(newReferences);
    }

    public void addBuffer(ByteBufferReference newReference) {
        addBuffers(newReference);
    }

    public void addBuffers(ByteBufferReference... refs) {
        int initialReferenceCount = references.size();
        int[] newOffsets = new int[offsets.length + refs.length];
        System.arraycopy(offsets, 0, newOffsets, 0, offsets.length);

        try {
            int i = refs.length;
            for (ByteBufferReference ref : refs) {
                addBuffer0(ref, newOffsets, newOffsets.length - i--);
            }
            this.offsets = newOffsets;
        } catch (IllegalArgumentException e) {
            removeAddedReferences(initialReferenceCount);
            throw e;
        }
    }

    public void dropUpTo(int index) {
        int offsetIndex = getOffsetIndex(index);
        int bytesDropped = 0;
        for (int i = 0; i < offsetIndex; ++i) {
            ByteBufferReference removed = references.removeFirst();
            int bytesOfRemoved = removed.length();
            bytesDropped += bytesOfRemoved;
        }

        int bytesToDropFromSplitBuffer = index - offsets[offsetIndex];
        if (bytesToDropFromSplitBuffer != 0) {
            ByteBufferReference first = references.removeFirst();
            ByteBufferReference newRef = first.slice(bytesToDropFromSplitBuffer, first.length() - bytesToDropFromSplitBuffer);
            references.addFirst(newRef);
            bytesDropped += bytesToDropFromSplitBuffer;
        }

        this.writeIndex = Math.max(0, writeIndex - bytesDropped);
        this.readIndex = Math.max(0, readIndex - bytesDropped);
        this.length -= bytesDropped;

        this.offsets = new int[references.size()];
        int currentOffset = 0;
        int i = 0;
        for (ObjectCursor<ByteBufferReference> reference : references) {
            offsets[i++] = currentOffset;
            currentOffset += reference.value.length();
        }
    }

    public int getWriteIndex() {
        return writeIndex;
    }

    public void incrementWrite(int delta) {
        int offsetIndex = getOffsetIndex(writeIndex);
        int i = delta;
        while (i != 0) {
            ByteBufferReference reference = getReference(offsetIndex++);
            int bytesToInc = Math.min(reference.getWriteRemaining(), i);
            reference.incrementWrite(bytesToInc);
            i -= bytesToInc;
        }
        writeIndex += delta;
    }

    public int getWriteRemaining() {
        return length - writeIndex;
    }

    public int getReadIndex() {
        return readIndex;
    }

    public void incrementRead(int delta) {
        int offsetIndex = getOffsetIndex(readIndex);
        int i = delta;
        while (i != 0) {
            ByteBufferReference reference = getReference(offsetIndex++);
            int bytesToInc = Math.min(reference.getReadRemaining(), i);
            reference.incrementRead(bytesToInc);
            i -= bytesToInc;
        }
        readIndex += delta;
    }

    public int getReadRemaining() {
        return length - readIndex;
    }

    public ByteBuffer[] getWriteByteBuffers() {
        if (getWriteRemaining() == 0) {
            return new ByteBuffer[0];
        }

        int offsetIndex = getOffsetIndex(writeIndex);

        int refCount = references.size();
        ByteBuffer[] buffers = new ByteBuffer[refCount - offsetIndex];

        int j = 0;
        for (int i = offsetIndex; i < refCount; ++i) {
            buffers[j++] = getReference(i).getWriteByteBuffer();
        }

        return buffers;
    }

    public ByteBuffer[] getReadByteBuffers() {
        if (getReadRemaining() == 0) {
            return new ByteBuffer[0];
        }

        int offsetIndex = getOffsetIndex(readIndex);

        int refCount = references.size();
        ByteBuffer[] buffers = new ByteBuffer[refCount - offsetIndex];

        int j = 0;
        for (int i = offsetIndex; i < refCount; ++i) {
            buffers[j++] = getReference(i).getReadByteBuffer();
        }

        return buffers;
    }

    @Override
    public byte get(int index) {
        final int i = getOffsetIndex(index);
        return getReference(i).get(index - offsets[i]);
    }

    @Override
    public int length() {
        return length;
    }

    @Override
    public BytesReference slice(int from, int length) {
        // for slices we only need to find the start and the end reference
        // adjust them and pass on the references in between as they are fully contained
        final int to = from + length;
        final int limit = getOffsetIndex(from + length);
        final int start = getOffsetIndex(from);
        final BytesReference[] inSlice = new BytesReference[1 + (limit - start)];
        for (int i = 0, j = start; i < inSlice.length; i++) {
            inSlice[i] = getReference(j++);
        }
        int inSliceOffset = from - offsets[start];
        if (inSlice.length == 1) {
            return inSlice[0].slice(inSliceOffset, length);
        }
        // now adjust slices in front and at the end
        inSlice[0] = inSlice[0].slice(inSliceOffset, inSlice[0].length() - inSliceOffset);
        inSlice[inSlice.length - 1] = inSlice[inSlice.length - 1].slice(0, to - offsets[limit]);
        return new CompositeBytesReference(inSlice);
    }

    @Override
    public BytesRef toBytesRef() {
        BytesRefBuilder builder = new BytesRefBuilder();
        builder.grow(length());
        BytesRef spare;
        BytesRefIterator iterator = iterator();
        try {
            while ((spare = iterator.next()) != null) {
                builder.append(spare);
            }
        } catch (IOException ex) {
            throw new AssertionError("won't happen", ex); // this is really an error since we don't do IO in our bytesreferences
        }
        return builder.toBytesRef();
    }

    @Override
    public BytesRefIterator iterator() {
        if (references.size() > 0) {
            return new BytesRefIterator() {
                int index = 0;
                private BytesRefIterator current = getReference(index++).iterator();

                @Override
                public BytesRef next() throws IOException {
                    BytesRef next = current.next();
                    if (next == null) {
                        while (index < references.size()) {
                            current = getReference(index++).iterator();
                            next = current.next();
                            if (next != null) {
                                break;
                            }
                        }
                    }
                    return next;
                }
            };
        } else {
            return () -> null;
        }
    }

    @Override
    public long ramBytesUsed() {
        // TODO: Naive
        return length;
    }

    private int getOffsetIndex(int offset) {
        final int i = Arrays.binarySearch(offsets, offset);
        return i < 0 ? (-(i + 1)) - 1 : i;
    }

    private ByteBufferReference getReference(int index) {
        Object[] rawBuffer = references.buffer;
        int actualIndex = (references.head + index) % rawBuffer.length;
        return (ByteBufferReference) rawBuffer[actualIndex];
    }

    private void addBuffer0(ByteBufferReference ref, int[] newOffsetArray, int offsetIndex) {
        int refReadIndex = ref.getReadIndex();
        int refWriteIndex = ref.getWriteIndex();
        validateReadAndWritesIndexes(refReadIndex, refWriteIndex);

        newOffsetArray[offsetIndex] = length;
        this.references.addLast(ref);
        this.writeIndex += refWriteIndex;
        this.readIndex += refReadIndex;
        this.length += ref.length();
    }

    private void removeAddedReferences(int initialReferenceCount) {
        int refsToDrop = references.size() - initialReferenceCount;
        for (int i = 0; i < refsToDrop; ++i) {
            ByteBufferReference reference = references.removeLast();
            this.length -= reference.length();
            this.writeIndex -= reference.getWriteIndex();
            this.readIndex -= reference.getReadIndex();
        }
    }

    private void validateReadAndWritesIndexes(int refReadIndex, int refWriteIndex) {
        if (writeIndex != length && refWriteIndex != 0) {
            throw new IllegalArgumentException("The writable spaces must be contiguous across buffers.");
        }
        if (this.readIndex != length && refReadIndex != 0) {
            throw new IllegalArgumentException("The readable spaces must be contiguous across buffers.");
        }
    }
}
