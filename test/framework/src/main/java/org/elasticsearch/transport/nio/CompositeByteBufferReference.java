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
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.bytes.CompositeBytesReference;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;


/**
 * A bytes reference that is composed of multiple underlying {@link ByteBufferReference} instances.
 * This buffer is mutable. So additional {@link ByteBufferReference} instances can be added to the
 * end or dropped from the beginning dropped efficiently.
 *
 * Modifications to the reader or writer indexes of this buffer will adjust the indexes of the
 * underlying buffers.
 *
 * Buffers that are added to this composite buffer must maintain contiguous readable or writable
 * regions. For example if the composite buffer is 10 bytes long with the writer index set to 9,
 * any buffer added to the end must have a writer index of 0. The same principle applies to reader
 * indexes.
 */
public class CompositeByteBufferReference extends NetworkBytesReference {

    private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.wrap(new byte[0]);

    private final ObjectArrayDeque<ByteBufferReference> references;
    private int[] offsets;

    public CompositeByteBufferReference(ByteBufferReference... newReferences) {
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

    /**
     * This method will cause all bytes up to the index to be dropped. The reader and writer
     * indexes will be modified to reflect the dropped bytes. If the reader and/or writer index is
     * less than the index passed to this method, it will be set to zero.
     *
     * @param index up to which buffers will be dropped
     */
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

    @Override
    public void incrementWrite(int delta) {
        int offsetIndex = getOffsetIndex(writeIndex);
        super.incrementWrite(delta);

        int i = delta;
        while (i != 0) {
            ByteBufferReference reference = getReference(offsetIndex++);
            int bytesToInc = Math.min(reference.getWriteRemaining(), i);
            reference.incrementWrite(bytesToInc);
            i -= bytesToInc;
        }
    }

    @Override
    public void incrementRead(int delta) {
        int offsetIndex = getOffsetIndex(readIndex);
        super.incrementRead(delta);

        int i = delta;
        while (i != 0) {
            ByteBufferReference reference = getReference(offsetIndex++);
            int bytesToInc = Math.min(reference.getReadRemaining(), i);
            reference.incrementRead(bytesToInc);
            i -= bytesToInc;
        }
    }

    @Override
    public void resetIndices() {
        for (ObjectCursor<ByteBufferReference> ref : references) {
            ref.value.resetIndices();
        }

        super.resetIndices();
    }

    @Override
    public boolean hasMultipleBuffers() {
        return references.size() > 1;
    }

    @Override
    public ByteBuffer getWriteByteBuffer() {
        if (references.isEmpty()) {
            return EMPTY_BUFFER;
        } else {
            return references.getLast().getWriteByteBuffer();
        }
    }

    @Override
    public ByteBuffer getReadByteBuffer() {
        if (references.isEmpty()) {
            return EMPTY_BUFFER;
        } else {
            return references.getLast().getReadByteBuffer();
        }
    }

    public ByteBuffer[] getWriteByteBuffers() {
        if (hasWriteRemaining() == false) {
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
        if (hasReadRemaining() == false) {
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
    public NetworkBytesReference slice(int from, int length) {
        // for slices we only need to find the start and the end reference
        // adjust them and pass on the references in between as they are fully contained
        final int to = from + length;
        final int limit = getOffsetIndex(from + length);
        final int start = getOffsetIndex(from);
        final ByteBufferReference[] inSlice = new ByteBufferReference[1 + (limit - start)];
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
        return new CompositeByteBufferReference(inSlice);
    }

    @Override
    public BytesRef toBytesRef() {
        return CompositeBytesReference.compositeReferenceToBytesRef(this);
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
        // TODO: Consider always tracking
        int totalRamBytes = 0;
        for (ObjectCursor<ByteBufferReference> ref :references) {
            totalRamBytes += ref.value.ramBytesUsed();
        }
        return totalRamBytes;
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
