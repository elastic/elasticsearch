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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.StreamSupport;

public class ChannelBuffer implements NetworkBytes, Iterable<NetworkBytesReference2> {

    private final ObjectArrayDeque<NetworkBytesReference2> references;
    private int[] offsets;

    private int length;
    private int writeIndex;
    private int readIndex;

    public ChannelBuffer(NetworkBytesReference2... newReferences) {
        this.references = new ObjectArrayDeque<>(Math.max(8, newReferences.length));
        this.offsets = new int[0];
        this.length = 0;

        addBuffers(newReferences);
    }

    public void addBuffer(NetworkBytesReference2 newReference) {
        addBuffers(newReference);
    }

    public void addBuffers(NetworkBytesReference2... refs) {
        int initialReferenceCount = references.size();
        int[] newOffsets = new int[offsets.length + refs.length];
        System.arraycopy(offsets, 0, newOffsets, 0, offsets.length);

        try {
            int i = refs.length;
            for (NetworkBytesReference2 ref : refs) {
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
    public BytesReference sliceOffMessage(int index) {
        int offsetIndex = getOffsetIndex(index);
        int bytesDropped = 0;
        // TODO: need to add partial old message
        BytesReference[] messageReferences = new BytesReference[offsetIndex + 1]; //TODO: Check + 1
        for (int i = 0; i < offsetIndex; ++i) {
            NetworkBytesReference2 removed = references.removeFirst();
            messageReferences[i] = removed;
            int bytesOfRemoved = removed.length();
            bytesDropped += bytesOfRemoved;
        }

        int bytesToDropFromSplitBuffer = index - offsets[offsetIndex];
        if (bytesToDropFromSplitBuffer != 0) {
            NetworkBytesReference2 first = references.removeFirst();
            NetworkBytesReference2 newRef = first.sliceAndRetain(bytesToDropFromSplitBuffer, first.length() - bytesToDropFromSplitBuffer);
            references.addFirst(newRef);
            bytesDropped += bytesToDropFromSplitBuffer;
        }

        this.writeIndex = Math.max(0, writeIndex - bytesDropped);
        this.readIndex = Math.max(0, readIndex - bytesDropped);
        this.length -= bytesDropped;

        this.offsets = new int[references.size()];
        int currentOffset = 0;
        int i = 0;
        for (ObjectCursor<NetworkBytesReference2> reference : references) {
            offsets[i++] = currentOffset;
            currentOffset += reference.value.length();
        }
        return new CompositeBytesReference(messageReferences);
    }

    @Override
    public int getWriteIndex() {
        return writeIndex;
    }

    @Override
    public void incrementWrite(int delta) {
        int offsetIndex = getOffsetIndex(writeIndex);

        int newWriteIndex = writeIndex + delta;
        NetworkBytes.validateWriteIndex(newWriteIndex, length);

        writeIndex = newWriteIndex;

        int i = delta;
        while (i != 0) {
            NetworkBytesReference2 reference = getReference(offsetIndex++);
            int bytesToInc = Math.min(reference.getWriteRemaining(), i);
            reference.incrementWrite(bytesToInc);
            i -= bytesToInc;
        }
    }

    @Override
    public int getWriteRemaining() {
        return length - writeIndex;
    }

    @Override
    public boolean hasWriteRemaining() {
        return getWriteRemaining() > 0;
    }

    @Override
    public int getReadIndex() {
        return readIndex;
    }

    @Override
    public void incrementRead(int delta) {
        int offsetIndex = getOffsetIndex(readIndex);

        int newReadIndex = readIndex + delta;
        NetworkBytes.validateReadIndex(newReadIndex, writeIndex);

        readIndex = newReadIndex;

        int i = delta;
        while (i != 0) {
            NetworkBytesReference2 reference = getReference(offsetIndex++);
            int bytesToInc = Math.min(reference.getReadRemaining(), i);
            reference.incrementRead(bytesToInc);
            i -= bytesToInc;
        }
    }

    @Override
    public int getReadRemaining() {
        return writeIndex - readIndex;
    }

    @Override
    public boolean hasReadRemaining() {
        return getReadRemaining() > 0;
    }

    @Override
    public boolean isCompositeBuffer() {
        return references.size() > 1;
    }

    @Override
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

    @Override
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
    public ByteBuffer getWriteByteBuffer() {
        return references.getLast().getWriteByteBuffer();
    }

    @Override
    public ByteBuffer getReadByteBuffer() {
        return references.getLast().getReadByteBuffer();
    }

    @Override
    public void close() {
        for (ObjectCursor<NetworkBytesReference2> reference : references) {
            reference.value.close();
        }
    }

    @Override
    public Iterator<NetworkBytesReference2> iterator() {
        return StreamSupport.stream(references.spliterator(), false).map(o -> o.value).iterator();
    }

    public byte get(int index) {
        final int i = getOffsetIndex(index);
        return getReference(i).get(index - offsets[i]);
    }

    public int length() {
        return length;
    }

    private int getOffsetIndex(int offset) {
        final int i = Arrays.binarySearch(offsets, offset);
        return i < 0 ? (-(i + 1)) - 1 : i;
    }

    private NetworkBytesReference2 getReference(int index) {
        Object[] rawBuffer = references.buffer;
        int actualIndex = (references.head + index) % rawBuffer.length;
        return (NetworkBytesReference2) rawBuffer[actualIndex];
    }

    private void addBuffer0(NetworkBytesReference2 ref, int[] newOffsetArray, int offsetIndex) {
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
            NetworkBytesReference2 reference = references.removeLast();
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
