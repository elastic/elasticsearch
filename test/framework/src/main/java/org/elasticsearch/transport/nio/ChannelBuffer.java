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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.common.bytes.BytesReferenceStreamInput;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.collect.IndexedArrayDeque;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.StreamSupport;

public class ChannelBuffer implements NetworkBytes, Iterable<NetworkBytesReference> {

    private final IndexedArrayDeque<NetworkBytesReference> references;
    private int[] offsets;

    private int length;
    private int writeIndex;
    private int readIndex;

    public ChannelBuffer(NetworkBytesReference... newReferences) {
        this.references = new IndexedArrayDeque<>(Math.max(8, newReferences.length));
        this.offsets = new int[0];
        this.length = 0;

        addBuffers(newReferences);
    }

    public void addBuffer(NetworkBytesReference newReference) {
        addBuffers(newReference);
    }

    public void addBuffers(NetworkBytesReference... refs) {
        int initialReferenceCount = references.size();
        int[] newOffsets = new int[offsets.length + refs.length];
        System.arraycopy(offsets, 0, newOffsets, 0, offsets.length);

        try {
            int i = refs.length;
            for (NetworkBytesReference ref : refs) {
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
     * @param messageLength up to which buffers will be dropped
     */
    public ChannelMessage sliceOffMessage(int messageLength) {
        int offsetIndex = getOffsetIndex(messageLength);
        int bytesDropped = 0;

        int messageBytesInFinalBuffer = messageLength - offsets[offsetIndex];
        NetworkBytesReference[] messageReferences;
        if (messageBytesInFinalBuffer == 0) {
            messageReferences = new NetworkBytesReference[offsetIndex];
        } else {
            messageReferences = new NetworkBytesReference[offsetIndex + 1];
        }
        for (int i = 0; i < offsetIndex; ++i) {
            NetworkBytesReference removed = references.removeFirst();
            messageReferences[i] = removed;
            int bytesOfRemoved = removed.length();
            bytesDropped += bytesOfRemoved;
        }

        if (messageBytesInFinalBuffer != 0) {
            NetworkBytesReference first = references.removeFirst();
            messageReferences[offsetIndex] = first.sliceAndRetain(0, messageBytesInFinalBuffer);
            NetworkBytesReference newRef = first.sliceAndRetain(messageBytesInFinalBuffer, first.length() - messageBytesInFinalBuffer);
            first.close();
            references.addFirst(newRef);
            bytesDropped += messageBytesInFinalBuffer;
        }

        this.writeIndex = Math.max(0, writeIndex - bytesDropped);
        this.readIndex = Math.max(0, readIndex - bytesDropped);
        this.length -= bytesDropped;

        this.offsets = new int[references.size()];
        int currentOffset = 0;
        int i = 0;
        for (ObjectCursor<NetworkBytesReference> reference : references) {
            offsets[i++] = currentOffset;
            currentOffset += reference.value.length();
        }
        return new ChannelMessage(new CompositeBytesReference(messageReferences), messageReferences);
    }

    public NetworkBytesReference peek() {
        if (references.isEmpty()) {
            return null;
        }
        return references.getFirst();
    }

    public NetworkBytesReference removeFirst() {
        NetworkBytesReference reference = references.removeFirst();
        int bytesDropped = reference.length();
        this.length -= bytesDropped;
        this.writeIndex = Math.max(0, writeIndex - bytesDropped);
        this.readIndex = Math.max(0, readIndex - bytesDropped);
        return reference;
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
            NetworkBytesReference reference = references.get(offsetIndex++);
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
            NetworkBytesReference reference = references.get(offsetIndex++);
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
            buffers[j++] = references.get(i).getWriteByteBuffer();
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
            buffers[j++] = references.get(i).getReadByteBuffer();
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
        for (ObjectCursor<NetworkBytesReference> reference : references) {
            reference.value.close();
        }
    }

    @Override
    public Iterator<NetworkBytesReference> iterator() {
        return StreamSupport.stream(references.spliterator(), false).map(o -> o.value).iterator();
    }

    public byte get(int index) {
        final int i = getOffsetIndex(index);
        return references.get(i).get(index - offsets[i]);
    }

    public int length() {
        return length;
    }

    int getOffsetIndex(int offset) {
        final int i = Arrays.binarySearch(offsets, offset);
        return i < 0 ? (-(i + 1)) - 1 : i;
    }

    private void addBuffer0(NetworkBytesReference ref, int[] newOffsetArray, int offsetIndex) {
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
            NetworkBytesReference reference = references.removeLast();
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

    public StreamInput streamInput() throws IOException {
        Iterator<NetworkBytesReference> refIterator = iterator();
        return new BytesReferenceStreamInput(() -> {
            if (refIterator.hasNext()) {
                return refIterator.next().toBytesRef();
            } else {
                return null;
            }
        }, length);
    }
}
