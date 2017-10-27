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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.BytesReferenceStreamInput;
import org.elasticsearch.common.collect.IndexedArrayDeque;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.lease.Releasable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.StreamSupport;

public class ChannelBuffer implements Iterable<CloseableHeapBytes> {

    private final IndexedArrayDeque<CloseableHeapBytes> references;
    private int[] offsets;

    private int length;
    private int index;

    public ChannelBuffer(CloseableHeapBytes... newReferences) {
        this.references = new IndexedArrayDeque<>(Math.max(8, newReferences.length));
        this.offsets = new int[0];
        this.length = 0;

        addBuffers(newReferences);
    }

    public void addBuffer(CloseableHeapBytes newReference) {
        addBuffers(newReference);
    }

    public void addBuffers(CloseableHeapBytes... refs) {
        int initialReferenceCount = references.size();
        int[] newOffsets = new int[offsets.length + refs.length];
        System.arraycopy(offsets, 0, newOffsets, 0, offsets.length);

        try {
            int i = refs.length;
            for (CloseableHeapBytes ref : refs) {
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
        if (messageLength > length) {
            throw new IllegalArgumentException("can't slice off a message with a length [" + messageLength + "] that is greater " +
                "than the length [" + length + "] of the buffer");
        }

        int offsetIndex = getOffsetIndex(messageLength);
        int bytesDropped = 0;

        int messageBytesInFinalBuffer = messageLength - offsets[offsetIndex];
        BytesReference[] messageReferences;
        Releasable[] closeables;
        if (messageBytesInFinalBuffer == 0) {
            messageReferences = new BytesReference[offsetIndex];
            closeables = new Releasable[offsetIndex];
        } else {
            messageReferences = new BytesReference[offsetIndex + 1];
            closeables = new Releasable[offsetIndex + 1];
        }
        for (int i = 0; i < offsetIndex; ++i) {
            CloseableHeapBytes removed = references.removeFirst();
            messageReferences[i] = removed;
            closeables[i] = removed;
            int bytesOfRemoved = removed.length();
            bytesDropped += bytesOfRemoved;
        }

        if (messageBytesInFinalBuffer != 0) {
            CloseableHeapBytes first = references.getFirst();
            if (messageBytesInFinalBuffer == first.length()) {
                references.removeFirst();
                messageReferences[offsetIndex] = first;
                closeables[offsetIndex] = first;
            } else {
                messageReferences[offsetIndex] = first.slice(0, messageBytesInFinalBuffer);
                closeables[offsetIndex] = () -> {};
            }
            bytesDropped += messageBytesInFinalBuffer;
        }

        this.index = Math.max(0, index - bytesDropped);
        this.length -= bytesDropped;

        this.offsets = new int[references.size()];
        int currentOffset = 0;
        int i = 0;
        for (ObjectCursor<CloseableHeapBytes> reference : references) {
            offsets[i++] = currentOffset;
            currentOffset += reference.value.length();
        }
        return new ChannelMessage(messageReferences, closeables);
    }

    public CloseableHeapBytes removeFirst() {
        CloseableHeapBytes reference = references.removeFirst();
        int bytesDropped = reference.length();
        this.length -= bytesDropped;
        this.index = Math.max(0, index - bytesDropped);
        return reference;
    }

    public int getIndex() {
        return index;
    }

    public void incrementIndex(int delta) {
        int newIndex = index + delta;
        if (newIndex > length) {
            throw new IndexOutOfBoundsException("New index [" + newIndex + "] would be greater than length" +
                " [" + length + "]");
        }

        index = newIndex;
    }

    public int getRemaining() {
        return length - index;
    }

    public boolean hasRemaining() {
        return getRemaining() != 0;
    }

    public boolean isComposite() {
        return references.size() > 1;
    }

    public ByteBuffer[] postIndexByteBuffers() {
        if (hasRemaining() == false) {
            return new ByteBuffer[0];
        }

        int offsetIndex = getOffsetIndex(index);

        int refCount = references.size();
        ByteBuffer[] buffers = new ByteBuffer[refCount - offsetIndex];

        int j = 0;
        int bytesForFirstRef = offsets[offsetIndex + 1] - index;
        BytesRef firstRef = references.get(offsetIndex).toBytesRef();
        buffers[j++] = ByteBuffer.wrap(firstRef.bytes, firstRef.offset + (firstRef.length - bytesForFirstRef), bytesForFirstRef);
        for (int i = offsetIndex + 1; i < refCount; ++i) {
            BytesRef ref = references.get(i).toBytesRef();
            buffers[j++] = ByteBuffer.wrap(ref.bytes, ref.offset, ref.length);
        }

        return buffers;
    }

    public ByteBuffer[] preIndexByteBuffers() {
        if (index == 0) {
            return new ByteBuffer[0];
        }

        int offsetIndex = getOffsetIndex(index);

        ByteBuffer[] buffers = new ByteBuffer[offsetIndex];

        int j = 0;
        for (int i = 0; i < (offsetIndex - 1); ++i) {
            BytesRef ref = references.get(i).toBytesRef();
            buffers[j++] = ByteBuffer.wrap(ref.bytes, ref.offset, ref.length);
        }
        int bytesForLastRef = index - offsets[offsetIndex];
        BytesRef lastRef = references.get(offsetIndex).toBytesRef();
        buffers[j++] = ByteBuffer.wrap(lastRef.bytes, lastRef.offset + bytesForLastRef, bytesForLastRef);

        return buffers;
    }

    public ByteBuffer postIndexByteBuffer() {
        assert references.size() == 1 : "multiple buffers";
        BytesRef last = references.getLast().toBytesRef();
        return ByteBuffer.wrap(last.bytes, last.offset + index, last.length - index);
    }

    public ByteBuffer preIndexByteBuffer() {
        assert references.size() == 1: "multiple buffers";
        BytesRef last = references.getLast().toBytesRef();
        return ByteBuffer.wrap(last.bytes, last.offset, index);
    }

    public void close() {
        for (ObjectCursor<CloseableHeapBytes> reference : references) {
            reference.value.close();
        }
    }

    public Iterator<CloseableHeapBytes> iterator() {
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

    private void addBuffer0(CloseableHeapBytes ref, int[] newOffsetArray, int offsetIndex) {
        newOffsetArray[offsetIndex] = length;
        this.references.addLast(ref);
        this.length += ref.length();
    }

    private void removeAddedReferences(int initialReferenceCount) {
        int refsToDrop = references.size() - initialReferenceCount;
        for (int i = 0; i < refsToDrop; ++i) {
            CloseableHeapBytes reference = references.removeLast();
            this.length -= reference.length();
        }
    }

    private void validateReadAndWritesIndexes(int refIndex) {
        if (index != length && refIndex != 0) {
            throw new IllegalArgumentException("The writable spaces must be contiguous across buffers.");
        }
    }

    public StreamInput streamInput() throws IOException {
        Iterator<CloseableHeapBytes> refIterator = iterator();
        return new BytesReferenceStreamInput(() -> {
            if (refIterator.hasNext()) {
                return refIterator.next().toBytesRef();
            } else {
                return null;
            }
        }, length);
    }
}
