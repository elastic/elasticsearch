/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.SetOnce;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.Objects;
import java.util.TreeMap;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

public class AsyncDirectIOIndexInput extends IndexInput {
    /**
     * see lucene
     */
    static final OpenOption ExtendedOpenOption_DIRECT; // visible for test

    static {
        OpenOption option;
        try {
            final Class<? extends OpenOption> clazz = Class.forName("com.sun.nio.file.ExtendedOpenOption").asSubclass(OpenOption.class);
            option = Arrays.stream(clazz.getEnumConstants()).filter(e -> e.toString().equalsIgnoreCase("DIRECT")).findFirst().orElse(null);
        } catch (Exception ex) {
            option = null;
        }
        ExtendedOpenOption_DIRECT = option;
    }

    static OpenOption getDirectOpenOption() {
        if (ExtendedOpenOption_DIRECT == null) {
            throw new UnsupportedOperationException(
                "com.sun.nio.file.ExtendedOpenOption.DIRECT is not available in the current JDK version."
            );
        }
        return ExtendedOpenOption_DIRECT;
    }

    private final int activePrefetches = 0;
    private final int nextSlot = 0;
    private final DirectIOPrefetcher prefetcher;
    private final ByteBuffer buffer;
    private final FileChannel channel;
    private final int blockSize;
    private final long offset;
    private final long length;
    private final boolean isClosable; // clones and slices are not closable
    private boolean isOpen;
    private long filePos;

    /**
     * Creates a new instance of DirectIOIndexInput for reading index input with direct IO bypassing
     * OS buffer
     *
     * @throws UnsupportedOperationException if the JDK does not support Direct I/O
     * @throws IOException if the operating system or filesystem does not support Direct I/O
     *     or a sufficient equivalent.
     */
    public AsyncDirectIOIndexInput(Path path, int blockSize, int bufferSize, int maxPrefetches) throws IOException {
        super("DirectIOIndexInput(path=\"" + path + "\")");
        this.channel = FileChannel.open(path, StandardOpenOption.READ, getDirectOpenOption());
        this.blockSize = blockSize;
        this.prefetcher = new DirectIOPrefetcher(bufferSize, maxPrefetches, maxPrefetches * 8);
        this.buffer = allocateBuffer(bufferSize, blockSize);
        this.isOpen = true;
        this.isClosable = true;
        this.length = channel.size();
        this.offset = 0L;
        this.filePos = -bufferSize;
        this.buffer.limit(0);
    }

    // for clone/slice
    private AsyncDirectIOIndexInput(String description, AsyncDirectIOIndexInput other, long offset, long length) throws IOException {
        super(description);
        Objects.checkFromIndexSize(offset, length, other.channel.size());
        final int bufferSize = other.buffer.capacity();
        this.buffer = allocateBuffer(bufferSize, other.blockSize);
        this.blockSize = other.blockSize;
        this.prefetcher = new DirectIOPrefetcher(bufferSize, other.prefetcher.maxConcurrentPrefetches, other.prefetcher.maxTotalPrefetches);
        this.channel = other.channel;
        this.isOpen = true;
        this.isClosable = false;
        this.length = length;
        this.offset = offset;
        this.filePos = -bufferSize;
        buffer.limit(0);
    }

    private static ByteBuffer allocateBuffer(int bufferSize, int blockSize) {
        return ByteBuffer.allocateDirect(bufferSize + blockSize - 1).alignedSlice(blockSize).order(LITTLE_ENDIAN);
    }

    @Override
    public void prefetch(long pos, long length) throws IOException {
        if (pos < 0 || length < 0 || pos + length > this.length) {
            throw new IllegalArgumentException("Invalid prefetch range: pos=" + pos + ", length=" + length + ", fileLength=" + this.length);
        }
        // check if our current buffer already contains the requested range
        final long absolutePos = pos + offset;
        if (absolutePos >= filePos && absolutePos <= filePos + buffer.limit() && absolutePos + length <= filePos + buffer.limit()) {
            // the new position is within the existing buffer
            return;
        }
        prefetcher.prefetch(absolutePos, length);
    }

    @Override
    public void close() throws IOException {
        prefetcher.close();
        if (isOpen && isClosable) {
            channel.close();
            isOpen = false;
        }
    }

    @Override
    public long getFilePointer() {
        long filePointer = filePos + buffer.position() - offset;

        // opening the input and immediately calling getFilePointer without calling readX (and thus
        // refill) first,
        // will result in negative value equal to bufferSize being returned,
        // due to the initialization method filePos = -bufferSize used in constructor.
        assert filePointer == -buffer.capacity() - offset || filePointer >= 0
            : "filePointer should either be initial value equal to negative buffer capacity, or larger than or equal to 0";
        return Math.max(filePointer, 0);
    }

    @Override
    public void seek(long pos) throws IOException {
        if (pos != getFilePointer()) {
            final long absolutePos = pos + offset;
            if (absolutePos >= filePos && absolutePos <= filePos + buffer.limit()) {
                // the new position is within the existing buffer
                buffer.position(Math.toIntExact(absolutePos - filePos));
            } else {
                seekInternal(pos); // do an actual seek/read
            }
        }
        assert pos == getFilePointer();
    }

    private void seekInternal(long pos) throws IOException {
        final long absPos = pos + offset;
        final long alignedPos = absPos - (absPos % blockSize);
        filePos = alignedPos - buffer.capacity();

        final int delta = (int) (absPos - alignedPos);
        refill(delta);
        buffer.position(delta);
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public byte readByte() throws IOException {
        if (buffer.hasRemaining() == false) {
            refill(1);
        }

        return buffer.get();
    }

    @Override
    public short readShort() throws IOException {
        if (buffer.remaining() >= Short.BYTES) {
            return buffer.getShort();
        } else {
            return super.readShort();
        }
    }

    @Override
    public int readInt() throws IOException {
        if (buffer.remaining() >= Integer.BYTES) {
            return buffer.getInt();
        } else {
            return super.readInt();
        }
    }

    @Override
    public long readLong() throws IOException {
        if (buffer.remaining() >= Long.BYTES) {
            return buffer.getLong();
        } else {
            return super.readLong();
        }
    }

    private void refill(int bytesToRead) throws IOException {
        filePos += buffer.capacity();
        // BaseDirectoryTestCase#testSeekPastEOF test for consecutive read past EOF,
        // hence throwing EOFException early to maintain buffer state (position in particular)
        if (filePos > offset + length || ((offset + length) - filePos < bytesToRead)) {
            throw new EOFException("read past EOF: " + this);
        }
        buffer.clear();
        try {
            if (prefetcher.readBytes(filePos, buffer)) {
                // successfully read from prefetch buffer
                return;
            }
            // read may return -1 here iff filePos == channel.size(), but that's ok as it just reaches
            // EOF
            // when filePos > channel.size(), an EOFException will be thrown from above
            channel.read(buffer, filePos);
        } catch (IOException ioe) {
            throw new IOException(ioe.getMessage() + ": " + this, ioe);
        }

        buffer.flip();
    }

    @Override
    public void readBytes(byte[] dst, int offset, int len) throws IOException {
        int toRead = len;
        while (true) {
            final int left = buffer.remaining();
            if (left < toRead) {
                buffer.get(dst, offset, left);
                toRead -= left;
                offset += left;
                refill(toRead);
            } else {
                buffer.get(dst, offset, toRead);
                break;
            }
        }
    }

    @Override
    public void readInts(int[] dst, int offset, int len) throws IOException {
        int remainingDst = len;
        while (remainingDst > 0) {
            int cnt = Math.min(buffer.remaining() / Integer.BYTES, remainingDst);
            buffer.asIntBuffer().get(dst, offset + len - remainingDst, cnt);
            buffer.position(buffer.position() + Integer.BYTES * cnt);
            remainingDst -= cnt;
            if (remainingDst > 0) {
                if (buffer.hasRemaining()) {
                    dst[offset + len - remainingDst] = readInt();
                    --remainingDst;
                } else {
                    refill(remainingDst * Integer.BYTES);
                }
            }
        }
    }

    @Override
    public void readFloats(float[] dst, int offset, int len) throws IOException {
        int remainingDst = len;
        while (remainingDst > 0) {
            int cnt = Math.min(buffer.remaining() / Float.BYTES, remainingDst);
            buffer.asFloatBuffer().get(dst, offset + len - remainingDst, cnt);
            buffer.position(buffer.position() + Float.BYTES * cnt);
            remainingDst -= cnt;
            if (remainingDst > 0) {
                if (buffer.hasRemaining()) {
                    dst[offset + len - remainingDst] = Float.intBitsToFloat(readInt());
                    --remainingDst;
                } else {
                    refill(remainingDst * Float.BYTES);
                }
            }
        }
    }

    @Override
    public void readLongs(long[] dst, int offset, int len) throws IOException {
        int remainingDst = len;
        while (remainingDst > 0) {
            int cnt = Math.min(buffer.remaining() / Long.BYTES, remainingDst);
            buffer.asLongBuffer().get(dst, offset + len - remainingDst, cnt);
            buffer.position(buffer.position() + Long.BYTES * cnt);
            remainingDst -= cnt;
            if (remainingDst > 0) {
                if (buffer.hasRemaining()) {
                    dst[offset + len - remainingDst] = readLong();
                    --remainingDst;
                } else {
                    refill(remainingDst * Long.BYTES);
                }
            }
        }
    }

    @Override
    public AsyncDirectIOIndexInput clone() {
        try {
            var clone = new AsyncDirectIOIndexInput("clone:" + this, this, offset, length);
            clone.seekInternal(getFilePointer());
            return clone;
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        if ((length | offset) < 0 || length > this.length - offset) {
            throw new IllegalArgumentException("slice() " + sliceDescription + " out of bounds: " + this);
        }
        var slice = new AsyncDirectIOIndexInput(sliceDescription, this, this.offset + offset, length);
        slice.seekInternal(0L);
        return slice;
    }

    record ThreadOrException(Thread thread, SetOnce<IOException> exception, SetOnce<ByteBuffer> result, int delta) {}

    record PrefetchReq(long pos, long length, int delta) {}

    private class DirectIOPrefetcher implements Closeable {
        private final int maxConcurrentPrefetches;
        private final int maxTotalPrefetches;
        private final long[] prefetchPos;
        private final ThreadOrException[] prefetchThreads;
        private final TreeMap<Long, Integer> posToSlot;
        private final IntArrayDeque slots;
        private final ByteBuffer[] prefetchBuffers;
        private final int prefetchBytesSize;
        private final Deque<PrefetchReq> pendingPrefetches = new ArrayDeque<>();

        DirectIOPrefetcher(int prefetchBytesSize, int maxConcurrentPrefetches, int maxTotalPrefetches) {
            this.maxConcurrentPrefetches = maxConcurrentPrefetches;
            this.prefetchPos = new long[maxConcurrentPrefetches];
            this.prefetchThreads = new ThreadOrException[maxConcurrentPrefetches];
            this.posToSlot = new TreeMap<>();
            this.slots = new IntArrayDeque(maxConcurrentPrefetches);
            for (int i = 0; i < maxConcurrentPrefetches; i++) {
                slots.addLast(i);
            }
            this.prefetchBuffers = new ByteBuffer[maxConcurrentPrefetches];
            for (int i = 0; i < maxConcurrentPrefetches; i++) {
                prefetchBuffers[i] = allocateBuffer(prefetchBytesSize, blockSize);
                prefetchBuffers[i].limit(0);
            }
            this.prefetchBytesSize = prefetchBytesSize;
            this.maxTotalPrefetches = maxTotalPrefetches;
        }

        /**
         * Initiate prefetch of the given range. The range will be aligned to blockSize and
         * chopped up into chunks of prefetchBytesSize.
         * @param pos
         * @param length
         * @throws IOException
         */
        void prefetch(long pos, long length) throws IOException {
            if (pos < 0 || length < 0 || pos + length > channel.size()) {
                throw new IllegalArgumentException("Invalid prefetch range: pos=" + pos + ", length=" + length);
            }
            final long absPos = pos;
            final long alignedPos = absPos - (absPos % blockSize);
            final long delta = absPos - alignedPos;
            // we must iterate and chop up the pos and length if length exceeds maxPrefetchBytesSize
            // we should just queue up the prefetch requests if we are at maxConcurrentPrefetches
            long remaining = length + delta;
            long currentPos = alignedPos;
            int prefetchCount = remaining == 0 ? 0 : (int) ((remaining - 1) / prefetchBytesSize) + 1;
            synchronized (this.posToSlot) {
                prefetchCount = Math.min(prefetchCount, maxTotalPrefetches - this.posToSlot.size() - pendingPrefetches.size());
                for (int i = 0; i < prefetchCount; i++) {
                    final long chunkPos = currentPos;
                    int localDelta = (int) (currentPos == alignedPos ? delta : 0);
                    // we need to determine if there is a chunk from an earlier position that is already being fetched that is covered
                    int alreadyFetching = alreadyPrefetching(chunkPos + localDelta, remaining);
                    if (alreadyFetching > 0) {
                        // already being prefetched
                        remaining -= alreadyFetching;
                        currentPos += alreadyFetching;
                        continue;
                    }
                    if (this.posToSlot.size() < maxConcurrentPrefetches && slots.size() > 0) {
                        final int slot = slots.removeFirst();
                        posToSlot.put(chunkPos + localDelta, slot);
                        prefetchPos[slot] = chunkPos + localDelta;
                        startPrefetch(chunkPos, localDelta, slot);
                    } else {
                        pendingPrefetches.addLast(new PrefetchReq(chunkPos, remaining, localDelta));
                    }
                    remaining -= prefetchBytesSize;
                    currentPos += prefetchBytesSize;
                }
            }
        }

        int alreadyPrefetching(long alignedPos, long length) {
            int delta = 0;
            while (delta < length) {
                final Long previousFetch = this.posToSlot.floorKey(alignedPos + delta);
                if (previousFetch == null) {
                    return delta;
                }
                delta += prefetchBytesSize;
            }
            return delta;
        }

        boolean readBytes(long pos, ByteBuffer slice) throws IOException {
            final int slot;
            synchronized (this.posToSlot) {
                var entry = this.posToSlot.floorEntry(pos);
                if (entry == null) {
                    return false;
                }
                slot = entry.getValue();
                if (pos < this.prefetchPos[slot] || pos >= this.prefetchPos[slot] + prefetchBytesSize) {
                    return false;
                }
            }
            final long prefetchedPos = this.prefetchPos[slot];
            final ThreadOrException threadOrException = prefetchThreads[slot];
            if (threadOrException == null) {
                // free slot and decrement active prefetches
                clearSlotAndMaybeStartPending(slot);
                return false;
            }
            try {
                threadOrException.thread.join();
                assert threadOrException.result.get() != null || threadOrException.exception.get() != null;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted while waiting for prefetch", e);
            }
            if (threadOrException.exception.get() != null) {
                clearSlotAndMaybeStartPending(slot);
                throw threadOrException.exception.get();
            }
            // our buffer sizes are uniform, and match the required buffer size, however, the position here
            // might be before the requested pos, so offset it
            slice.put(threadOrException.result.get());
            slice.position(Math.toIntExact(pos - prefetchedPos));
            clearSlotAndMaybeStartPending(slot);
            return true;
        }

        void clearSlotAndMaybeStartPending(int slot) {
            synchronized (this.posToSlot) {
                prefetchThreads[slot] = null;
                posToSlot.remove(prefetchPos[slot]);
                if (pendingPrefetches.isEmpty()) {
                    slots.addLast(slot);
                    return;
                }
                PrefetchReq req = pendingPrefetches.removeFirst();
                int alreadyPrefetching = alreadyPrefetching(req.pos, req.length);
                long truePos = req.pos + alreadyPrefetching;
                long trueLength = req.length - alreadyPrefetching;
                if (trueLength <= 0) {
                    // already being prefetched
                    slots.addLast(slot);
                    return;
                }
                int delta = (int) (truePos - (truePos - (truePos % blockSize)));
                posToSlot.put(truePos + delta, slot);
                startPrefetch(truePos, delta, slot);
            }
        }

        void startPrefetch(long pos, int delta, int slot) {
            SetOnce<IOException> exception = new SetOnce<>();
            SetOnce<ByteBuffer> result = new SetOnce<>();
            ByteBuffer prefetchBuffer = this.prefetchBuffers[slot];
            prefetchBuffer.clear();
            Thread virtualThread = Thread.ofVirtual().name("async-io-prefetch", 0).start(() -> {
                try {
                    channel.read(prefetchBuffer, pos);
                    prefetchBuffer.flip();
                    result.set(prefetchBuffer);
                } catch (IOException e) {
                    exception.set(e);
                }
            });
            prefetchThreads[slot] = new ThreadOrException(virtualThread, exception, result, delta);
        }

        @Override
        public void close() throws IOException {
            synchronized (this.posToSlot) {
                for (final ThreadOrException threadOrException : prefetchThreads) {
                    if (threadOrException != null) {
                        try {
                            threadOrException.thread.join();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IOException("interrupted while waiting for prefetch", e);
                        }
                        if (threadOrException.exception.get() != null) {
                            throw threadOrException.exception.get();
                        }
                    }
                }
            }
        }
    }

    private static class IntArrayDeque {
        private int[] array;
        private int head = 0;
        private int tail = 0;
        private int size = 0;

        IntArrayDeque(int initialCapacity) {
            array = new int[initialCapacity];
        }

        void addLast(int value) {
            if (size == array.length) {
                resize();
            }
            array[tail] = value;
            tail = (tail + 1) % array.length;
            size++;
        }

        int removeFirst() {
            if (size == 0) {
                throw new IllegalStateException("Deque is empty");
            }
            int value = array[head];
            head = (head + 1) % array.length;
            size--;
            return value;
        }

        int size() {
            return size;
        }

        private void resize() {
            int newCapacity = array.length * 2;
            int[] newArray = new int[newCapacity];
            for (int i = 0; i < size; i++) {
                newArray[i] = array[(head + i) % array.length];
            }
            array = newArray;
            head = 0;
            tail = size;
        }
    }
}
