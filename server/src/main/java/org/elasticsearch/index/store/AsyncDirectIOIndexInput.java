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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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
        this.prefetcher = new DirectIOPrefetcher(bufferSize, maxPrefetches, maxPrefetches * 64);
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
        final long absPos = pos + offset;
        final long alignedPos = absPos - (absPos % blockSize);
        final int delta = (int) (absPos - alignedPos);
        prefetcher.prefetch(alignedPos, length + delta);
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
        refill(delta, delta);
    }

    private void refill(int bytesToRead) throws IOException {
        refill(bytesToRead, 0);
    }

    private void refill(int bytesToRead, int delta) throws IOException {
        filePos += buffer.capacity();
        // BaseDirectoryTestCase#testSeekPastEOF test for consecutive read past EOF,
        // hence throwing EOFException early to maintain buffer state (position in particular)
        if (filePos > offset + length || ((offset + length) - filePos < bytesToRead)) {
            throw new EOFException("read past EOF: " + this);
        }
        buffer.clear();
        try {
            if (prefetcher.readBytes(filePos, buffer, delta)) {
                // successfully read from prefetch buffer
                return;
            }
            // read may return -1 here iff filePos == channel.size(), but that's ok as it just reaches
            // EOF
            // when filePos > channel.size(), an EOFException will be thrown from above
            // we failed, log stacktrace to figure out why
            channel.read(buffer, filePos);
        } catch (IOException ioe) {
            throw new IOException(ioe.getMessage() + ": " + this, ioe);
        }
        buffer.flip();
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
            return new AsyncDirectIOIndexInput("clone:" + this, this, offset, length);
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        if ((length | offset) < 0 || length > this.length - offset) {
            throw new IllegalArgumentException("slice() " + sliceDescription + " out of bounds: " + this);
        }
        return new AsyncDirectIOIndexInput(sliceDescription, this, this.offset + offset, length);
    }

    record PrefetchReq(long pos, long length) {}

    private class DirectIOPrefetcher implements Closeable {
        private final int maxConcurrentPrefetches;
        private final int maxTotalPrefetches;
        private final long[] prefetchPos;
        private final Future<?>[] prefetchThreads;
        private final TreeMap<Long, Integer> posToSlot;
        private final IntArrayDeque slots;
        private final ByteBuffer[] prefetchBuffers;
        private final IOException[] prefetchExceptions;
        private final int prefetchBytesSize;
        private final Deque<PrefetchReq> pendingPrefetches = new ArrayDeque<>();
        private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

        DirectIOPrefetcher(int prefetchBytesSize, int maxConcurrentPrefetches, int maxTotalPrefetches) {
            this.maxConcurrentPrefetches = maxConcurrentPrefetches;
            this.prefetchPos = new long[maxConcurrentPrefetches];
            this.prefetchThreads = new Future<?>[maxConcurrentPrefetches];
            this.posToSlot = new TreeMap<>();
            this.slots = new IntArrayDeque(maxConcurrentPrefetches);
            for (int i = 0; i < maxConcurrentPrefetches; i++) {
                slots.addLast(i);
            }
            this.prefetchExceptions = new IOException[maxConcurrentPrefetches];
            this.prefetchBuffers = new ByteBuffer[maxConcurrentPrefetches];
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
            if (length > buffer.capacity()) {
                throw new IllegalArgumentException(
                    "Invalid prefetch length: length="
                        + length
                        + ", maxLength="
                        + buffer.capacity()
                        + ", pos="
                        + pos
                        + ", fileLength="
                        + channel.size()
                );
            }
            // we must iterate and chop up the pos and length if length exceeds maxPrefetchBytesSize
            // we should just queue up the prefetch requests if we are at maxConcurrentPrefetches
            final int slot;
            if (this.posToSlot.size() < maxConcurrentPrefetches && slots.size() > 0) {
                slot = slots.removeFirst();
                posToSlot.put(pos, slot);
                prefetchPos[slot] = pos;
            } else {
                slot = -1;
                pendingPrefetches.addLast(new PrefetchReq(pos, length));
            }
            if (slot != -1) {
                startPrefetch(pos, slot);
            }
        }

        boolean readBytes(long pos, ByteBuffer slice, int delta) throws IOException {
            final var entry = this.posToSlot.floorEntry(pos);
            if (entry == null) {
                return false;
            }
            final int slot = entry.getValue();
            final long prefetchedPos = entry.getKey();
            // determine if the requested pos is within the prefetched range
            if (pos >= this.prefetchPos[slot] + prefetchBytesSize) {
                return false;
            }
            final Future<?> thread = prefetchThreads[slot];
            if (thread == null) {
                // free slot and decrement active prefetches
                clearSlotAndMaybeStartPending(slot);
                return false;
            }
            try {
                thread.get();
            } catch (ExecutionException | InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted while waiting for prefetch", e);
            }
            if (prefetchExceptions[slot] != null) {
                IOException e = prefetchExceptions[slot];
                clearSlotAndMaybeStartPending(slot);
                throw e;
            }
            if (prefetchBuffers[slot] == null) {
                clearSlotAndMaybeStartPending(slot);
                return false;
            }
            // our buffer sizes are uniform, and match the required buffer size, however, the position here
            // might be before the requested pos, so offset it
            slice.put(prefetchBuffers[slot]);
            slice.position(Math.toIntExact(pos - prefetchedPos) + delta);
            clearSlotAndMaybeStartPending(slot);
            return true;
        }

        void clearSlotAndMaybeStartPending(int slot) {
            final PrefetchReq req;
            prefetchExceptions[slot] = null;
            prefetchThreads[slot] = null;
            posToSlot.remove(prefetchPos[slot]);
            req = pendingPrefetches.pollFirst();
            if (req == null) {
                slots.addLast(slot);
                return;
            }
            posToSlot.put(req.pos, slot);
            startPrefetch(req.pos, slot);
        }

        void startPrefetch(long pos, int slot) {
            prefetchExceptions[slot] = null;
            Future<?> future = executor.submit(() -> {
                try {
                    ByteBuffer prefetchBuffer = this.prefetchBuffers[slot];
                    if (prefetchBuffer == null) {
                        prefetchBuffer = allocateBuffer(prefetchBytesSize, blockSize);
                        this.prefetchBuffers[slot] = prefetchBuffer;
                    } else {
                        prefetchBuffer.clear();
                    }
                    channel.read(prefetchBuffer, pos);
                    prefetchBuffer.flip();
                } catch (IOException e) {
                    prefetchExceptions[slot] = e;
                }
            });
            prefetchThreads[slot] = future;
        }

        @Override
        public void close() throws IOException {
            executor.shutdownNow();
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
