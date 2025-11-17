/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2025 Elasticsearch B.V.
 */

package org.elasticsearch.index.store;

import com.carrotsearch.hppc.IntArrayDeque;
import com.carrotsearch.hppc.IntDeque;

import org.apache.lucene.store.IndexInput;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

/**
 * An implementation of {@link IndexInput} that uses Direct I/O to bypass OS cache and
 * provides asynchronous prefetching of data.
 */
public class AsyncDirectIOIndexInput extends IndexInput {

    private static final Logger LOGGER = LogManager.getLogger(AsyncDirectIOIndexInput.class);

    /**
     * Copied from Lucene
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

    @SuppressForbidden(reason = "requires FileChannel#read")
    private static void readDirectChannel(FileChannel c, ByteBuffer bb, long p) throws IOException {
        c.read(bb, p);
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
     * Creates a new instance of AsyncDirectIOIndexInput for reading index input with direct IO bypassing
     * OS buffer
     *
     * @param path the path to the file to read
        * @param blockSize the block size to use for alignment. This must match the filesystem
        *                  block size, otherwise an IOException will be thrown.
     * @param bufferSize the size of the read buffer. This must be a multiple of blockSize.
     * @param maxPrefetches the maximum number of concurrent prefetches to allow.
     *                      This also determines the maximum number of total prefetches that can be
     *                      outstanding. The total number of prefetches is maxPrefetches * 16.
     *                      A larger number of maxPrefetches allows for more aggressive prefetching,
     *                      but also uses more memory (maxPrefetches * bufferSize).
     * @throws UnsupportedOperationException if the JDK does not support Direct I/O
     * @throws IOException if the operating system or filesystem does not support Direct I/O
     *     or a sufficient equivalent.
     */
    public AsyncDirectIOIndexInput(Path path, int blockSize, int bufferSize, int maxPrefetches) throws IOException {
        super("DirectIOIndexInput(path=\"" + path + "\")");
        this.channel = FileChannel.open(path, StandardOpenOption.READ, getDirectOpenOption());
        this.blockSize = blockSize;
        this.prefetcher = new DirectIOPrefetcher(blockSize, this.channel, bufferSize, maxPrefetches);
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
        this.channel = other.channel;
        this.prefetcher = new DirectIOPrefetcher(this.blockSize, this.channel, bufferSize, other.prefetcher.maxConcurrentPrefetches);
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

    /**
     * Prefetches the given range of bytes. The range will be aligned to blockSize and will
     * be chopped up into chunks of buffer size.
     * @param pos the position to prefetch from, must be non-negative and within file length
     * @param length the length to prefetch, must be non-negative. This length may cause multiple
     *               prefetches to be issued, depending on the buffer size.
     */
    @Override
    public void prefetch(long pos, long length) throws IOException {
        if (pos < 0 || length < 0 || pos + length > this.length) {
            throw new IllegalArgumentException("Invalid prefetch range: pos=" + pos + ", length=" + length + ", fileLength=" + this.length);
        }

        // align to prefetch buffer
        final long absPos = pos + offset;
        long alignedPos = absPos - absPos % blockSize;

        // check if our current buffer already contains the requested range
        if (alignedPos >= filePos && alignedPos < filePos + buffer.capacity()) {
            // The current buffer contains bytes of this request.
            // Adjust the position and length accordingly to skip the current buffer.
            alignedPos = filePos + buffer.capacity();
            length -= alignedPos - absPos;
        } else {
            // Add to the total length the bytes added by the alignment
            length += absPos - alignedPos;
        }
        // do the prefetch
        prefetcher.prefetch(alignedPos, length);
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
        assert filePointer == -buffer.capacity() - offset || filePointer >= 0
            : "filePointer should either be initial value equal to negative buffer capacity, or larger than or equal to 0";
        return Math.max(filePointer, 0);
    }

    @Override
    public void seek(long pos) throws IOException {
        if (pos != getFilePointer()) {
            final long absolutePos = pos + offset;
            if (absolutePos >= filePos && absolutePos < filePos + buffer.limit()) {
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
        assert filePos % blockSize == 0;
        refill(bytesToRead, 0);
    }

    private void refill(int bytesToRead, int delta) throws IOException {
        long nextFilePos = filePos + buffer.capacity();
        // BaseDirectoryTestCase#testSeekPastEOF test for consecutive read past EOF,
        // hence throwing EOFException early to maintain buffer state (position in particular)
        if (nextFilePos > offset + length || ((offset + length) - nextFilePos < bytesToRead)) {
            filePos = nextFilePos;
            throw new EOFException("read past EOF: " + this);
        }
        buffer.clear();
        try {
            if (prefetcher.readBytes(nextFilePos, buffer, delta)) {
                // handle potentially differently aligned prefetch buffer
                // this gets tricky as the prefetch buffer is always blockSize aligned
                // but the prefetches might be aligned on an earlier block boundary
                // so we need to adjust the filePos accordingly
                long currentLogicalPos = nextFilePos + delta;
                filePos = currentLogicalPos - buffer.position();
                return;
            }
            filePos = nextFilePos;
            // read may return -1 here iff filePos == channel.size(), but that's ok as it just reaches
            // EOF
            // when filePos > channel.size(), an EOFException will be thrown from above
            // we failed, log stacktrace to figure out why
            assert filePos % blockSize == 0 : "filePos [" + filePos + "] must be aligned to block size [" + blockSize + "]";
            readDirectChannel(channel, buffer, filePos);
            buffer.flip();
            buffer.position(delta);
        } catch (IOException ioe) {
            throw new IOException(ioe.getMessage() + ": " + this, ioe);
        }
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
            var clone = new AsyncDirectIOIndexInput("clone:" + this, this, offset, length);
            // TODO figure out how to make this async
            // https://github.com/elastic/elasticsearch/issues/136046
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
        // TODO figure out how to make this async
        // https://github.com/elastic/elasticsearch/issues/136046
        slice.seekInternal(0L);
        return slice;
    }

    // pkg private for testing
    int prefetchSlots() {
        return prefetcher.posToSlot.size();
    }

    /**
     * A simple prefetcher that uses virtual threads to prefetch data into direct byte buffers.
     */
    private static class DirectIOPrefetcher implements Closeable {
        private final int maxConcurrentPrefetches;
        private final FileChannel channel;
        private final int blockSize;
        private final long[] prefetchPos;
        // statically initialized to maxConcurrentPrefetches
        private final List<Future<ByteBuffer>> prefetchThreads;
        private final TreeMap<Long, Integer> posToSlot;
        private final IntDeque slots;
        private final ByteBuffer[] prefetchBuffers;
        private final int prefetchBytesSize;
        private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

        DirectIOPrefetcher(int blockSize, FileChannel channel, int prefetchBytesSize, int maxConcurrentPrefetches) {
            this.blockSize = blockSize;
            this.channel = channel;
            this.maxConcurrentPrefetches = maxConcurrentPrefetches;
            this.prefetchPos = new long[maxConcurrentPrefetches];
            this.prefetchThreads = new ArrayList<>(maxConcurrentPrefetches);
            this.slots = new IntArrayDeque(maxConcurrentPrefetches);
            for (int i = 0; i < maxConcurrentPrefetches; i++) {
                prefetchThreads.add(null);
                slots.addLast(i);
            }
            this.posToSlot = new TreeMap<>();
            this.prefetchBuffers = new ByteBuffer[maxConcurrentPrefetches];
            this.prefetchBytesSize = prefetchBytesSize;
        }

        /**
         * Initiate prefetch of the given range. The range will be aligned to blockSize and
         * chopped up into chunks of prefetchBytesSize.
         * If there are not enough slots available, the prefetch request will reuse the slot
         * with the lowest file pointer. If that slot is still being prefetched, the prefetch request
         * will be skipped.
         * @param pos the position to prefetch from, must be non-negative and within file length
         * @param length the length to prefetch, must be non-negative.
         */
        void prefetch(long pos, long length) {
            assert pos % blockSize == 0 : "prefetch pos [" + pos + "] must be aligned to block size [" + blockSize + "]";
            // first determine how many slots we need given the length
            while (length > 0) {
                Map.Entry<Long, Integer> floor = this.posToSlot.floorEntry(pos);
                if (floor == null || floor.getKey() + prefetchBytesSize <= pos) {
                    // check if there are any slots available. If not we will reuse the one with the
                    // lower file pointer.
                    if (slots.isEmpty()) {
                        assert this.posToSlot.size() == maxConcurrentPrefetches;
                        final int oldestSlot = posToSlot.firstEntry().getValue();
                        if (prefetchThreads.get(oldestSlot).isDone() == false) {
                            // cannot reuse oldest slot. We are over-prefetching
                            LOGGER.debug("could not prefetch pos [{}] with length [{}]", pos, length);
                            return;
                        }
                        LOGGER.debug("prefetch on reused slot with pos [{}] with length [{}]", pos, length);
                        clearSlot(oldestSlot);
                        assert slots.isEmpty() == false;
                    }
                    final int slot = slots.removeFirst();
                    posToSlot.put(pos, slot);
                    prefetchPos[slot] = pos;
                    startPrefetch(pos, slot);
                    length -= prefetchBytesSize;
                    pos += prefetchBytesSize;
                } else {
                    length -= floor.getKey() + prefetchBytesSize - pos;
                    pos = floor.getKey() + prefetchBytesSize;
                }
            }
        }

        /**
         * Try to read the requested bytes from an already prefetched buffer.
         * If the requested bytes are not in a prefetched buffer, return false.
         * @param pos the absolute position to read from
         * @param slice the buffer to read into, must be pre-sized to the required length
         * @param delta an offset into the slice buffer to start writing at
         * @return true if the requested bytes were read from a prefetched buffer, false otherwise
         * @throws IOException if an I/O error occurs
         */
        boolean readBytes(long pos, ByteBuffer slice, int delta) throws IOException {
            final var entry = this.posToSlot.floorEntry(pos + delta);
            if (entry == null) {
                return false;
            }
            final int slot = entry.getValue();
            final long prefetchedPos = entry.getKey();
            // determine if the requested pos is within the prefetched range
            if (pos + delta >= prefetchedPos + prefetchBytesSize) {
                return false;
            }
            final Future<ByteBuffer> thread = prefetchThreads.get(slot);
            ByteBuffer prefetchBuffer = null;
            try {
                prefetchBuffer = thread == null ? null : thread.get();
            } catch (ExecutionException e) {
                IOException ioException = (IOException) ExceptionsHelper.unwrap(e, IOException.class);
                if (ioException != null) {
                    throw ioException;
                }
                throw new IOException(e.getCause());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                if (prefetchBuffer == null) {
                    clearSlot(slot);
                }
            }
            if (prefetchBuffer == null) {
                return false;
            }

            // our buffer sizes are uniform, and match the required buffer size, however, the position here
            // might be before the requested pos, so offset it
            slice.put(prefetchBuffer);
            slice.flip();
            slice.position(Math.toIntExact(pos - prefetchedPos) + delta);
            clearSlot(slot);
            return true;
        }

        void clearSlot(int slot) {
            assert prefetchThreads.get(slot) != null;
            prefetchThreads.set(slot, null);
            posToSlot.remove(prefetchPos[slot]);
            slots.addLast(slot);
        }

        private boolean assertSlotsConsistent() {
            posToSlot.forEach((k, v) -> {
                if (prefetchThreads.get(v) == null) {
                    throw new AssertionError("posToSlot inconsistent: slot " + v + " for pos " + k + " has no prefetch thread");
                }
                if (prefetchPos[v] != k) {
                    throw new AssertionError("posToSlot inconsistent: slot " + v + " for pos " + k + " has prefetchPos " + prefetchPos[v]);
                }
            });
            return true;
        }

        void startPrefetch(long pos, int slot) {
            Future<ByteBuffer> future = executor.submit(() -> {
                var prefetchBuffers = DirectIOPrefetcher.this.prefetchBuffers;
                ByteBuffer prefetchBuffer = prefetchBuffers[slot];
                if (prefetchBuffer == null) {
                    prefetchBuffer = allocateBuffer(prefetchBytesSize, blockSize);
                    prefetchBuffers[slot] = prefetchBuffer;
                } else {
                    prefetchBuffer.clear();
                }
                assert pos % blockSize == 0 : "prefetch pos [" + pos + "] must be aligned to block size [" + blockSize + "]";
                readDirectChannel(channel, prefetchBuffer, pos);
                prefetchBuffer.flip();
                return prefetchBuffer;
            });
            prefetchThreads.set(slot, future);
            assert assertSlotsConsistent();
        }

        @Override
        public void close() throws IOException {
            executor.shutdownNow();
        }
    }
}
