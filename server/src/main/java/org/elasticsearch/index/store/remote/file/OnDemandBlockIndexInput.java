/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.store.remote.file;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.elasticsearch.common.util.concurrent.OpenSearchExecutors;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.lang.ref.Cleaner;
import java.util.Objects;

abstract class OnDemandBlockIndexInput extends IndexInput implements RandomAccessInput {
    private static final Logger logger = LogManager.getLogger(OnDemandBlockIndexInput.class);

    public static final String CLEANER_THREAD_NAME_PREFIX = "index-input-cleaner";

    /**
     * A single static Cleaner instance to ensure any unclosed clone of an
     * IndexInput is closed. This instance creates a single daemon thread on
     * which it performs the cleaning actions. For an already-closed IndexInput,
     * the cleaning action is a no-op. For an open IndexInput, the close action
     * will decrement a reference count.
     */
    private static final Cleaner CLEANER = Cleaner.create(OpenSearchExecutors.daemonThreadFactory(CLEANER_THREAD_NAME_PREFIX));

    /**
     * Start offset of the virtual file : non-zero in the slice case
     */
    protected final long offset;
    /**
     * Length of the virtual file, smaller than actual file size if it's a slice
     */
    protected final long length;

    /**
     * Whether this index input is a clone or otherwise the root file before slicing
     */
    protected final boolean isClone;

    /**
     * Variables used for block calculation and fetching. blockSize must be a
     * power of two, and is defined as 2^blockShiftSize. blockMask is defined
     * as blockSize - 1 and is used to calculate the offset within a block.
     */
    protected final int blockSizeShift;
    protected final int blockSize;
    protected final int blockMask;

    /**
     * ID of the current block
     */
    private int currentBlockId;

    private final BlockHolder blockHolder = new BlockHolder();

    OnDemandBlockIndexInput(Builder builder) {
        super(builder.resourceDescription);
        this.isClone = builder.isClone;
        this.offset = builder.offset;
        this.length = builder.length;
        this.blockSizeShift = builder.blockSizeShift;
        this.blockSize = builder.blockSize;
        this.blockMask = builder.blockMask;
        CLEANER.register(this, blockHolder);
    }

    /**
     * Builds the actual sliced IndexInput (may apply extra offset in subclasses).
     **/
    protected abstract OnDemandBlockIndexInput buildSlice(String sliceDescription, long offset, long length);

    /**
     * Given a blockId, fetch it's IndexInput which might be partial/split/cloned one
     * @param blockId to fetch for
     * @return fetched IndexInput
     */
    protected abstract IndexInput fetchBlock(int blockId) throws IOException;

    @Override
    public abstract OnDemandBlockIndexInput clone();

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        if (offset < 0 || length < 0 || offset + length > this.length()) {
            throw new IllegalArgumentException(
                "slice() "
                    + sliceDescription
                    + " out of bounds: offset="
                    + offset
                    + ",length="
                    + length
                    + ",fileLength="
                    + this.length()
                    + ": "
                    + this
            );
        }

        // The slice is seeked to the beginning.
        return buildSlice(sliceDescription, offset, length);
    }

    @Override
    public void close() throws IOException {
        blockHolder.close();
        currentBlockId = 0;
    }

    @Override
    public long getFilePointer() {
        if (blockHolder.block == null) return 0L;
        return currentBlockStart() + currentBlockPosition() - offset;
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public byte readByte() throws IOException {
        if (blockHolder.block == null) {
            // seek to the beginning
            seek(0);
        } else if (currentBlockPosition() >= blockSize) {
            int blockId = currentBlockId + 1;
            demandBlock(blockId);
        }
        return blockHolder.block.readByte();
    }

    @Override
    public short readShort() throws IOException {
        if (blockHolder.block != null && Short.BYTES <= (blockSize - currentBlockPosition())) {
            return blockHolder.block.readShort();
        } else {
            return super.readShort();
        }
    }

    @Override
    public int readInt() throws IOException {
        if (blockHolder.block != null && Integer.BYTES <= (blockSize - currentBlockPosition())) {
            return blockHolder.block.readInt();
        } else {
            return super.readInt();
        }
    }

    @Override
    public long readLong() throws IOException {
        if (blockHolder.block != null && Long.BYTES <= (blockSize - currentBlockPosition())) {
            return blockHolder.block.readLong();
        } else {
            return super.readLong();
        }
    }

    @Override
    public final int readVInt() throws IOException {
        if (blockHolder.block != null && 5 <= (blockSize - currentBlockPosition())) {
            return blockHolder.block.readVInt();
        } else {
            return super.readVInt();
        }
    }

    @Override
    public final long readVLong() throws IOException {
        if (blockHolder.block != null && 9 <= (blockSize - currentBlockPosition())) {
            return blockHolder.block.readVLong();
        } else {
            return super.readVLong();
        }
    }

    @Override
    public void seek(long pos) throws IOException {
        if (pos > length()) {
            throw new EOFException("read past EOF: pos=" + pos + " vs length=" + length() + ": " + this);
        }

        seekInternal(pos + offset);
    }

    @Override
    public final byte readByte(long pos) throws IOException {
        // adjust the pos if it's sliced
        pos = pos + offset;
        if (blockHolder.block != null && isInCurrentBlockRange(pos)) {
            // the block contains the byte
            return ((RandomAccessInput) blockHolder.block).readByte(getBlockOffset(pos));
        } else {
            // the block does not have the byte, seek to the pos first
            seekInternal(pos);
            // then read the byte
            return blockHolder.block.readByte();
        }
    }

    @Override
    public short readShort(long pos) throws IOException {
        // adjust the pos if it's sliced
        pos = pos + offset;
        if (blockHolder.block != null && isInCurrentBlockRange(pos, Short.BYTES)) {
            // the block contains enough data to satisfy this request
            return ((RandomAccessInput) blockHolder.block).readShort(getBlockOffset(pos));
        } else {
            // the block does not have enough data, seek to the pos first
            seekInternal(pos);
            // then read the data
            return super.readShort();
        }
    }

    @Override
    public int readInt(long pos) throws IOException {
        // adjust the pos if it's sliced
        pos = pos + offset;
        if (blockHolder.block != null && isInCurrentBlockRange(pos, Integer.BYTES)) {
            // the block contains enough data to satisfy this request
            return ((RandomAccessInput) blockHolder.block).readInt(getBlockOffset(pos));
        } else {
            // the block does not have enough data, seek to the pos first
            seekInternal(pos);
            // then read the data
            return super.readInt();
        }
    }

    @Override
    public long readLong(long pos) throws IOException {
        // adjust the pos if it's sliced
        pos = pos + offset;
        if (blockHolder.block != null && isInCurrentBlockRange(pos, Long.BYTES)) {
            // the block contains enough data to satisfy this request
            return ((RandomAccessInput) blockHolder.block).readLong(getBlockOffset(pos));
        } else {
            // the block does not have enough data, seek to the pos first
            seekInternal(pos);
            // then read the data
            return super.readLong();
        }
    }

    @Override
    public final void readBytes(byte[] b, int offset, int len) throws IOException {
        if (blockHolder.block == null) {
            // lazy seek to the beginning
            seek(0);
        }

        int available = blockSize - currentBlockPosition();
        if (len <= available) {
            // the block contains enough data to satisfy this request
            blockHolder.block.readBytes(b, offset, len);
        } else {
            // the block does not have enough data. First serve all we've got.
            if (available > 0) {
                blockHolder.block.readBytes(b, offset, available);
                offset += available;
                len -= available;
            }

            // and now, read the remaining 'len' bytes:
            // len > blocksize example: FST <init>
            while (len > 0) {
                int blockId = currentBlockId + 1;
                int toRead = Math.min(len, blockSize);
                demandBlock(blockId);
                blockHolder.block.readBytes(b, offset, toRead);
                offset += toRead;
                len -= toRead;
            }
        }

    }

    /**
     * Seek to a block position, download the block if it's necessary
     * NOTE: the pos should be an adjusted position for slices
     */
    @SuppressWarnings("checkstyle:DescendantToken")
    private void seekInternal(long pos) throws IOException {
        if (blockHolder.block == null || !isInCurrentBlockRange(pos)) {
            demandBlock(getBlock(pos));
        }
        blockHolder.block.seek(getBlockOffset(pos));
    }

    /**
     * Check if pos in current block range
     * NOTE: the pos should be an adjusted position for slices
     */
    private boolean isInCurrentBlockRange(long pos) {
        long offset = pos - currentBlockStart();
        return offset >= 0 && offset < blockSize;
    }

    /**
     * Check if [pos, pos + len) in current block range
     * NOTE: the pos should be an adjusted position for slices
     */
    private boolean isInCurrentBlockRange(long pos, int len) {
        long offset = pos - currentBlockStart();
        return offset >= 0 && (offset + len) <= blockSize;
    }

    private void demandBlock(int blockId) throws IOException {
        if (blockHolder.block != null && currentBlockId == blockId) return;

        // close the current block before jumping to the new block
        blockHolder.close();

        blockHolder.set(fetchBlock(blockId));
        currentBlockId = blockId;
    }

    protected void cloneBlock(OnDemandBlockIndexInput other) {
        if (other.blockHolder.block != null) {
            this.blockHolder.set(other.blockHolder.block.clone());
            this.currentBlockId = other.currentBlockId;
        }
    }

    protected int getBlock(long pos) {
        return (int) (pos >>> blockSizeShift);
    }

    protected int getBlockOffset(long pos) {
        return (int) (pos & blockMask);
    }

    protected long getBlockStart(int blockId) {
        return (long) blockId << blockSizeShift;
    }

    protected long currentBlockStart() {
        return getBlockStart(currentBlockId);
    }

    protected int currentBlockPosition() {
        return (int) blockHolder.block.getFilePointer();
    }

    public static Builder builder() {
        return new Builder();
    }

    @SuppressWarnings("checkstyle:OneStatementPerLine")
    public static class Builder {
        // Block size shift (default value is 23 == 2^23 == 8MiB)
        public static final int DEFAULT_BLOCK_SIZE_SHIFT = 23;
        public static final int DEFAULT_BLOCK_SIZE = 1 << DEFAULT_BLOCK_SIZE_SHIFT;;

        private String resourceDescription;
        private boolean isClone;
        private long offset;
        private long length;
        private int blockSizeShift = DEFAULT_BLOCK_SIZE_SHIFT;
        private int blockSize = 1 << blockSizeShift;
        private int blockMask = blockSize - 1;

        private Builder() {}

        public Builder resourceDescription(String resourceDescription) {
            this.resourceDescription = resourceDescription;
            return this;
        }

        public Builder isClone(boolean clone) {
            isClone = clone;
            return this;
        }

        public Builder offset(long offset) {
            this.offset = offset;
            return this;
        }

        public Builder length(long length) {
            this.length = length;
            return this;
        }

        Builder blockSizeShift(int blockSizeShift) {
            assert blockSizeShift < 31 : "blockSizeShift must be < 31";
            this.blockSizeShift = blockSizeShift;
            this.blockSize = 1 << blockSizeShift;
            this.blockMask = blockSize - 1;
            return this;
        }
    }

    /**
     * Simple class to hold the currently open IndexInput backing an instance
     * of an {@link OnDemandBlockIndexInput}. Lucene may clone one of these
     * instances, and per the contract[1], the clones will never be closed.
     * However, closing the instances is critical for our reference counting.
     * Therefore, we are using the {@link Cleaner} mechanism from the JDK to
     * close these clones when they become phantom reachable. The clean action
     * must not hold a reference to the {@link OnDemandBlockIndexInput} itself
     * (otherwise it would never become phantom reachable!) so we need a wrapper
     * instance to hold the current underlying IndexInput, while allowing it to
     * be changed out with different instances as {@link OnDemandBlockIndexInput}
     * reads through the data.
     * <p>
     * This class implements {@link Runnable} so that it can be passed directly
     * to the cleaner to run its close action.
     * <p>
     * [1]: https://github.com/apache/lucene/blob/8340b01c3cc229f33584ce2178b07b8984daa6a9/lucene/core/src/java/org/apache/lucene/store/IndexInput.java#L32-L33
     */
    private static class BlockHolder implements Closeable, Runnable {
        private volatile IndexInput block;

        private void set(IndexInput block) {
            if (this.block != null) {
                throw new IllegalStateException("Previous block was not closed!");
            }
            this.block = Objects.requireNonNull(block);
        }

        @Override
        public void close() throws IOException {
            if (block != null) {
                block.close();
                block = null;
            }
        }

        @Override
        public void run() {
            try {
                close();
            } catch (IOException e) {
                // Exceptions thrown in the cleaning action are ignored,
                // so log and swallow the exception here
                logger.info("Exception thrown while closing block owned by phantom reachable instance", e);
            }
        }
    }
}

