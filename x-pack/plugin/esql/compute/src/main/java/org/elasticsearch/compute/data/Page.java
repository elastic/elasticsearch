/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * A page is a column-oriented data abstraction that allows data to be passed between operators in
 * batches.
 *
 * <p> A page has a fixed number of positions (or rows), exposed via {@link #getPositionCount()}.
 * It is further composed of a number of {@link Block}s, which represent the columnar data.
 * The number of blocks can be retrieved via {@link #getBlockCount()}, and the respective
 * blocks can be retrieved via their index {@link #getBlock(int)}.
 *
 * <p> Pages are immutable and can be passed between threads. This class may be subclassed to
 * add metadata (e.g., batch information for streaming exchanges). Subclasses must maintain
 * the immutability and thread-safety guarantees.
 */
public final class Page implements Writeable, Releasable {

    /**
     * Sentinel value indicating this page has no partition assignment. Used when the operator
     * is not running in partitioned mode (i.e. {@code numPartitions == 1}).
     */
    public static final int NO_PARTITION = -1;

    private static final TransportVersion ESQL_PAGE_PARTITION_ID = TransportVersion.fromName("esql_page_partition_id");
    private static final TransportVersion BATCH_METADATA_VERSION = TransportVersion.fromName("esql_batch_page");

    private final Block[] blocks;

    private final int positionCount;

    /**
     * The partition ID for this page, used by the hash aggregation operator to tag
     * output pages with the partition they belong to. A value of {@link #NO_PARTITION}
     * indicates this page has no partition assignment.
     */
    private final int partitionId;

    /**
     * Optional batch metadata for bidirectional batch exchanges.
     */
    @Nullable
    private final BatchMetadata batchMetadata;

    /**
     * True if we've called {@link #releaseBlocks()} which causes us to remove the
     * circuit breaker for the {@link Block}s. The {@link Page} reference should be
     * removed shortly after this and reading {@linkplain Block}s after release
     * will fail.
     */
    private boolean blocksReleased = false;

    /**
     * Creates a new page with the given blocks. Every block has the same number of positions.
     *
     * @param blocks the blocks
     * @throws IllegalArgumentException if all blocks do not have the same number of positions
     */
    public Page(Block... blocks) {
        this(true, NO_PARTITION, determinePositionCount(blocks), blocks, null);
    }

    /**
     * Creates a new page with the given positionCount and blocks. Assumes that every block has the
     * same number of positions as the positionCount that's passed in - there is no validation of
     * this.
     *
     * @param positionCount the block position count
     * @param blocks the blocks
     */
    public Page(int positionCount, Block... blocks) {
        this(true, NO_PARTITION, positionCount, blocks, null);
    }

    /**
     * Creates a new page tagged with a partition ID for partitioned hash aggregation.
     * The partition ID identifies which downstream FINAL-stage driver should process this page.
     * Called by {@link org.elasticsearch.compute.operator.HashAggregationOperator#emitPartitioned()}
     * when splitting groups across multiple output pages.
     *
     * @param partitionId the partition this page belongs to (0 to numPartitions-1)
     * @param blocks the blocks
     * @return a new Page tagged with the given partition ID
     */
    public static Page withPartitionId(int partitionId, Block... blocks) {
        return new Page(true, partitionId, determinePositionCount(blocks), blocks, null);
    }

    private Page(boolean copyBlocks, int partitionId, int positionCount, Block[] blocks, @Nullable BatchMetadata batchMetadata) {
        Objects.requireNonNull(blocks, "blocks is null");
        // assert assertPositionCount(blocks);
        this.partitionId = partitionId;
        this.positionCount = positionCount;
        this.blocks = copyBlocks ? blocks.clone() : blocks;
        this.batchMetadata = batchMetadata;
        for (Block b : blocks) {
            assert b.getPositionCount() == positionCount : "expected positionCount=" + positionCount + " but was " + b;
            if (b.isReleased()) {
                throw new IllegalArgumentException("can't build page out of released blocks but [" + b + "] was released");
            }
        }
    }

    /**
     * Appending ctor, see {@link #appendBlocks}.
     */
    private Page(Page prev, Block[] toAdd) {
        for (Block block : toAdd) {
            if (prev.positionCount != block.getPositionCount()) {
                throw new IllegalStateException(
                    "Block [" + block + "] does not have same position count: " + block.getPositionCount() + " != " + prev.positionCount
                );
            }
        }
        this.partitionId = prev.partitionId;
        this.positionCount = prev.positionCount;
        this.batchMetadata = prev.batchMetadata;

        this.blocks = Arrays.copyOf(prev.blocks, prev.blocks.length + toAdd.length);
        System.arraycopy(toAdd, 0, this.blocks, prev.blocks.length, toAdd.length);
    }

    public Page(StreamInput in) throws IOException {
        int positionCount = in.readVInt();
        int blockPositions = in.readVInt();
        if (in.getTransportVersion().supports(ESQL_PAGE_PARTITION_ID)) {
            this.partitionId = in.readInt();
        } else {
            this.partitionId = NO_PARTITION;
        }
        Block[] blocks = new Block[blockPositions];
        BlockStreamInput blockStreamInput = (BlockStreamInput) in;
        boolean success = false;
        try {
            for (int blockIndex = 0; blockIndex < blockPositions; blockIndex++) {
                blocks[blockIndex] = Block.readTypedBlock(blockStreamInput);
            }
            success = true;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(blocks);
            }
        }
        this.positionCount = positionCount;
        this.blocks = blocks;
        // Read optional batch metadata at the end (added in BATCH_METADATA_VERSION)
        this.batchMetadata = in.getTransportVersion().supports(BATCH_METADATA_VERSION) ? in.readOptional(BatchMetadata::readFrom) : null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(positionCount);
        out.writeVInt(getBlockCount());
        if (out.getTransportVersion().supports(ESQL_PAGE_PARTITION_ID)) {
            out.writeInt(partitionId);
        }
        for (Block block : blocks) {
            Block.writeTypedBlock(block, out);
        }
        if (out.getTransportVersion().supports(BATCH_METADATA_VERSION)) {
            out.writeOptionalWriteable(batchMetadata);
        }
    }

    private static int determinePositionCount(Block... blocks) {
        Objects.requireNonNull(blocks, "blocks is null");
        if (blocks.length == 0) {
            throw new IllegalArgumentException("blocks is empty");
        }
        return blocks[0].getPositionCount();
    }

    /**
     * Returns the block at the given block index.
     *
     * @param blockIndex the block index
     * @return the block
     */
    public <B extends Block> B getBlock(int blockIndex) {
        if (blocksReleased) {
            throw new IllegalStateException("can't read released page");
        }
        @SuppressWarnings("unchecked")
        B block = (B) blocks[blockIndex];
        if (block.isReleased()) {
            throw new IllegalStateException("can't read released block [" + block + "]");
        }
        return block;
    }

    /**
     * Creates a new page, appending the given block to the existing blocks in this Page.
     *
     * @param block the block to append
     * @return a new Page with the block appended
     * @throws IllegalArgumentException if the given block does not have the same number of
     *         positions as the blocks in this Page
     */
    public Page appendBlock(Block block) {
        return new Page(this, new Block[] { block });
    }

    /**
     * Creates a new page, appending the given blocks to the existing blocks in this Page.
     *
     * @param toAdd the blocks to append
     * @return a new Page with the block appended
     * @throws IllegalArgumentException if one of the given blocks does not have the same number of
     *        positions as the blocks in this Page
     */
    public Page appendBlocks(Block[] toAdd) {
        return new Page(this, toAdd);
    }

    /**
     * Creates a new page, appending the blocks of the given block to the existing blocks in this Page.
     *
     * @param toAdd the page to append
     * @return a new Page
     * @throws IllegalArgumentException if any blocks of the given page does not have the same number of
     *                                  positions as the blocks in this Page
     */
    public Page appendPage(Page toAdd) {
        return appendBlocks(toAdd.blocks);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(positionCount);
        for (Block block : blocks) {
            result = 31 * result + Objects.hashCode(block);
        }
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Page page = (Page) o;
        return positionCount == page.positionCount && Arrays.equals(blocks, page.blocks);
    }

    @Override
    public String toString() {
        return "Page{" + "blocks=" + Arrays.toString(blocks) + '}';
    }

    /**
     * Returns the number of positions (rows) in this page.
     *
     * @return the number of positions
     */
    public int getPositionCount() {
        return positionCount;
    }

    /**
     * Returns the partition ID for this page, or {@link #NO_PARTITION} if this page
     * has no partition assignment.
     */
    public int getPartitionId() {
        return partitionId;
    }

    /**
     * Returns the number of blocks in this page. Blocks can then be retrieved via
     * {@link #getBlock(int)} where channel ranges from 0 to {@code getBlockCount}.
     *
     * @return the number of blocks in this page
     */
    public int getBlockCount() {
        return blocks.length;
    }

    @Nullable
    public BatchMetadata batchMetadata() {
        return batchMetadata;
    }

    /**
     * Creates a new page with the same blocks but with the given batch metadata.
     * The blocks are shared (ref count incremented) with the original page.
     */
    public Page withBatchMetadata(BatchMetadata metadata) {
        for (Block block : blocks) {
            block.incRef();
        }
        return new Page(false, partitionId, positionCount, blocks.clone(), metadata);
    }

    /**
     * Check if this page is a batch marker (empty page with isLastPageInBatch=true).
     * Marker pages are used to signal batch completion for batches that produce no output.
     */
    public boolean isBatchMarkerOnly() {
        return positionCount == 0 && batchMetadata != null && batchMetadata.isLastPageInBatch();
    }

    /**
     * Creates an empty marker page for batch completion.
     * A marker page is an empty page with isLastPageInBatch=true.
     */
    public static Page createBatchMarkerPage(long batchId, int pageIndexInBatch) {
        return new Page(false, NO_PARTITION, 0, new Block[0], BatchMetadata.createMarker(batchId, pageIndexInBatch));
    }

    public long ramBytesUsedByBlocks() {
        return Arrays.stream(blocks).mapToLong(Accountable::ramBytesUsed).sum();
    }

    /**
     * Release all blocks in this page, decrementing any breakers accounting for these blocks.
     */
    public void releaseBlocks() {
        if (blocksReleased) {
            return;
        }

        blocksReleased = true;

        Releasables.closeExpectNoException(blocks);
    }

    @Override
    public void close() {
        releaseBlocks();
    }

    /**
     * Before passing a Page to another Driver, it is necessary to switch the owning block factories of its Blocks to their parents,
     * which are associated with the global circuit breaker. This ensures that when the new driver releases this Page, it returns
     * memory directly to the parent block factory instead of the local block factory. This is important because the local block
     * factory is not thread safe and doesn't support simultaneous access by more than one thread.
     */
    public void allowPassingToDifferentDriver() {
        for (Block block : blocks) {
            block.allowPassingToDifferentDriver();
        }
    }

    public Page shallowCopy() {
        for (Block b : blocks) {
            b.incRef();
        }
        return new Page(false, partitionId, positionCount, blocks.clone(), batchMetadata);
    }

    /**
     * Returns a new page with blocks in the containing {@link Block}s
     * shifted around or removed. The new {@link Page} will have as
     * many blocks as the {@code length} of the provided array. Those
     * blocks will be set to the block at the position of the
     * <strong>value</strong> of each entry in the parameter.
     */
    public Page projectBlocks(int[] blockMapping) {
        if (blocksReleased) {
            throw new IllegalStateException("can't read released page");
        }
        Block[] mapped = new Block[blockMapping.length];
        try {
            for (int b = 0; b < blockMapping.length; b++) {
                if (blockMapping[b] >= blocks.length) {
                    throw new IllegalArgumentException(
                        "Cannot project block with index [" + blockMapping[b] + "] from a page with size [" + blocks.length + "]"
                    );
                }
                mapped[b] = blocks[blockMapping[b]];
                mapped[b].incRef();
            }
            Page result = new Page(false, partitionId, getPositionCount(), mapped, batchMetadata);
            mapped = null;
            return result;
        } finally {
            if (mapped != null) {
                Releasables.close(mapped);
            }
        }
    }

    /**
     * Creates a new page that only exposes the positions provided.
     * @param mayContainDuplicates may the positions array contain duplicate positions?
     * @param positions the positions to retain
     * @return a filtered page
     */
    public Page filter(boolean mayContainDuplicates, int... positions) {
        Block[] filteredBlocks = new Block[blocks.length];
        boolean success = false;
        try {
            for (int i = 0; i < blocks.length; i++) {
                filteredBlocks[i] = getBlock(i).filter(mayContainDuplicates, positions);
            }
            success = true;
        } finally {
            releaseBlocks();
            if (success == false) {
                Releasables.closeExpectNoException(filteredBlocks);
            }
        }
        return new Page(false, partitionId, positions.length, filteredBlocks, batchMetadata);
    }
}
