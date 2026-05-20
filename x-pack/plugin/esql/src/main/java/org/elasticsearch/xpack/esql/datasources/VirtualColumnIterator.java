/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.util.Check;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Producer-side wrapper that augments pages from the format reader with constant blocks for virtual
 * partition / {@code _file.*} columns. The reader emits data-only pages; this iterator slots the
 * data blocks into the unified output schema and fills the remaining slots with constant blocks
 * carrying the partition values for the file currently being read.
 * <p>
 * Memory: constant blocks are allocated against the same {@link BlockFactory} the reader uses
 * (the root, request-scoped factory), so they are charged to the global request circuit breaker
 * just like the data blocks they accompany. This avoids the driver-local breaker's single-thread
 * assertion firing when the producer drains pages off a generic-pool thread.
 *
 * @see ExternalSourceOperatorFactory for where this iterator is attached.
 */
final class VirtualColumnIterator implements CloseableIterator<Page> {

    private final CloseableIterator<Page> delegate;
    private final List<Attribute> fullOutput;
    private final Map<String, Object> partitionValues;
    private final BlockFactory blockFactory;
    private final int[] dataColumnIndices;
    private final int[] partitionColumnIndices;

    VirtualColumnIterator(
        CloseableIterator<Page> delegate,
        List<Attribute> fullOutput,
        Set<String> partitionColumnNames,
        Map<String, Object> partitionValues,
        BlockFactory blockFactory
    ) {
        Check.notNull(delegate, "delegate cannot be null");
        Check.isTrue(fullOutput != null && fullOutput.isEmpty() == false, "fullOutput cannot be null or empty");
        Check.notNull(partitionColumnNames, "partitionColumnNames cannot be null");
        Check.notNull(blockFactory, "blockFactory cannot be null");
        this.delegate = delegate;
        this.fullOutput = fullOutput;
        this.partitionValues = partitionValues != null ? partitionValues : Map.of();
        this.blockFactory = blockFactory;

        List<Integer> dataIdxList = new ArrayList<>();
        List<Integer> partIdxList = new ArrayList<>();
        for (int i = 0; i < fullOutput.size(); i++) {
            if (partitionColumnNames.contains(fullOutput.get(i).name())) {
                partIdxList.add(i);
            } else {
                dataIdxList.add(i);
            }
        }
        this.dataColumnIndices = toIntArray(dataIdxList);
        this.partitionColumnIndices = toIntArray(partIdxList);
    }

    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }

    @Override
    public Page next() {
        return inject(delegate.next());
    }

    @Override
    public SubscribableListener<Void> waitForReady() {
        return delegate.waitForReady();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    /**
     * Public for unit tests.
     * <p>
     * On success the input page's data blocks are reused inside the returned page; any extra
     * blocks the producer emitted beyond the configured projection are released here so the
     * iterator never silently drops a refcount. On failure both the partial constant-block
     * allocations and the input page's blocks are released so the caller does not have to
     * clean up the input page separately.
     */
    Page inject(Page dataPage) {
        if (partitionColumnIndices.length == 0) {
            // Any extra blocks the producer emitted beyond what we project would otherwise leak
            // when the dataPage reference is dropped without releaseBlocks(). Forward as-is when
            // the block count matches; otherwise rebuild a tightly-projected page and release the
            // surplus blocks.
            int producedBlocks = dataPage.getBlockCount();
            if (producedBlocks == dataColumnIndices.length) {
                return dataPage;
            }
            return projectAndReleaseSurplus(dataPage);
        }

        int positions = dataPage.getPositionCount();
        Block[] blocks = new Block[fullOutput.size()];

        int producedBlocks = dataPage.getBlockCount();
        int expectedDataBlocks = dataColumnIndices.length;
        int dataBlockIdx = 0;
        for (int idx : dataColumnIndices) {
            blocks[idx] = dataPage.getBlock(dataBlockIdx++);
        }
        // Producer emitted more blocks than we project (e.g. a format reader that falls back to
        // the full file schema when the projection list is empty). Release the surplus so their
        // breaker bytes are returned; the kept blocks are still owned by {@code blocks}.
        if (producedBlocks > expectedDataBlocks) {
            for (int i = expectedDataBlocks; i < producedBlocks; i++) {
                Block extra = dataPage.getBlock(i);
                if (extra != null) {
                    extra.close();
                }
            }
        }

        int partitionBlocksAllocated = 0;
        try {
            for (int idx : partitionColumnIndices) {
                Attribute attr = fullOutput.get(idx);
                Object value = partitionValues.get(attr.name());
                blocks[idx] = createConstantBlock(attr, value, positions);
                partitionBlocksAllocated++;
            }
            return new Page(positions, blocks);
        } catch (Throwable t) {
            for (int i = 0, released = 0; i < partitionColumnIndices.length && released < partitionBlocksAllocated; i++) {
                Block b = blocks[partitionColumnIndices[i]];
                if (b != null) {
                    b.close();
                    released++;
                }
            }
            dataPage.releaseBlocks();
            throw t;
        }
    }

    /**
     * Builds a page containing only the projected data blocks and releases the surplus. Used when
     * there are no partition columns to inject but the producer over-projected.
     */
    private Page projectAndReleaseSurplus(Page dataPage) {
        int positions = dataPage.getPositionCount();
        int expected = dataColumnIndices.length;
        Block[] kept = new Block[expected];
        for (int i = 0; i < expected; i++) {
            kept[i] = dataPage.getBlock(i);
        }
        for (int i = expected; i < dataPage.getBlockCount(); i++) {
            Block extra = dataPage.getBlock(i);
            if (extra != null) {
                extra.close();
            }
        }
        return new Page(positions, kept);
    }

    boolean hasPartitionColumns() {
        return partitionColumnIndices.length > 0;
    }

    List<String> dataColumnNames() {
        List<String> names = new ArrayList<>(dataColumnIndices.length);
        for (int idx : dataColumnIndices) {
            names.add(fullOutput.get(idx).name());
        }
        return names;
    }

    private static int[] toIntArray(List<Integer> list) {
        int[] result = new int[list.size()];
        for (int i = 0; i < list.size(); i++) {
            result[i] = list.get(i);
        }
        return result;
    }

    private Block createConstantBlock(Attribute attr, Object value, int positions) {
        return switch (value) {
            case null -> blockFactory.newConstantNullBlock(positions);
            case Integer intVal -> blockFactory.newConstantIntBlockWith(intVal, positions);
            case Long longVal -> blockFactory.newConstantLongBlockWith(longVal, positions);
            case Double doubleVal -> blockFactory.newConstantDoubleBlockWith(doubleVal, positions);
            case Boolean boolVal -> blockFactory.newConstantBooleanBlockWith(boolVal, positions);
            case BytesRef bytesRef -> blockFactory.newConstantBytesRefBlockWith(bytesRef, positions);
            default -> blockFactory.newConstantBytesRefBlockWith(new BytesRef(value.toString()), positions);
        };
    }
}
