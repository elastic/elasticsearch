/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntArrayBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

/**
 * A top-N operator for grouped (SORT + LIMIT BY) queries. Maintains per-group priority queues
 * using a {@link BlockHash} to map group key columns to integer group IDs.
 * <p>
 * Unlike {@link TopNOperator}, this operator does not support sorted input optimization
 * or {@link SharedMinCompetitive} tracking, as these optimizations are not applicable
 * to grouped top-N.
 */
public class GroupedTopNOperator implements Operator, Accountable {

    /**
     * Emit batch size for the BlockHash's {@link org.elasticsearch.compute.aggregation.blockhash.AddPage}.
     */
    static final int EMIT_BATCH_SIZE = 10 * 1024;

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(GroupedTopNOperator.class) + RamUsageEstimator
        .shallowSizeOfInstance(List.class) * 3;

    private static final long SORT_ORDER_SIZE = RamUsageEstimator.shallowSizeOfInstance(TopNOperator.SortOrder.class);

    public record GroupedTopNOperatorFactory(
        int topCount,
        List<ElementType> elementTypes,
        List<TopNEncoder> encoders,
        List<TopNOperator.SortOrder> sortOrders,
        List<Integer> groupKeys,
        int maxPageSize,
        long jumboPageBytes
    ) implements OperatorFactory {
        public GroupedTopNOperatorFactory {
            for (ElementType e : elementTypes) {
                if (e == null) {
                    throw new IllegalArgumentException("ElementType not known");
                }
            }
            if (groupKeys.isEmpty()) {
                throw new IllegalArgumentException("GroupedTopNOperator requires at least one group key");
            }
        }

        @Override
        public GroupedTopNOperator get(DriverContext driverContext) {
            return new GroupedTopNOperator(
                driverContext.blockFactory(),
                driverContext.breaker(),
                topCount,
                elementTypes,
                encoders,
                sortOrders,
                groupKeys.stream().mapToInt(Integer::intValue).toArray(),
                maxPageSize,
                jumboPageBytes
            );
        }

        @Override
        public String describe() {
            return "GroupedTopNOperator[count="
                + topCount
                + ", elementTypes="
                + elementTypes
                + ", encoders="
                + encoders
                + ", sortOrders="
                + sortOrders
                + ", groupKeys="
                + groupKeys
                + "]";
        }
    }

    private final BlockFactory blockFactory;
    private final CircuitBreaker breaker;
    private final int maxPageSize;
    private final long jumboPageBytes;
    private final int topCount;
    private final List<ElementType> elementTypes;
    private final List<TopNEncoder> encoders;
    private final List<TopNOperator.SortOrder> sortOrders;
    private final int[] groupKeys;
    private final boolean[] channelInKey;
    private final BlockHash blockHash;

    private GroupedQueue inputQueue;
    private GroupedRow spare;

    private ReleasableIterator<Page> output;

    private long receiveNanos;
    private long emitNanos;
    private int pagesReceived;
    private int pagesEmitted;
    private long rowsReceived;
    private long rowsEmitted;

    public GroupedTopNOperator(
        BlockFactory blockFactory,
        CircuitBreaker breaker,
        int topCount,
        List<ElementType> elementTypes,
        List<TopNEncoder> encoders,
        List<TopNOperator.SortOrder> sortOrders,
        int[] groupKeys,
        int maxPageSize,
        long jumboPageBytes
    ) {
        BlockHash blockHash = null;
        GroupedQueue inputQueue = null;
        boolean success = false;
        try {
            List<BlockHash.GroupSpec> groupSpecs = IntStream.of(groupKeys)
                .mapToObj(ch -> new BlockHash.GroupSpec(ch, elementTypes.get(ch)))
                .toList();
            blockHash = BlockHash.build(groupSpecs, blockFactory, EMIT_BATCH_SIZE, false);
            inputQueue = new GroupedQueue(breaker, blockFactory.bigArrays(), topCount);
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(blockHash, inputQueue);
            }
        }
        this.blockHash = blockHash;
        this.inputQueue = inputQueue;
        this.blockFactory = blockFactory;
        this.breaker = breaker;
        this.maxPageSize = maxPageSize;
        this.jumboPageBytes = jumboPageBytes;
        this.topCount = topCount;
        this.elementTypes = elementTypes;
        this.encoders = encoders;
        this.sortOrders = sortOrders;
        this.groupKeys = groupKeys;
        this.channelInKey = new boolean[elementTypes.size()];
        for (TopNOperator.SortOrder so : sortOrders) {
            channelInKey[so.channel()] = true;
        }
    }

    @Override
    public boolean needsInput() {
        return output == null;
    }

    @Override
    public void addInput(Page page) {
        long start = System.nanoTime();
        try {
            if (this.topCount <= 0) {
                return;
            }
            GroupedRowFiller rowFiller = new GroupedRowFiller(elementTypes, encoders, sortOrders, channelInKey, page);
            blockHash.add(page, new GroupingAggregatorFunction.AddInput() {
                @Override
                public void add(int positionOffset, IntVector groupIds) {
                    for (int j = 0; j < groupIds.getPositionCount(); j++) {
                        processRow(rowFiller, positionOffset + j, groupIds.getInt(j));
                    }
                }

                @Override
                public void add(int positionOffset, IntArrayBlock groupIds) {
                    addFromBlock(rowFiller, positionOffset, groupIds);
                }

                @Override
                public void add(int positionOffset, IntBigArrayBlock groupIds) {
                    addFromBlock(rowFiller, positionOffset, groupIds);
                }

                @Override
                public void close() {}
            });
        } finally {
            page.releaseBlocks();
            pagesReceived++;
            rowsReceived += page.getPositionCount();
            receiveNanos += System.nanoTime() - start;
        }
    }

    private void addFromBlock(GroupedRowFiller rowFiller, int positionOffset, IntBlock groupIds) {
        for (int j = 0; j < groupIds.getPositionCount(); j++) {
            if (groupIds.isNull(j)) {
                continue;
            }
            int start = groupIds.getFirstValueIndex(j);
            int count = groupIds.getValueCount(j);
            for (int g = 0; g < count; g++) {
                processRow(rowFiller, positionOffset + j, groupIds.getInt(start + g));
            }
        }
    }

    private void processRow(GroupedRowFiller rowFiller, int position, int groupId) {
        if (spare == null) {
            spare = new GroupedRow(breaker, rowFiller.preAllocatedKeysSize(), rowFiller.preAllocatedValueSize());
        } else {
            spare.clear();
        }
        spare.groupId = groupId;
        rowFiller.writeSortKey(position, spare);

        var nextSpare = inputQueue.addRow(spare);
        if (nextSpare != spare) {
            var insertedRow = spare;
            spare = nextSpare;
            rowFiller.writeValues(position, insertedRow);
        }
    }

    @Override
    public void finish() {
        if (output == null) {
            long start = System.nanoTime();
            output = buildResult();
            emitNanos += System.nanoTime() - start;
        }
    }

    @Override
    public boolean isFinished() {
        return output != null && output.hasNext() == false;
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return output != null && output.hasNext();
    }

    @Override
    public Page getOutput() {
        if (output == null || output.hasNext() == false) {
            return null;
        }
        Page ret = output.next();
        pagesEmitted++;
        rowsEmitted += ret.getPositionCount();
        return ret;
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(spare, inputQueue, output, blockHash);
        inputQueue = null;
        output = null;
    }

    @Override
    public long ramBytesUsed() {
        long arrHeader = RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
        long ref = RamUsageEstimator.NUM_BYTES_OBJECT_REF;
        long size = SHALLOW_SIZE;
        size += RamUsageEstimator.alignObjectSize(arrHeader + ref * elementTypes.size());
        size += RamUsageEstimator.alignObjectSize(arrHeader + ref * encoders.size());
        size += RamUsageEstimator.alignObjectSize(arrHeader + ref * sortOrders.size());
        size += RamUsageEstimator.sizeOf(groupKeys);
        size += RamUsageEstimator.sizeOf(channelInKey);
        size += sortOrders.size() * SORT_ORDER_SIZE;
        if (inputQueue != null) {
            size += inputQueue.ramBytesUsed();
        }
        return size;
    }

    @Override
    public Status status() {
        // TODO: Make a custom status
        return new TopNOperatorStatus(
            receiveNanos,
            emitNanos,
            inputQueue != null ? inputQueue.size() : 0,
            ramBytesUsed(),
            pagesReceived,
            pagesEmitted,
            rowsReceived,
            rowsEmitted,
            null
        );
    }

    @Override
    public String toString() {
        return "GroupedTopNOperator[count="
            + inputQueue
            + ", elementTypes="
            + elementTypes
            + ", encoders="
            + encoders
            + ", sortOrders="
            + sortOrders
            + ", groupKeys="
            + Arrays.toString(groupKeys)
            + "]";
    }

    /**
     * Build the result iterator. Moves all rows from the {@link #inputQueue} and
     * {@link #close}s it.
     */
    private ReleasableIterator<Page> buildResult() {
        if (spare != null) {
            spare.close();
            spare = null;
        }

        if (inputQueue.size() == 0) {
            return ReleasableIterator.empty();
        }

        List<GroupedRow> rows = inputQueue.popAll();
        inputQueue.close();
        inputQueue = null;
        Block[] groupKeyBlocks = blockHash.getKeys();
        int[] groupIdToKeyPosition = computeGroupIdToKeyPosition();
        boolean success = false;
        try {
            Result result = new Result(rows, groupKeyBlocks, groupIdToKeyPosition);
            success = true;
            return result;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(groupKeyBlocks);
                Releasables.close(rows);
            }
        }
    }

    private int[] computeGroupIdToKeyPosition() {
        try (IntVector nonEmpty = blockHash.nonEmpty()) {
            int maxGroupId = 0;
            for (int i = 0; i < nonEmpty.getPositionCount(); i++) {
                maxGroupId = Math.max(maxGroupId, nonEmpty.getInt(i));
            }
            int[] mapping = new int[maxGroupId + 1];
            for (int i = 0; i < nonEmpty.getPositionCount(); i++) {
                mapping[nonEmpty.getInt(i)] = i;
            }
            return mapping;
        }
    }

    private class Result implements ReleasableIterator<Page> {
        private final List<GroupedRow> rows;
        /**
         * The expanded single-value key blocks from the BlockHash, one per group key channel.
         */
        private final Block[] groupKeyBlocks;
        /**
         * Maps group IDs (from {@link GroupedRow#groupId}) to positions in {@link #groupKeyBlocks}.
         * Group IDs from BlockHash may not be 0-based (e.g., {@code hashOrdToGroupNullReserved}
         * reserves 0 for null), so this mapping is needed.
         */
        private final int[] groupIdToKeyPosition;
        private int r;

        private Result(List<GroupedRow> rows, Block[] groupKeyBlocks, int[] groupIdToKeyPosition) {
            this.rows = rows;
            this.groupKeyBlocks = groupKeyBlocks;
            this.groupIdToKeyPosition = groupIdToKeyPosition;
        }

        @Override
        public boolean hasNext() {
            return r < rows.size();
        }

        @Override
        public Page next() {
            long start = System.nanoTime();
            int size = Math.min(maxPageSize, rows.size() - r);
            if (size <= 0) {
                throw new IllegalStateException("can't make empty pages. " + size + " must be > 0");
            }
            int[] rowKeyPositions = new int[size];
            ResultBuilder[] builders = new ResultBuilder[elementTypes.size()];
            try {
                for (int b = 0; b < builders.length; b++) {
                    builders[b] = ResultBuilder.resultBuilderFor(blockFactory, elementTypes.get(b), encoders.get(b), channelInKey[b], size);
                }
                int rEnd = r + size;
                int idx = 0;
                while (r < rEnd) {
                    try (GroupedRow row = rows.set(r++, null)) {
                        rowKeyPositions[idx] = groupIdToKeyPosition[row.groupId];
                        readKeys(builders, row.keys().bytesRefView());
                        readValues(builders, row.values().bytesRefView());
                    }
                    idx++;
                    if (totalSize(builders) > jumboPageBytes) {
                        break;
                    }
                }
                if (idx < size) {
                    rowKeyPositions = Arrays.copyOf(rowKeyPositions, idx);
                }

                Block[] blocks = ResultBuilder.buildAll(builders);
                for (int k = 0; k < groupKeys.length; k++) {
                    int channel = groupKeys[k];
                    Block replacement = groupKeyBlocks[k].filter(true, rowKeyPositions);
                    blocks[channel].close();
                    blocks[channel] = replacement;
                }
                return new Page(blocks);
            } finally {
                Releasables.close(builders);
                emitNanos += System.nanoTime() - start;
            }
        }

        private long totalSize(ResultBuilder[] builders) {
            long total = 0;
            for (ResultBuilder b : builders) {
                total += b.estimatedBytes();
            }
            return total;
        }

        @Override
        public void close() {
            Releasables.close(rows);
            Releasables.closeExpectNoException(groupKeyBlocks);
        }

        private void readKeys(ResultBuilder[] builders, BytesRef keys) {
            for (TopNOperator.SortOrder so : sortOrders) {
                if (keys.bytes[keys.offset] == so.nul()) {
                    keys.offset++;
                    keys.length--;
                    continue;
                }
                keys.offset++;
                keys.length--;
                builders[so.channel()].decodeKey(keys, so.asc());
            }
            if (keys.length != 0) {
                throw new IllegalArgumentException("didn't read all keys");
            }
        }

        private void readValues(ResultBuilder[] builders, BytesRef values) {
            for (ResultBuilder builder : builders) {
                builder.decodeValue(values);
            }
            if (values.length != 0) {
                throw new IllegalArgumentException("didn't read all values");
            }
        }
    }
}
