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
import org.elasticsearch.common.util.BytesRefHashTable;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.aggregation.blockhash.HashImplFactory;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.GroupKeyEncoder;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;
import java.util.List;

/**
 * A top-N operator for grouped (SORT + LIMIT BY) queries. Maintains per-group priority queues
 * using a {@link GroupKeyEncoder} to map group key columns to integer group IDs.
 * <p>
 * Group keys use list semantics for multivalues: {@code [1,2]} and {@code [2,1]} are different groups.
 * <p>
 * Unlike {@link TopNOperator}, this operator does not support sorted input optimization
 * or {@link SharedMinCompetitive} tracking, as these optimizations are not applicable
 * to grouped top-N.
 */
public class GroupedTopNOperator implements Operator, Accountable {

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
            var scratch = new BreakingBytesRefBuilder(driverContext.breaker(), "group-key-encoder");
            int[] groupKeysArray = groupKeys.stream().mapToInt(Integer::intValue).toArray();
            var keyEncoder = new GroupKeyEncoder(groupKeysArray, elementTypes, scratch);
            return new GroupedTopNOperator(
                driverContext.blockFactory(),
                driverContext.breaker(),
                topCount,
                elementTypes,
                encoders,
                sortOrders,
                keyEncoder,
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
    private final boolean[] channelInKey;
    private final GroupKeyEncoder keyEncoder;

    private BytesRefHashTable keysHash;
    private GroupedQueue inputQueue;
    private TopNRow spare;

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
        GroupKeyEncoder keyEncoder,
        int maxPageSize,
        long jumboPageBytes
    ) {
        BytesRefHashTable keysHash = null;
        GroupedQueue inputQueue = null;
        boolean success = false;
        try {
            keysHash = HashImplFactory.newBytesRefHash(blockFactory);
            inputQueue = new GroupedQueue(breaker, blockFactory.bigArrays(), topCount);
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(keyEncoder, keysHash, inputQueue);
            }
        }
        this.keyEncoder = keyEncoder;
        this.keysHash = keysHash;
        this.inputQueue = inputQueue;
        this.blockFactory = blockFactory;
        this.breaker = breaker;
        this.maxPageSize = maxPageSize;
        this.jumboPageBytes = jumboPageBytes;
        this.topCount = topCount;
        this.elementTypes = elementTypes;
        this.encoders = encoders;
        this.sortOrders = sortOrders;
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
            TopNOperator.RowFiller rowFiller = new TopNOperator.RowFiller(elementTypes, encoders, sortOrders, channelInKey, page);
            for (int pos = 0; pos < page.getPositionCount(); pos++) {
                BytesRef key = keyEncoder.encode(page, pos);
                long hashOrd = keysHash.add(key);
                long groupId = BlockHash.hashOrdToGroup(hashOrd);
                processRow(rowFiller, pos, groupId);
            }
        } finally {
            page.releaseBlocks();
            pagesReceived++;
            rowsReceived += page.getPositionCount();
            receiveNanos += System.nanoTime() - start;
        }
    }

    private void processRow(TopNOperator.RowFiller rowFiller, int position, long groupId) {
        if (spare == null) {
            spare = new TopNRow(breaker, rowFiller.preAllocatedKeysSize(), rowFiller.preAllocatedValueSize());
        } else {
            spare.clear();
        }
        rowFiller.writeKey(position, spare);

        // Write values BEFORE modifying the queue so that if writeValues throws (e.g. circuit breaker),
        // spare is not left in both the queue and the spare field (which would double-close).
        TopNQueue queue = inputQueue.getOrCreateQueue(groupId);
        if (queue.size() < queue.topCount) {
            rowFiller.writeValues(position, spare);
            queue.add(spare);
            spare = null;
        } else if (queue.lessThan(queue.top(), spare)) {
            rowFiller.writeValues(position, spare);
            TopNRow nextSpare = queue.top();
            queue.updateTop(spare);
            spare = nextSpare;
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
        Releasables.closeExpectNoException(spare, inputQueue, output, keysHash, keyEncoder);
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
        size += RamUsageEstimator.sizeOf(channelInKey);
        size += sortOrders.size() * SORT_ORDER_SIZE;
        size += keyEncoder.ramBytesUsed();
        if (keysHash != null) {
            size += keysHash.ramBytesUsed();
        }
        if (inputQueue != null) {
            size += inputQueue.ramBytesUsed();
        }
        if (spare != null) {
            size += spare.ramBytesUsed();
        }
        return size;
    }

    @Override
    public Status status() {
        return new GroupedTopNOperatorStatus(
            receiveNanos,
            emitNanos,
            inputQueue != null ? inputQueue.size() : 0,
            keysHash != null ? (int) keysHash.size() : 0,
            ramBytesUsed(),
            pagesReceived,
            pagesEmitted,
            rowsReceived,
            rowsEmitted
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
            + Arrays.toString(keyEncoder.groupChannels())
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

        List<TopNRow> rows = inputQueue.popAll();
        inputQueue.close();
        keysHash.close();
        inputQueue = null;
        keysHash = null;
        return new Result(rows);
    }

    private class Result implements ReleasableIterator<Page> {
        private final List<TopNRow> rows;
        private int r;

        private Result(List<TopNRow> rows) {
            this.rows = rows;
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
            ResultBuilder[] builders = new ResultBuilder[elementTypes.size()];
            try {
                for (int b = 0; b < builders.length; b++) {
                    builders[b] = ResultBuilder.resultBuilderFor(blockFactory, elementTypes.get(b), encoders.get(b), channelInKey[b], size);
                }
                int rEnd = r + size;
                while (r < rEnd) {
                    try (TopNRow row = rows.set(r++, null)) {
                        readKeys(builders, row.keys.bytesRefView());
                        readValues(builders, row.values.bytesRefView());
                    }
                    if (totalSize(builders) > jumboPageBytes) {
                        break;
                    }
                }

                return new Page(ResultBuilder.buildAll(builders));
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
