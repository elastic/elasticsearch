/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * An operator that sorts "rows" of values by encoding the values to sort on, as bytes (using BytesRef). Each data type is encoded
 * in a specific way, defined by methods of a TopNEncoder. All the values used to sort a specific row (think of column/block 3
 * and column/block 6) are converted/encoded in a byte array and the concatenated bytes are all compared in bulk.
 * For now, the only values that have a special "treatment" when it comes to encoding are the text-based ones (text, keyword, ip, version).
 * For each "special" encoding there is should be new TopNEncoder implementation. See {@link TopNEncoder#UTF8} for
 * encoding regular "text" and "keyword" data types. See LocalExecutionPlanner for which data type uses which encoder.
 *
 * This Operator will not be able to sort binary values (encoded as BytesRef) because the bytes used as separator and "null"s can appear
 * as valid bytes inside a binary value.
 */
public class TopNOperator implements Operator, Accountable {
    private static final byte SMALL_NULL = 0x01; // "null" representation for "nulls first"
    private static final byte BIG_NULL = 0x02; // "null" representation for "nulls last"

    public enum InputOrdering {
        SORTED,
        NOT_SORTED
    }

    /**
     * A single top "row". Implements {@link Comparable} and {@link Row#equals} comparing
     * the sort keys.
     */
    static final class Row implements Accountable, Comparable<Row>, Releasable {
        private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(Row.class);

        private final CircuitBreaker breaker;

        /**
         * The sort keys, encoded into bytes so we can sort by calling {@link Arrays#compareUnsigned}.
         */
        final BreakingBytesRefBuilder keys;

        /**
         * Values to reconstruct the row. Sort of. When we reconstruct the row we read
         * from both the {@link #keys} and the {@link #values}. So this only contains
         * what is required to reconstruct the row that isn't already stored in {@link #values}.
         */
        final BreakingBytesRefBuilder values;

        /**
         * Reference counter for the shard this row belongs to, used for rows containing a {@link DocVector} to ensure that the shard
         * context before we build the final result.
         */
        @Nullable
        RefCounted shardRefCounter;

        Row(CircuitBreaker breaker, int preAllocatedKeysSize, int preAllocatedValueSize) {
            breaker.addEstimateBytesAndMaybeBreak(SHALLOW_SIZE, "topn");
            this.breaker = breaker;
            boolean success = false;
            try {
                keys = new BreakingBytesRefBuilder(breaker, "topn", preAllocatedKeysSize);
                values = new BreakingBytesRefBuilder(breaker, "topn", preAllocatedValueSize);
                success = true;
            } finally {
                if (success == false) {
                    close();
                }
            }
        }

        @Override
        public long ramBytesUsed() {
            return SHALLOW_SIZE + keys.ramBytesUsed() + values.ramBytesUsed();
        }

        @Override
        public void close() {
            clearRefCounters();
            Releasables.closeExpectNoException(() -> breaker.addWithoutBreaking(-SHALLOW_SIZE), keys, values);
        }

        public void clearRefCounters() {
            if (shardRefCounter != null) {
                shardRefCounter.decRef();
            }
            shardRefCounter = null;
        }

        void setShardRefCounted(RefCounted shardRefCounted) {
            if (this.shardRefCounter != null) {
                this.shardRefCounter.decRef();
            }
            this.shardRefCounter = shardRefCounted;
            this.shardRefCounter.mustIncRef();
        }

        @Override
        public int compareTo(Row rhs) {
            // TODO if we fill the trailing bytes with 0 we could do a comparison on the entire array
            // When Nik measured this it was marginally faster. But it's worth a bit of research.
            return -keys.bytesRefView().compareTo(rhs.keys.bytesRefView());
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ;
            Row row = (Row) o;
            return keys.bytesRefView().equals(row.keys.bytesRefView());
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(keys);
        }

        @Override
        public String toString() {
            StringBuilder b = new StringBuilder("Row[key=");
            b.append(keys.bytesRefView());
            b.append(", values=");

            if (values.length() < 100) {
                b.append(values.bytesRefView());
            } else {
                b.append('[');
                assert values.bytesRefView().offset == 0;
                for (int i = 0; i < 100; i++) {
                    if (i != 0) {
                        b.append(" ");
                    }
                    b.append(Integer.toHexString(values.bytesRefView().bytes[i] & 255));
                }
                b.append("...");
            }
            return b.append("]").toString();
        }
    }

    static final class RowFiller {
        private final ValueExtractor[] valueExtractors;
        private final KeyExtractor[] keyExtractors;

        RowFiller(
            List<ElementType> elementTypes,
            List<TopNEncoder> encoders,
            List<SortOrder> sortOrders,
            boolean[] channelInKey,
            Page page
        ) {
            valueExtractors = new ValueExtractor[page.getBlockCount()];
            for (int b = 0; b < valueExtractors.length; b++) {
                valueExtractors[b] = ValueExtractor.extractorFor(
                    elementTypes.get(b),
                    encoders.get(b).toUnsortable(),
                    channelInKey[b],
                    page.getBlock(b)
                );
            }
            keyExtractors = new KeyExtractor[sortOrders.size()];
            for (int k = 0; k < keyExtractors.length; k++) {
                SortOrder so = sortOrders.get(k);
                keyExtractors[k] = KeyExtractor.extractorFor(
                    elementTypes.get(so.channel),
                    encoders.get(so.channel),
                    so.asc,
                    so.nul(),
                    so.nonNul(),
                    page.getBlock(so.channel)
                );
            }
        }

        void writeKey(int position, Row row) {
            for (KeyExtractor keyExtractor : keyExtractors) {
                keyExtractor.writeKey(row.keys, position);
            }
        }

        void writeValues(int position, Row destination) {
            for (ValueExtractor e : valueExtractors) {
                var refCounted = e.getRefCountedForShard(position);
                if (refCounted != null) {
                    destination.setShardRefCounted(refCounted);
                }
                e.writeValue(destination.values, position);
            }
        }
    }

    public record SortOrder(int channel, boolean asc, boolean nullsFirst) {

        private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(SortOrder.class);

        @Override
        public String toString() {
            return "SortOrder[channel=" + this.channel + ", asc=" + this.asc + ", nullsFirst=" + this.nullsFirst + "]";
        }

        byte nul() {
            return nullsFirst ? SMALL_NULL : BIG_NULL;
        }

        byte nonNul() {
            return nullsFirst ? BIG_NULL : SMALL_NULL;
        }
    }

    public record TopNOperatorFactory(
        int topCount,
        List<ElementType> elementTypes,
        List<TopNEncoder> encoders,
        List<SortOrder> sortOrders,
        int maxPageSize,
        InputOrdering inputOrdering
    ) implements OperatorFactory {
        public TopNOperatorFactory {
            for (ElementType e : elementTypes) {
                if (e == null) {
                    throw new IllegalArgumentException("ElementType not known");
                }
            }
        }

        @Override
        public TopNOperator get(DriverContext driverContext) {
            return new TopNOperator(
                driverContext.blockFactory(),
                driverContext.breaker(),
                topCount,
                elementTypes,
                encoders,
                sortOrders,
                maxPageSize,
                inputOrdering
            );
        }

        @Override
        public String describe() {
            return "TopNOperator[count="
                + topCount
                + ", elementTypes="
                + elementTypes
                + ", encoders="
                + encoders
                + ", sortOrders="
                + sortOrders
                + ", inputOrdering="
                + inputOrdering
                + "]";
        }
    }

    private final BlockFactory blockFactory;
    private final CircuitBreaker breaker;

    private final int maxPageSize;

    private final List<ElementType> elementTypes;
    private final List<TopNEncoder> encoders;
    private final List<SortOrder> sortOrders;
    private final boolean[] channelInKey;

    private Queue inputQueue;
    private Row spare;
    private int spareValuesPreAllocSize = 0;
    private int spareKeysPreAllocSize = 0;

    private ReleasableIterator<Page> output;

    private long receiveNanos;
    private long emitNanos;

    /**
     * Count of pages that have been received by this operator.
     */
    private int pagesReceived;

    /**
     * Count of pages that have been emitted by this operator.
     */
    private int pagesEmitted;

    /**
     * Count of rows this operator has received.
     */
    private long rowsReceived;

    /**
     * Count of rows this operator has emitted.
     */
    private long rowsEmitted;

    private final InputOrdering inputOrdering;

    public TopNOperator(
        BlockFactory blockFactory,
        CircuitBreaker breaker,
        int topCount,
        List<ElementType> elementTypes,
        List<TopNEncoder> encoders,
        List<SortOrder> sortOrders,
        int maxPageSize,
        InputOrdering inputOrdering
    ) {
        this.blockFactory = blockFactory;
        this.breaker = breaker;
        this.maxPageSize = maxPageSize;
        this.elementTypes = elementTypes;
        this.encoders = encoders;
        this.sortOrders = sortOrders;
        this.inputQueue = Queue.build(breaker, topCount);
        this.inputOrdering = inputOrdering;
        this.channelInKey = new boolean[elementTypes.size()];
        for (SortOrder so : sortOrders) {
            channelInKey[so.channel] = true;
        }
    }

    @Override
    public boolean needsInput() {
        return output == null;
    }

    @Override
    public void addInput(Page page) {
        long start = System.nanoTime();
        /*
         * Since row tracks memory we have to be careful to close any unused rows,
         * including any rows that fail while constructing because they allocate
         * too much memory. The simplest way to manage that is this try/finally
         * block and the mutable row variable here.
         *
         * If you exit the try/finally block and row is non-null it's always unused
         * and must be closed. That happens either because it's overflow from the
         * inputQueue or because we hit an allocation failure while building it.
         */
        try {
            if (inputQueue.topCount <= 0) {
                return;
            }
            RowFiller rowFiller = new RowFiller(elementTypes, encoders, sortOrders, channelInKey, page);

            for (int i = 0; i < page.getPositionCount(); i++) {
                if (spare == null) {
                    spare = new Row(breaker, spareKeysPreAllocSize, spareValuesPreAllocSize);
                } else {
                    spare.keys.clear();
                    spare.values.clear();
                    spare.clearRefCounters();
                }
                rowFiller.writeKey(i, spare);

                // When rows are very long, appending the values one by one can lead to lots of allocations.
                // To avoid this, pre-allocate at least as much size as in the last seen row.
                // Let the pre-allocation size decay in case we only have 1 huge row and smaller rows otherwise.
                spareKeysPreAllocSize = Math.max(spare.keys.length(), spareKeysPreAllocSize / 2);

                // This is `inputQueue.insertWithOverflow` with followed by filling in the value only if we inserted.
                if (inputQueue.size() < inputQueue.topCount) {
                    // Heap not yet full, just add elements
                    rowFiller.writeValues(i, spare);
                    spareValuesPreAllocSize = Math.max(spare.values.length(), spareValuesPreAllocSize / 2);
                    inputQueue.add(spare);
                    spare = null;
                } else if (inputQueue.lessThan(inputQueue.top(), spare)) {
                    // Heap full AND this node fit in it.
                    Row nextSpare = inputQueue.top();
                    rowFiller.writeValues(i, spare);
                    spareValuesPreAllocSize = Math.max(spare.values.length(), spareValuesPreAllocSize / 2);
                    inputQueue.updateTop(spare);
                    spare = nextSpare;
                } else if (inputOrdering == InputOrdering.SORTED) {
                    /*
                     The queue (min-heap) is full and we have sorted input for the input page. Any other element that comes after the one
                     we just compared will be greater or equal than any other one in the queue, so we can short circuit the execution here.

                     Note we always need to check whether the min-heap top's is greater or equal than the current element. For example: we
                     could have processed all the data from a first data node, have a full queue (a partial result), but a page from a
                     second data node could interleave with our partial result in arbitrary ways.
                     */
                    break;
                }
            }
        } finally {
            page.releaseBlocks();
            pagesReceived++;
            rowsReceived += page.getPositionCount();
            receiveNanos += System.nanoTime() - start;
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
        Releasables.closeExpectNoException(
            /*
             * The spare is used during most collections. It's cleared when this Operator
             * is finish()ed. So it could be null here.
             */
            spare,
            /*
             * The inputQueue is a min heap of all live rows. Closing it will close all
             * the rows it contains and all decrement the breaker for the size of
             * the heap itself.
             */
            inputQueue,
            /*
             * If we're in the process of outputting pages then output will contain all
             * allocated but un-emitted rows.
             */
            output
        );
        // Aggressively null these so they can be GCed more quickly.
        inputQueue = null;
        output = null;
    }

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(TopNOperator.class) + RamUsageEstimator
        .shallowSizeOfInstance(List.class) * 3;

    @Override
    public long ramBytesUsed() {
        // NOTE: this is ignoring the output iterator for now. Pages are not Accountable. Yet.
        long arrHeader = RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
        long ref = RamUsageEstimator.NUM_BYTES_OBJECT_REF;
        long size = SHALLOW_SIZE;
        // These lists may slightly under-count, but it's not likely to be by much.
        size += RamUsageEstimator.alignObjectSize(arrHeader + ref * elementTypes.size());
        size += RamUsageEstimator.alignObjectSize(arrHeader + ref * encoders.size());
        size += RamUsageEstimator.alignObjectSize(arrHeader + ref * sortOrders.size());
        size += sortOrders.size() * SortOrder.SHALLOW_SIZE;
        if (inputQueue != null) {
            size += inputQueue.ramBytesUsed();
        }
        return size;
    }

    @Override
    public Status status() {
        return new TopNOperatorStatus(
            receiveNanos,
            emitNanos,
            inputQueue != null ? inputQueue.size() : 0,
            ramBytesUsed(),
            pagesReceived,
            pagesEmitted,
            rowsReceived,
            rowsEmitted
        );
    }

    @Override
    public String toString() {
        return "TopNOperator[count="
            + inputQueue
            + ", elementTypes="
            + elementTypes
            + ", encoders="
            + encoders
            + ", sortOrders="
            + sortOrders
            + ", inputOrdering="
            + inputOrdering
            + "]";
    }

    private static class Queue extends PriorityQueue<Row> implements Accountable, Releasable {
        private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(Queue.class);
        private final CircuitBreaker breaker;
        private final int topCount;

        /**
         * Track memory usage in the breaker then build the {@link Queue}.
         */
        static Queue build(CircuitBreaker breaker, int topCount) {
            breaker.addEstimateBytesAndMaybeBreak(Queue.sizeOf(topCount), "esql engine topn");
            return new Queue(breaker, topCount);
        }

        private Queue(CircuitBreaker breaker, int topCount) {
            super(topCount);
            this.breaker = breaker;
            this.topCount = topCount;
        }

        @Override
        protected boolean lessThan(Row lhs, Row rhs) {
            return lhs.compareTo(rhs) < 0;
        }

        @Override
        public String toString() {
            return size() + "/" + topCount;
        }

        @Override
        public long ramBytesUsed() {
            long total = SHALLOW_SIZE;
            total += RamUsageEstimator.alignObjectSize(
                RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + RamUsageEstimator.NUM_BYTES_OBJECT_REF * ((long) topCount + 1)
            );
            for (Row r : this) {
                total += r == null ? 0 : r.ramBytesUsed();
            }
            return total;
        }

        @Override
        public void close() {
            Releasables.close(
                /*
                 * Release all entries in the topn, nulling references to each row after closing them
                 * so they can be GC immediately. Without this nulling very large heaps can race with
                 * the circuit breaker itself. With this we're still racing, but we're only racing a
                 * single row at a time. And single rows can only be so large. And we have enough slop
                 * to live with being inaccurate by one row.
                 */
                () -> {
                    for (int i = 0; i < getHeapArray().length; i++) {
                        Row row = (Row) getHeapArray()[i];
                        if (row != null) {
                            row.close();
                            getHeapArray()[i] = null;
                        }
                    }
                },
                // Release the array itself
                () -> breaker.addWithoutBreaking(-Queue.sizeOf(topCount))
            );
        }

        private static long sizeOf(int topCount) {
            long total = SHALLOW_SIZE;
            total += RamUsageEstimator.alignObjectSize(
                RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + RamUsageEstimator.NUM_BYTES_OBJECT_REF * ((long) topCount + 1)
            );
            return total;
        }
    }

    /**
     * Build the result iterator. Moves all rows from the {@link #inputQueue} and
     * {@link #close}s it.
     */
    private ReleasableIterator<Page> buildResult() {
        if (spare != null) {
            // Remove the spare, we're never going to use it again.
            spare.close();
            spare = null;
        }

        if (inputQueue.size() == 0) {
            return ReleasableIterator.empty();
        }

        List<Row> rows = new ArrayList<>(inputQueue.size());
        while (inputQueue.size() > 0) {
            rows.add(inputQueue.pop());
        }
        Collections.reverse(rows);
        inputQueue.close();
        inputQueue = null;
        return new Result(rows);
    }

    private class Result implements ReleasableIterator<Page> {
        private final List<Row> rows;
        private int r;

        private Result(List<Row> rows) {
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
                    try (Row row = rows.set(r++, null)) {
                        readKeys(builders, row.keys.bytesRefView());
                        readValues(builders, row.values.bytesRefView());
                    }
                }

                Block[] blocks = new Block[builders.length];
                try {
                    for (int b = 0; b < blocks.length; b++) {
                        blocks[b] = builders[b].build();
                    }
                } finally {
                    if (blocks[blocks.length - 1] == null) {
                        Releasables.closeExpectNoException(blocks);
                    }
                }
                Releasables.closeExpectNoException(builders);
                return new Page(blocks);
            } finally {
                Releasables.close(builders);
                emitNanos += System.nanoTime() - start;
            }
        }

        @Override
        public void close() {
            Releasables.close(rows);
        }

        /**
         * Read keys into the results. See {@link KeyExtractor} for the key layout.
         */
        private void readKeys(ResultBuilder[] builders, BytesRef keys) {
            for (SortOrder so : sortOrders) {
                if (keys.bytes[keys.offset] == so.nul()) {
                    // Discard the null byte.
                    keys.offset++;
                    keys.length--;
                    continue;
                }
                // Discard the non_null byte.
                keys.offset++;
                keys.length--;
                // Read the key. This will modify offset and length for the next iteration.
                builders[so.channel].decodeKey(keys, so.asc);
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
