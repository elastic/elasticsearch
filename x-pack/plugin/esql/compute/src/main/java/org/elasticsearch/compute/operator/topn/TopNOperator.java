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
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;
import java.util.List;

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

    static final class BytesOrder implements Releasable, Accountable {
        private static final long BASE_RAM_USAGE = RamUsageEstimator.shallowSizeOfInstance(BytesOrder.class);
        private final CircuitBreaker breaker;
        final List<SortOrder> sortOrders;
        final int[] endOffsets;

        BytesOrder(List<SortOrder> sortOrders, CircuitBreaker breaker, String label) {
            this.breaker = breaker;
            this.sortOrders = sortOrders;
            breaker.addEstimateBytesAndMaybeBreak(memoryUsed(sortOrders.size()), label);
            this.endOffsets = new int[sortOrders.size()];
        }

        /**
         * Returns true if the byte at the given position is ordered ascending; otherwise, return false
         */
        boolean isByteOrderAscending(int bytePosition) {
            int index = Arrays.binarySearch(endOffsets, bytePosition);
            if (index < 0) {
                index = -1 - index;
            }
            return sortOrders.get(index).asc();
        }

        private long memoryUsed(int numKeys) {
            // sortOrders is global and its memory is accounted at the top level TopNOperator
            return BASE_RAM_USAGE + RamUsageEstimator.alignObjectSize(
                (long) RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) Integer.BYTES * numKeys
            );
        }

        @Override
        public long ramBytesUsed() {
            return memoryUsed(sortOrders.size());
        }

        @Override
        public void close() {
            breaker.addWithoutBreaking(-ramBytesUsed());
        }
    }

    public record SortOrder(int channel, boolean asc, boolean nullsFirst) {

        private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(SortOrder.class);

        @Override
        public String toString() {
            return "SortOrder[channel=" + this.channel + ", asc=" + this.asc + ", nullsFirst=" + this.nullsFirst + "]";
        }

        byte nul() {
            if (nullsFirst) {
                return asc ? SMALL_NULL : BIG_NULL;
            } else {
                return asc ? BIG_NULL : SMALL_NULL;
            }
        }

        byte nonNul() {
            if (nullsFirst) {
                return asc ? BIG_NULL : SMALL_NULL;
            } else {
                return asc ? SMALL_NULL : BIG_NULL;
            }
        }
    }

    public record TopNOperatorFactory(
        int topCount,
        List<ElementType> elementTypes,
        List<TopNEncoder> encoders,
        List<SortOrder> sortOrders,
        List<Integer> groupKeys,
        int maxPageSize
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
                groupKeys.stream().mapToInt(Integer::intValue).toArray(),
                maxPageSize
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
                + (groupKeys.isEmpty() ? "" : ", groupKeys=" + groupKeys)
                + "]";
        }
    }

    private final BlockFactory blockFactory;
    private final CircuitBreaker breaker;

    private final int maxPageSize;

    private final List<ElementType> elementTypes;
    private final List<TopNEncoder> encoders;
    private final List<SortOrder> sortOrders;
    private final int[] groupKeys;

    private TopNQueue inputQueue;
    private Row spare;

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

    public TopNOperator(
        BlockFactory blockFactory,
        CircuitBreaker breaker,
        int topCount,
        List<ElementType> elementTypes,
        List<TopNEncoder> encoders,
        List<SortOrder> sortOrders,
        int[] groupKeys,
        int maxPageSize
    ) {
        this.blockFactory = blockFactory;
        this.breaker = breaker;
        this.maxPageSize = maxPageSize;
        this.elementTypes = elementTypes;
        this.encoders = encoders;
        this.sortOrders = sortOrders;
        this.groupKeys = groupKeys;
        this.processor = groupKeys.length == 0 ? new UngroupedTopNProcessor() : new GroupedTopNProcessor(groupKeys);
        this.inputQueue = processor.queue(breaker, topCount);
    }

    static int compareRows(Row r1, Row r2) {
        // This is similar to r1.key.compareTo(r2.key) but stopping somewhere in the middle so that
        // we check the byte that mismatched
        BytesRef br1 = r1.keys().bytesRefView();
        BytesRef br2 = r2.keys().bytesRefView();
        int mismatchedByteIndex = Arrays.mismatch(
            br1.bytes,
            br1.offset,
            br1.offset + br1.length,
            br2.bytes,
            br2.offset,
            br2.offset + br2.length
        );
        if (mismatchedByteIndex < 0) {
            // the two rows are equal
            return 0;
        }

        int length = Math.min(br1.length, br2.length);
        // one value is the prefix of the other
        if (mismatchedByteIndex == length) {
            // the value with the greater length is considered greater than the other
            if (length == br1.length) {// first row is less than the second row
                return r2.bytesOrder().isByteOrderAscending(length) ? 1 : -1;
            } else {// second row is less than the first row
                return r1.bytesOrder().isByteOrderAscending(length) ? -1 : 1;
            }
        } else {
            // compare the byte that mismatched accounting for that respective byte asc/desc ordering
            int c = Byte.compareUnsigned(br1.bytes[br1.offset + mismatchedByteIndex], br2.bytes[br2.offset + mismatchedByteIndex]);
            return r1.bytesOrder().isByteOrderAscending(mismatchedByteIndex) ? -c : c;
        }
    }

    @Override
    public boolean needsInput() {
        return output == null;
    }

    private final TopNProcessor processor;

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
            RowFiller rowFiller = processor.rowFiller(elementTypes, encoders, sortOrders, page);

            for (int i = 0; i < page.getPositionCount(); i++) {
                if (spare == null) {
                    spare = processor.row(breaker, sortOrders, rowFiller);
                } else {
                    spare.clear();
                }
                rowFiller.writeKey(i, spare);

                var nextSpare = inputQueue.addRow(spare);
                if (nextSpare != spare) {
                    var insertedRow = spare;
                    spare = nextSpare; // Update spare before writing values in case the writing fails, to avoid releasing spare twice.
                    rowFiller.writeValues(i, insertedRow);
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

    static boolean channelInKey(List<SortOrder> sortOrders, int channel) {
        for (SortOrder so : sortOrders) {
            if (so.channel == channel) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isFinished() {
        return output != null && output.hasNext() == false;
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
        size += RamUsageEstimator.sizeOf(groupKeys);
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
            + (groupKeys.length == 0 ? "" : ", groupKeys=" + Arrays.toString(groupKeys))
            + "]";
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

        List<Row> rows = inputQueue.popAll();
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
                    builders[b] = ResultBuilder.resultBuilderFor(
                        blockFactory,
                        elementTypes.get(b),
                        encoders.get(b).toUnsortable(),
                        channelInKey(sortOrders, b),
                        size
                    );
                }
                int rEnd = r + size;
                while (r < rEnd) {
                    try (Row row = rows.set(r++, null)) {
                        readKeys(builders, row.keys().bytesRefView());
                        readValues(builders, row.values().bytesRefView());
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
                builders[so.channel].decodeKey(keys);
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
