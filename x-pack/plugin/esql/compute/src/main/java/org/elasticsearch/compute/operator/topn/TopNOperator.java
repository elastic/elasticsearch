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
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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

    /**
     * Internal row to be used in the PriorityQueue instead of the full blown Page.
     * It mirrors somehow the Block build in the sense that it keeps around an array of offsets and a count of values (to account for
     * multivalues) to reference each position in each block of the Page.
     */
    static final class Row implements Accountable, Releasable {
        private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(Row.class);

        /**
         * The sort key.
         */
        final BreakingBytesRefBuilder keys;

        /**
         * A true/false value (bit set/unset) for each byte in the BytesRef above corresponding to an asc/desc ordering.
         * For ex, if a Long is represented as 8 bytes, each of these bytes will have the same value (set/unset) if the respective Long
         * value is used for sorting ascending/descending.
         */
        final BytesOrder bytesOrder;

        /**
         * Values to reconstruct the row. Sort of. When we reconstruct the row we read
         * from both the {@link #keys} and the {@link #values}. So this only contains
         * what is required to reconstruct the row that isn't already stored in {@link #values}.
         */
        final BreakingBytesRefBuilder values;

        Row(CircuitBreaker breaker, List<SortOrder> sortOrders, int preAllocatedKeysSize, int preAllocatedValueSize) {
            boolean success = false;
            try {
                keys = new BreakingBytesRefBuilder(breaker, "topn", preAllocatedKeysSize);
                values = new BreakingBytesRefBuilder(breaker, "topn", preAllocatedValueSize);
                bytesOrder = new BytesOrder(sortOrders, breaker, "topn");
                success = true;
            } finally {
                if (success == false) {
                    close();
                }
            }
        }

        @Override
        public long ramBytesUsed() {
            return SHALLOW_SIZE + keys.ramBytesUsed() + bytesOrder.ramBytesUsed() + values.ramBytesUsed();
        }

        @Override
        public void close() {
            Releasables.closeExpectNoException(keys, values, bytesOrder);
        }
    }

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

    record KeyFactory(KeyExtractor extractor, boolean ascending) {}

    static final class RowFiller {
        private final ValueExtractor[] valueExtractors;
        private final KeyFactory[] keyFactories;

        RowFiller(List<ElementType> elementTypes, List<TopNEncoder> encoders, List<SortOrder> sortOrders, Page page) {
            valueExtractors = new ValueExtractor[page.getBlockCount()];
            for (int b = 0; b < valueExtractors.length; b++) {
                valueExtractors[b] = ValueExtractor.extractorFor(
                    elementTypes.get(b),
                    encoders.get(b).toUnsortable(),
                    channelInKey(sortOrders, b),
                    page.getBlock(b)
                );
            }
            keyFactories = new KeyFactory[sortOrders.size()];
            for (int k = 0; k < keyFactories.length; k++) {
                SortOrder so = sortOrders.get(k);
                KeyExtractor extractor = KeyExtractor.extractorFor(
                    elementTypes.get(so.channel),
                    encoders.get(so.channel).toSortable(),
                    so.asc,
                    so.nul(),
                    so.nonNul(),
                    page.getBlock(so.channel)
                );
                keyFactories[k] = new KeyFactory(extractor, so.asc);
            }
        }

        /**
         * Fill a {@link Row} for {@code position}.
         */
        void row(int position, Row destination) {
            writeKey(position, destination);
            writeValues(position, destination.values);
        }

        private void writeKey(int position, Row row) {
            int orderByCompositeKeyCurrentPosition = 0;
            for (int i = 0; i < keyFactories.length; i++) {
                int valueAsBytesSize = keyFactories[i].extractor.writeKey(row.keys, position);
                assert valueAsBytesSize > 0 : valueAsBytesSize;
                orderByCompositeKeyCurrentPosition += valueAsBytesSize;
                row.bytesOrder.endOffsets[i] = orderByCompositeKeyCurrentPosition - 1;
            }
        }

        private void writeValues(int position, BreakingBytesRefBuilder values) {
            for (ValueExtractor e : valueExtractors) {
                e.writeValue(values, position);
            }
        }
    }

    public record Partition(int channel) {

        private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(Partition.class);

        @Override
        public String toString() {
            return "Partition[channel=" + this.channel + "]";
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
        List<Partition> partitions,
        List<SortOrder> sortOrders,
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
                partitions,
                sortOrders,
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
                + ", partitions="
                + partitions
                + ", sortOrders="
                + sortOrders
                + "]";
        }
    }

    private final BlockFactory blockFactory;
    private final CircuitBreaker breaker;
    private final Map<BytesRef, Queue> inputQueues;

    private final int topCount;
    private final int maxPageSize;

    private final List<ElementType> elementTypes;
    private final List<TopNEncoder> encoders;
    private final List<Partition> partitions;
    private final List<SortOrder> sortOrders;

    private Row spare;
    private int spareValuesPreAllocSize = 0;
    private int spareKeysPreAllocSize = 0;

    private Iterator<Page> output;

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
        List<Partition> partitions,
        List<SortOrder> sortOrders,
        int maxPageSize
    ) {
        this.blockFactory = blockFactory;
        this.breaker = breaker;
        this.topCount = topCount;
        this.maxPageSize = maxPageSize;
        this.elementTypes = elementTypes;
        this.encoders = encoders;
        this.partitions = partitions;
        this.sortOrders = sortOrders;
        this.inputQueues = new TreeMap<>();
    }

    static int compareRows(Row r1, Row r2) {
        // This is similar to r1.key.compareTo(r2.key) but stopping somewhere in the middle so that
        // we check the byte that mismatched
        BytesRef br1 = r1.keys.bytesRefView();
        BytesRef br2 = r2.keys.bytesRefView();
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
                return r2.bytesOrder.isByteOrderAscending(length) ? 1 : -1;
            } else {// second row is less than the first row
                return r1.bytesOrder.isByteOrderAscending(length) ? -1 : 1;
            }
        } else {
            // compare the byte that mismatched accounting for that respective byte asc/desc ordering
            int c = Byte.compareUnsigned(br1.bytes[br1.offset + mismatchedByteIndex], br2.bytes[br2.offset + mismatchedByteIndex]);
            return r1.bytesOrder.isByteOrderAscending(mismatchedByteIndex) ? -c : c;
        }
    }

    @Override
    public boolean needsInput() {
        return output == null;
    }

    @Override
    public void addInput(Page page) {
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
            RowFiller rowFiller = new RowFiller(elementTypes, encoders, sortOrders, page);

            for (int i = 0; i < page.getPositionCount(); i++) {
                if (spare == null) {
                    spare = new Row(breaker, sortOrders, spareKeysPreAllocSize, spareValuesPreAllocSize);
                } else {
                    spare.keys.clear();
                    spare.values.clear();
                }
                rowFiller.row(i, spare);

                // When rows are very long, appending the values one by one can lead to lots of allocations.
                // To avoid this, pre-allocate at least as much size as in the last seen row.
                // Let the pre-allocation size decay in case we only have 1 huge row and smaller rows otherwise.
                spareKeysPreAllocSize = Math.max(spare.keys.length(), spareKeysPreAllocSize / 2);
                spareValuesPreAllocSize = Math.max(spare.values.length(), spareValuesPreAllocSize / 2);

                BytesRef partitionKey = getPartitionKey(page, i);
                Queue inputQueue = inputQueues.computeIfAbsent(partitionKey, key -> new Queue(topCount));
                spare = inputQueue.insertWithOverflow(spare);
            }
        } finally {
            page.releaseBlocks();
            pagesReceived++;
            rowsReceived += page.getPositionCount();
        }
    }

    /**
     * Calculates the partition key of the i-th row of the given page.
     *
     * @param page page for which the partition key should be calculated
     * @param i row index
     * @return partition key of the i-th row of the given page
     */
    private BytesRef getPartitionKey(Page page, int i) {
        if (partitions.isEmpty()) {
            return new BytesRef();
        }
        assert page.getPositionCount() > 0;
        BreakingBytesRefBuilder builder = new BreakingBytesRefBuilder(breaker, "topn");
        for (Partition partition : partitions) {
            try (var block = page.getBlock(partition.channel)) {
                BytesRef partitionFieldValue = ((BytesRefBlock) block).getBytesRef(i, new BytesRef());
                builder.append(partitionFieldValue);
            }
        }
        return builder.bytesRefView();
    }

    @Override
    public void finish() {
        if (output == null) {
            output = toPages();
        }
    }

    private Iterator<Page> toPages() {
        if (spare != null) {
            // Remove the spare, we're never going to use it again.
            spare.close();
            spare = null;
        }
        boolean success = false;
        List<Row> list = null;
        ResultBuilder[] builders = null;
        List<Page> result = new ArrayList<>();
        // TODO: optimize case where all the queues are empty
        try {
            for (var entry : inputQueues.entrySet()) {
                Queue inputQueue = entry.getValue();

                list = new ArrayList<>(inputQueue.size());
                builders = null;
                while (inputQueue.size() > 0) {
                    list.add(inputQueue.pop());
                }
                Collections.reverse(list);

                int p = 0;
                int size = 0;
                for (int i = 0; i < list.size(); i++) {
                    if (builders == null) {
                        size = Math.min(maxPageSize, list.size() - i);
                        builders = new ResultBuilder[elementTypes.size()];
                        for (int b = 0; b < builders.length; b++) {
                            builders[b] = ResultBuilder.resultBuilderFor(
                                blockFactory,
                                elementTypes.get(b),
                                encoders.get(b).toUnsortable(),
                                channelInKey(sortOrders, b),
                                size
                            );
                        }
                        p = 0;
                    }

                    Row row = list.get(i);
                    BytesRef keys = row.keys.bytesRefView();
                    for (SortOrder so : sortOrders) {
                        if (keys.bytes[keys.offset] == so.nul()) {
                            keys.offset++;
                            keys.length--;
                            continue;
                        }
                        keys.offset++;
                        keys.length--;
                        builders[so.channel].decodeKey(keys);
                    }
                    if (keys.length != 0) {
                        throw new IllegalArgumentException("didn't read all keys");
                    }

                    BytesRef values = row.values.bytesRefView();
                    for (ResultBuilder builder : builders) {
                        builder.decodeValue(values);
                    }
                    if (values.length != 0) {
                        throw new IllegalArgumentException("didn't read all values");
                    }

                    list.set(i, null);
                    row.close();

                    p++;
                    if (p == size) {
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
                        result.add(new Page(blocks));
                        Releasables.closeExpectNoException(builders);
                        builders = null;
                    }
                }
                assert builders == null;
            }
            success = true;
            return result.iterator();
        } finally {
            if (success == false) {
                List<Releasable> close = new ArrayList<>(list);
                for (Page p : result) {
                    close.add(p::releaseBlocks);
                }
                Collections.addAll(close, builders);
                Releasables.closeExpectNoException(Releasables.wrap(close));
            }
        }
    }

    private static boolean channelInKey(List<SortOrder> sortOrders, int channel) {
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
        List<Releasable> releasables = new ArrayList<>();
        releasables.addAll(inputQueues.values().stream().map(Releasables::wrap).toList());
        releasables.add(output == null ? null : Releasables.wrap(() -> Iterators.map(output, p -> p::releaseBlocks)));
        /*
         * If we close before calling finish then spare and inputQueue will be live rows
         * that need closing. If we close after calling finish then the output iterator
         * will contain pages of results that have yet to be returned.
         */
        Releasables.closeExpectNoException(spare, Releasables.wrap(releasables));
    }

    private static long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(TopNOperator.class) + RamUsageEstimator
        .shallowSizeOfInstance(List.class) * 4 + RamUsageEstimator.shallowSizeOfInstance(Map.class);

    @Override
    public long ramBytesUsed() {
        // NOTE: this is ignoring the output iterator for now. Pages are not Accountable. Yet.
        long arrHeader = RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
        long ref = RamUsageEstimator.NUM_BYTES_OBJECT_REF;
        long size = SHALLOW_SIZE;
        // These lists may slightly under-count, but it's not likely to be by much.
        size += RamUsageEstimator.alignObjectSize(arrHeader + ref * elementTypes.size());
        size += RamUsageEstimator.alignObjectSize(arrHeader + ref * encoders.size());
        size += RamUsageEstimator.alignObjectSize(arrHeader + ref * partitions.size());
        size += partitions.size() * Partition.SHALLOW_SIZE;
        size += RamUsageEstimator.alignObjectSize(arrHeader + ref * sortOrders.size());
        size += sortOrders.size() * SortOrder.SHALLOW_SIZE;
        long ramBytesUsedSum = inputQueues.entrySet().stream().mapToLong(e -> e.getKey().bytes.length + e.getValue().ramBytesUsed()).sum();
        size += ramBytesUsedSum;
        return size;
    }

    @Override
    public Status status() {
        int queueSizeSum = inputQueues.values().stream().mapToInt(Queue::size).sum();
        return new TopNOperatorStatus(queueSizeSum, ramBytesUsed(), pagesReceived, pagesEmitted, rowsReceived, rowsEmitted);
    }

    @Override
    public String toString() {
        int queueSizeSum = inputQueues.values().stream().mapToInt(Queue::size).sum();
        return "TopNOperator[count="
            + queueSizeSum
            + "/"
            + topCount
            + ", elementTypes="
            + elementTypes
            + ", encoders="
            + encoders
            + ", partitions="
            + partitions
            + ", sortOrders="
            + sortOrders
            + "]";
    }

    CircuitBreaker breaker() {
        return breaker;
    }

    private static class Queue extends PriorityQueue<Row> implements Accountable {
        private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(Queue.class);
        private final int maxSize;

        Queue(int maxSize) {
            super(maxSize);
            this.maxSize = maxSize;
        }

        @Override
        protected boolean lessThan(Row r1, Row r2) {
            return compareRows(r1, r2) < 0;
        }

        @Override
        public String toString() {
            return size() + "/" + maxSize;
        }

        @Override
        public long ramBytesUsed() {
            long total = SHALLOW_SIZE;
            total += RamUsageEstimator.alignObjectSize(
                RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + RamUsageEstimator.NUM_BYTES_OBJECT_REF * (maxSize + 1)
            );
            for (Row r : this) {
                total += r == null ? 0 : r.ramBytesUsed();
            }
            return total;
        }
    }
}
