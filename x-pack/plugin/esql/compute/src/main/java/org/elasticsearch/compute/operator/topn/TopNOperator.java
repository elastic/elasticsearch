/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Iterator;
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

    /**
     * Internal row to be used in the PriorityQueue instead of the full blown Page.
     * It mirrors somehow the Block build in the sense that it keeps around an array of offsets and a count of values (to account for
     * multivalues) to reference each position in each block of the Page.
     */
    static final class Row implements Accountable {
        private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(Row.class) + 2 * (RamUsageEstimator
            .shallowSizeOfInstance(BytesRefBuilder.class) + RamUsageEstimator.shallowSizeOfInstance(BytesRef.class)) + RamUsageEstimator
                .shallowSizeOfInstance(BitSet.class);

        final BytesRefBuilder keys = new BytesRefBuilder(); // BytesRef used to sort rows between each other
        /**
         * A true/false value (bit set/unset) for each byte in the BytesRef above corresponding to an asc/desc ordering.
         * For ex, if a Long is represented as 8 bytes, each of these bytes will have the same value (set/unset) if the respective Long
         * value is used for sorting ascending/descending.
         */
        final BitSet orderByCompositeKeyAscending = new BitSet();

        final BytesRefBuilder values = new BytesRefBuilder();

        @Override
        public long ramBytesUsed() {
            return SHALLOW_SIZE + RamUsageEstimator.sizeOf(keys.bytes()) + orderByCompositeKeyAscending.size() / Byte.SIZE
                + RamUsageEstimator.sizeOf(values.bytes());
        }
    }

    record KeyFactory(KeyExtractor extractor, boolean ascending) {}

    static final class RowFactory {
        private final ValueExtractor[] valueExtractors;
        private final KeyFactory[] keyFactories;

        RowFactory(List<ElementType> elementTypes, List<TopNEncoder> encoders, List<SortOrder> sortOrders, Page page) {
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

        Row row(int position, Row spare) {
            Row result;
            if (spare == null) {
                result = new Row();
            } else {
                result = spare;
                result.keys.clear();
                result.orderByCompositeKeyAscending.clear();
                result.values.clear();
            }

            writeKey(position, result);
            writeValues(position, result.values);

            return result;
        }

        private void writeKey(int position, Row row) {
            int orderByCompositeKeyCurrentPosition = 0;
            for (KeyFactory factory : keyFactories) {
                int valueAsBytesSize = factory.extractor.writeKey(row.keys, position);
                row.orderByCompositeKeyAscending.set(
                    orderByCompositeKeyCurrentPosition,
                    valueAsBytesSize + orderByCompositeKeyCurrentPosition,
                    factory.ascending
                );
                orderByCompositeKeyCurrentPosition += valueAsBytesSize;
            }
        }

        private void writeValues(int position, BytesRefBuilder values) {
            for (ValueExtractor e : valueExtractors) {
                e.writeValue(values, position);
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
        int maxPageSize
    ) implements OperatorFactory {
        public TopNOperatorFactory

        {
            for (ElementType e : elementTypes) {
                if (e == null) {
                    throw new IllegalArgumentException("ElementType not known");
                }
            }
        }

        @Override
        public TopNOperator get(DriverContext driverContext) {
            return new TopNOperator(topCount, elementTypes, encoders, sortOrders, maxPageSize);
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
                + "]";
        }
    }

    private final Queue inputQueue;

    private final int maxPageSize;

    private final List<ElementType> elementTypes;
    private final List<TopNEncoder> encoders;
    private final List<SortOrder> sortOrders;

    private Iterator<Page> output;

    public TopNOperator(
        int topCount,
        List<ElementType> elementTypes,
        List<TopNEncoder> encoders,
        List<SortOrder> sortOrders,
        int maxPageSize
    ) {
        this.maxPageSize = maxPageSize;
        this.elementTypes = elementTypes;
        this.encoders = encoders;
        this.sortOrders = sortOrders;
        this.inputQueue = new Queue(topCount);
    }

    static int compareRows(Row r1, Row r2) {
        // This is similar to r1.key.compareTo(r2.key) but stopping somewhere in the middle so that
        // we check the byte that mismatched
        BytesRef br1 = r1.keys.get();
        BytesRef br2 = r2.keys.get();
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
                return r2.orderByCompositeKeyAscending.get(length) ? 1 : -1;
            } else {// second row is less than the first row
                return r1.orderByCompositeKeyAscending.get(length) ? -1 : 1;
            }
        } else {
            // compare the byte that mismatched accounting for that respective byte asc/desc ordering
            int c = Byte.compareUnsigned(
                r1.keys.bytes()[br1.offset + mismatchedByteIndex],
                r2.keys.bytes()[br2.offset + mismatchedByteIndex]
            );
            return r1.orderByCompositeKeyAscending.get(mismatchedByteIndex) ? -c : c;
        }
    }

    @Override
    public boolean needsInput() {
        return output == null;
    }

    @Override
    public void addInput(Page page) {
        RowFactory rowFactory = new RowFactory(elementTypes, encoders, sortOrders, page);

        Row removed = null;
        for (int i = 0; i < page.getPositionCount(); i++) {
            Row x = rowFactory.row(i, removed);
            removed = inputQueue.insertWithOverflow(x);
        }
    }

    @Override
    public void finish() {
        if (output == null) {
            output = toPages();
        }
    }

    private Iterator<Page> toPages() {
        if (inputQueue.size() == 0) {
            return Collections.emptyIterator();
        }
        List<Row> list = new ArrayList<>(inputQueue.size());
        while (inputQueue.size() > 0) {
            list.add(inputQueue.pop());
        }
        Collections.reverse(list);

        List<Page> result = new ArrayList<>();
        ResultBuilder[] builders = null;
        int p = 0;
        int size = 0;
        for (int i = 0; i < list.size(); i++) {
            if (builders == null) {
                size = Math.min(maxPageSize, list.size() - i);
                builders = new ResultBuilder[elementTypes.size()];
                for (int b = 0; b < builders.length; b++) {
                    builders[b] = ResultBuilder.resultBuilderFor(
                        elementTypes.get(b),
                        encoders.get(b).toUnsortable(),
                        channelInKey(sortOrders, b),
                        size
                    );
                }
                p = 0;
            }

            Row row = list.get(i);
            BytesRef keys = row.keys.get();
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

            BytesRef values = row.values.get();
            for (ResultBuilder builder : builders) {
                builder.decodeValue(values);
            }
            if (values.length != 0) {
                throw new IllegalArgumentException("didn't read all values");
            }

            p++;
            if (p == size) {
                result.add(new Page(Arrays.stream(builders).map(ResultBuilder::build).toArray(Block[]::new)));
                builders = null;
            }
        }
        assert builders == null;
        return result.iterator();
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
        if (output != null && output.hasNext()) {
            return output.next();
        }
        return null;
    }

    @Override
    public void close() {

    }

    private static long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(TopNOperator.class) + RamUsageEstimator
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
        size += inputQueue.ramBytesUsed();
        return size;
    }

    @Override
    public Status status() {
        return new TopNOperatorStatus(inputQueue.size(), ramBytesUsed());
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
            + "]";
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
