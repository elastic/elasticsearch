/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;

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
 * For each "special" encoding there is should be new TopNEncoder implementation. See {@link UTF8TopNEncoder} for encoding regular
 * "text" and "keyword" data types. See LocalExecutionPlanner for which data type uses which encoder.
 *
 * This Operator will not be able to sort binary values (encoded as BytesRef) because the bytes used as separator and "null"s can appear
 * as valid bytes inside a binary value.
 */
public class TopNOperator implements Operator {

    private static final byte SEPARATOR = 0x0; // separator for values inside the BytesRef sorting key
    private static final byte SMALL_NULL = 0x01; // "null" representation for "nulls first"
    private static final byte BIG_NULL = 0x02; // "null" representation for "nulls last"
    public static final TopNEncoder BYTESREF_FIXED_LENGTH_ENCODER = new FixedLengthTopNEncoder();
    public static final TopNEncoder BYTESREF_UTF8_ENCODER = new UTF8TopNEncoder();
    public static final TopNEncoder DEFAULT_ENCODER = new TopNEncoder() {
        @Override
        public void encodeBytesRef(BytesRef value, BytesRefBuilder bytesRefBuilder) {
            throw new IllegalStateException("Cannot find encoder for BytesRef value");
        }

        @Override
        public String toString() {
            return "DefaultEncoder";
        }
    };

    // enum to be extended in the future with other sorting modes (AVG average, for example)
    private enum MvSortMode {
        MIN,
        MAX
    }

    /**
     * Internal row to be used in the PriorityQueue instead of the full blown Page.
     * It mirrors somehow the Block build in the sense that it keeps around an array of offsets and a count of values (to account for
     * multivalues) to reference each position in each block of the Page.
     */
    static final class Row {
        boolean[] booleans;
        int[] ints;
        long[] longs;
        double[] doubles;
        BytesRef[] byteRefs;
        int[] docs;
        boolean[] nullValues;

        int[] idToFirstValueIndex; // keeps the offset inside each of the arrays above where a specific block position starts from
        ElementType[] idToType;
        int[] numberOfValues; // keeps the count of values of each field in the specialized array

        BitSet blockIsUnordered;
        BytesRefBuilder orderByCompositeKey = new BytesRefBuilder(); // BytesRef used to sort rows between each other
        /**
         * A true/false value (bit set/unset) for each byte in the BytesRef above corresponding to an asc/desc ordering.
         * For ex, if a Long is represented as 8 bytes, each of these bytes will have the same value (set/unset) if the respective Long
         * value is used for sorting ascending/descending.
         */
        BitSet orderByCompositeKeyAscending;

        boolean isNull(int i) {
            return nullValues[i];
        }

        boolean getBoolean(int i, int offset) {
            return booleans[idToFirstValueIndex[i] + offset];
        }

        int getInt(int i, int offset) {
            return ints[idToFirstValueIndex[i] + offset];
        }

        long getLong(int i, int offset) {
            return longs[idToFirstValueIndex[i] + offset];
        }

        double getDouble(int i, int offset) {
            return doubles[idToFirstValueIndex[i] + offset];
        }

        BytesRef getBytesRef(int i, int offset) {
            return byteRefs[idToFirstValueIndex[i] + offset];
        }

        boolean blockIsUnordered(int i) {
            return blockIsUnordered.get(i);
        }
    }

    static final class RowFactory {

        int size;
        int nBooleans;
        int nInts;
        int nLongs;
        int nDoubles;
        int nByteRefs;
        int nDocs;

        ElementType[] idToType;

        RowFactory(Page page) {
            size = page.getBlockCount();
            idToType = new ElementType[size];
            for (int i = 0; i < size; i++) {
                Block block = page.getBlock(i);
                switch (block.elementType()) {
                    case LONG -> nLongs++;
                    case INT -> nInts++;
                    case DOUBLE -> nDoubles++;
                    case BYTES_REF -> nByteRefs++;
                    case BOOLEAN -> nBooleans++;
                    case DOC -> nDocs++;
                    case NULL -> {
                    }
                    case UNKNOWN -> {
                        assert false : "Must not occur here as TopN should never receive intermediate blocks";
                        throw new UnsupportedOperationException("Block doesn't support retrieving elements");
                    }
                }
                idToType[i] = block.elementType();
            }
        }

        Row row(Page origin, int rowNum, Row spare, List<SortOrder> sortOrders) {
            Row result;
            if (spare == null) {
                result = new Row();
                result.nullValues = new boolean[size];
                result.booleans = new boolean[nBooleans];
                result.ints = new int[nInts];
                result.longs = new long[nLongs];
                result.doubles = new double[nDoubles];
                result.byteRefs = new BytesRef[nByteRefs];
                for (int i = 0; i < nByteRefs; i++) {
                    result.byteRefs[i] = new BytesRef();
                }
                result.idToFirstValueIndex = new int[size];
                result.idToType = idToType;
                result.docs = new int[nDocs * 3];
                result.numberOfValues = new int[size];
                result.orderByCompositeKeyAscending = new BitSet();
            } else {
                result = spare;
                // idToType has to be set because different pages could have different block types due to different mappings
                result.idToType = idToType;
                Arrays.fill(result.nullValues, false);
                result.orderByCompositeKey = new BytesRefBuilder();
                result.orderByCompositeKeyAscending.clear();
            }
            result.blockIsUnordered = new BitSet(size);

            int lastLongFirstValueIndex = 0;
            int lastIntFirstValueIndex = 0;
            int lastDoubleFirstValueIndex = 0;
            int lastBytesRefFirstValueIndex = 0;
            int lastBooleanFirstValueIndex = 0;
            int lastDocFirstValueIndex = 0;

            for (int i = 0; i < size; i++) {
                Block block = origin.getBlock(i);
                if (block.mvOrdering() == Block.MvOrdering.UNORDERED) {
                    result.blockIsUnordered.set(i);
                }
                if (block.isNull(rowNum)) {
                    result.nullValues[i] = true;
                } else {
                    int valuesCount = block.getValueCount(rowNum);
                    result.numberOfValues[i] = valuesCount;
                    switch (block.elementType()) {
                        case LONG -> {
                            int firstValueIndex = lastLongFirstValueIndex;
                            if (firstValueIndex + valuesCount > result.longs.length) {
                                result.longs = Arrays.copyOf(result.longs, firstValueIndex + valuesCount);
                            }
                            int start = block.getFirstValueIndex(rowNum);
                            int end = start + valuesCount;
                            for (int j = start, offset = 0; j < end; j++, offset++) {
                                result.longs[firstValueIndex + offset] = ((LongBlock) block).getLong(j);
                            }
                            result.idToFirstValueIndex[i] = firstValueIndex;
                            lastLongFirstValueIndex = firstValueIndex + valuesCount;
                        }
                        case INT -> {
                            int firstValueIndex = lastIntFirstValueIndex;
                            if (firstValueIndex + valuesCount > result.ints.length) {
                                result.ints = Arrays.copyOf(result.ints, firstValueIndex + valuesCount);
                            }
                            int start = block.getFirstValueIndex(rowNum);
                            int end = start + valuesCount;
                            for (int j = start, offset = 0; j < end; j++, offset++) {
                                result.ints[firstValueIndex + offset] = ((IntBlock) block).getInt(j);
                            }
                            result.idToFirstValueIndex[i] = firstValueIndex;
                            lastIntFirstValueIndex = firstValueIndex + valuesCount;
                        }
                        case DOUBLE -> {
                            int firstValueIndex = lastDoubleFirstValueIndex;
                            if (firstValueIndex + valuesCount > result.doubles.length) {
                                result.doubles = Arrays.copyOf(result.doubles, firstValueIndex + valuesCount);
                            }
                            int start = block.getFirstValueIndex(rowNum);
                            int end = start + valuesCount;
                            for (int j = start, offset = 0; j < end; j++, offset++) {
                                result.doubles[firstValueIndex + offset] = ((DoubleBlock) block).getDouble(j);
                            }
                            result.idToFirstValueIndex[i] = firstValueIndex;
                            lastDoubleFirstValueIndex = firstValueIndex + valuesCount;
                        }
                        case BYTES_REF -> {
                            int firstValueIndex = lastBytesRefFirstValueIndex;
                            if (firstValueIndex + valuesCount > result.byteRefs.length) {
                                int additionalSize = firstValueIndex + valuesCount - result.byteRefs.length;
                                result.byteRefs = Arrays.copyOf(result.byteRefs, firstValueIndex + valuesCount);
                                for (int j = 1; j <= additionalSize; j++) {
                                    result.byteRefs[result.byteRefs.length - j] = new BytesRef();
                                }
                            }
                            int start = block.getFirstValueIndex(rowNum);
                            int end = start + valuesCount;
                            for (int j = start, offset = 0; j < end; j++, offset++) {
                                BytesRef b = result.byteRefs[firstValueIndex + offset];
                                b = ((BytesRefBlock) block).getBytesRef(j, b);
                                result.byteRefs[firstValueIndex + offset] = b;
                            }
                            result.idToFirstValueIndex[i] = firstValueIndex;
                            lastBytesRefFirstValueIndex = firstValueIndex + valuesCount;
                        }
                        case BOOLEAN -> {
                            int firstValueIndex = lastBooleanFirstValueIndex;
                            if (firstValueIndex + valuesCount > result.booleans.length) {
                                result.booleans = Arrays.copyOf(result.booleans, firstValueIndex + valuesCount);
                            }
                            int start = block.getFirstValueIndex(rowNum);
                            int end = start + valuesCount;
                            for (int j = start, offset = 0; j < end; j++, offset++) {
                                result.booleans[firstValueIndex + offset] = ((BooleanBlock) block).getBoolean(j);
                            }
                            result.idToFirstValueIndex[i] = firstValueIndex;
                            lastBooleanFirstValueIndex = firstValueIndex + valuesCount;
                        }
                        case DOC -> {
                            int firstValueIndex = lastDocFirstValueIndex;
                            if (firstValueIndex + 3 > result.docs.length) {
                                result.docs = Arrays.copyOf(result.docs, firstValueIndex + 3);
                            }
                            DocVector doc = ((DocBlock) block).asVector();
                            result.docs[firstValueIndex] = doc.shards().getInt(rowNum);
                            result.docs[firstValueIndex + 1] = doc.segments().getInt(rowNum);
                            result.docs[firstValueIndex + 2] = doc.docs().getInt(rowNum);

                            result.idToFirstValueIndex[i] = firstValueIndex;
                            lastDocFirstValueIndex = firstValueIndex + 3;
                        }
                        case NULL -> {
                            assert false : "Must not occur here as we check nulls above already";
                            throw new UnsupportedOperationException("Block of nulls doesn't support comparison");
                        }
                        default -> {
                            assert false : "Must not occur here as TopN should never receive intermediate blocks";
                            throw new UnsupportedOperationException("Block doesn't support retrieving elements");
                        }

                    }
                }
            }

            int orderByCompositeKeyCurrentPosition = 0;
            for (SortOrder so : sortOrders) {
                byte nul, nonNul;
                if (so.nullsFirst) {
                    nul = so.asc ? SMALL_NULL : BIG_NULL;
                    nonNul = so.asc ? BIG_NULL : SMALL_NULL;
                } else {
                    nul = so.asc ? BIG_NULL : SMALL_NULL;
                    nonNul = so.asc ? SMALL_NULL : BIG_NULL;
                }

                MvSortMode sortMode = so.asc ? MvSortMode.MIN : MvSortMode.MAX;
                int mvOffset = result.blockIsUnordered(so.channel)
                    ? -1
                    : (sortMode == MvSortMode.MIN ? 0 : result.numberOfValues[so.channel] - 1);
                int valueAsBytesSize;

                if (result.isNull(so.channel)) {
                    result.orderByCompositeKey.append(nul);
                    valueAsBytesSize = 0;
                } else {
                    result.orderByCompositeKey.append(nonNul);
                    switch (result.idToType[so.channel]) {
                        case LONG -> {
                            long rowValue;
                            if (mvOffset >= 0) {
                                rowValue = result.getLong(so.channel, mvOffset);
                            } else {
                                rowValue = result.getLong(so.channel, 0);
                                for (int j = 1; j < result.numberOfValues[so.channel]; j++) {
                                    long value = result.getLong(so.channel, j);
                                    if (sortMode == MvSortMode.MIN) {
                                        rowValue = Math.min(value, rowValue);
                                    } else if (sortMode == MvSortMode.MAX) {
                                        rowValue = Math.max(value, rowValue);
                                    }
                                }
                            }
                            so.encoder.encodeLong(rowValue, result.orderByCompositeKey);
                            valueAsBytesSize = Long.BYTES;
                        }
                        case INT -> {
                            int rowValue;
                            if (mvOffset >= 0) {
                                rowValue = result.getInt(so.channel, mvOffset);
                            } else {
                                rowValue = result.getInt(so.channel, 0);
                                for (int j = 1; j < result.numberOfValues[so.channel]; j++) {
                                    int value = result.getInt(so.channel, j);
                                    if (sortMode == MvSortMode.MIN) {
                                        rowValue = Math.min(value, rowValue);
                                    } else if (sortMode == MvSortMode.MAX) {
                                        rowValue = Math.max(value, rowValue);
                                    }
                                }
                            }
                            so.encoder.encodeInteger(rowValue, result.orderByCompositeKey);
                            valueAsBytesSize = Integer.BYTES;
                        }
                        case DOUBLE -> {
                            double rowValue;
                            if (mvOffset >= 0) {
                                rowValue = result.getDouble(so.channel, mvOffset);
                            } else {
                                rowValue = result.getDouble(so.channel, 0);
                                for (int j = 1; j < result.numberOfValues[so.channel]; j++) {
                                    double value = result.getDouble(so.channel, j);
                                    if (sortMode == MvSortMode.MIN) {
                                        rowValue = Math.min(value, rowValue);
                                    } else if (sortMode == MvSortMode.MAX) {
                                        rowValue = Math.max(value, rowValue);
                                    }
                                }
                            }
                            so.encoder.encodeDouble(rowValue, result.orderByCompositeKey);
                            valueAsBytesSize = Long.BYTES;
                        }
                        case BYTES_REF -> {
                            BytesRef rowValue;
                            if (mvOffset >= 0) {
                                rowValue = result.getBytesRef(so.channel, mvOffset);
                            } else {
                                rowValue = result.getBytesRef(so.channel, 0);
                                for (int j = 1; j < result.numberOfValues[so.channel]; j++) {
                                    BytesRef value = result.getBytesRef(so.channel, j);
                                    int compare = value.compareTo(rowValue);
                                    if (sortMode == MvSortMode.MIN && compare < 0 || sortMode == MvSortMode.MAX && compare > 0) {
                                        rowValue = value;
                                    }
                                }
                            }
                            so.encoder.encodeBytesRef(rowValue, result.orderByCompositeKey);
                            valueAsBytesSize = rowValue.length;
                        }
                        case BOOLEAN -> {
                            boolean rowValue;
                            if (mvOffset >= 0) {
                                rowValue = result.getBoolean(so.channel, mvOffset);
                            } else {
                                rowValue = result.getBoolean(so.channel, 0);
                                for (int j = 1; j < result.numberOfValues[so.channel]
                                    && (sortMode == MvSortMode.MIN && rowValue || sortMode == MvSortMode.MAX && rowValue == false); j++) {
                                    boolean value = result.getBoolean(so.channel, j);
                                    if (sortMode == MvSortMode.MIN && value == false) {
                                        rowValue = false;
                                    } else if (sortMode == MvSortMode.MAX && value) {
                                        rowValue = true;
                                    }
                                }
                            }
                            so.encoder.encodeBoolean(rowValue, result.orderByCompositeKey);
                            valueAsBytesSize = 1;
                        }
                        default -> {
                            assert false : "Must not occur here as TopN should never receive intermediate blocks";
                            throw new UnsupportedOperationException("Block doesn't support retrieving elements");
                        }
                    }
                }
                result.orderByCompositeKeyAscending.set(
                    orderByCompositeKeyCurrentPosition,
                    valueAsBytesSize + orderByCompositeKeyCurrentPosition + 2,
                    so.asc
                );
                orderByCompositeKeyCurrentPosition += valueAsBytesSize + 2;
                result.orderByCompositeKey.append(SEPARATOR);
            }

            return result;
        }
    }

    public record SortOrder(int channel, boolean asc, boolean nullsFirst, TopNEncoder encoder) {

        public SortOrder(int channel, boolean asc, boolean nullsFirst) {
            this(channel, asc, nullsFirst, DEFAULT_ENCODER);
        }

        @Override
        public String toString() {
            return "SortOrder[channel="
                + this.channel
                + ", asc="
                + this.asc
                + ", nullsFirst="
                + this.nullsFirst
                + ", encoder="
                + this.encoder
                + "]";
        }
    }

    public record TopNOperatorFactory(int topCount, List<SortOrder> sortOrders, int maxPageSize) implements OperatorFactory {

        @Override
        public Operator get(DriverContext driverContext) {
            return new TopNOperator(topCount, sortOrders, maxPageSize);
        }

        @Override
        public String describe() {
            return "TopNOperator[count = " + topCount + ", sortOrders = " + sortOrders + "]";
        }
    }

    private final PriorityQueue<Row> inputQueue;

    private final int maxPageSize;
    private RowFactory rowFactory;

    private final List<SortOrder> sortOrders;

    // these will be inferred at runtime: one input page might not contain all the information needed
    // eg. it could be missing some fields in the mapping, so it could have NULL blocks as placeholders
    private ElementType[] outputTypes;

    private Iterator<Page> output;

    public TopNOperator(int topCount, List<SortOrder> sortOrders, int maxPageSize) {
        this.maxPageSize = maxPageSize;
        this.sortOrders = sortOrders;
        this.inputQueue = new PriorityQueue<>(topCount) {
            @Override
            protected boolean lessThan(Row r1, Row r2) {
                return compareRows(r1, r2) < 0;
            }

            @Override
            public String toString() {
                if (sortOrders.size() == 1) {
                    SortOrder order = sortOrders.get(0);
                    return "count = " + size() + "/" + topCount + ", sortOrder = " + order;
                } else {
                    return "count = " + size() + "/" + topCount + ", sortOrders = " + sortOrders;
                }
            }
        };
    }

    static int compareRows(Row r1, Row r2) {
        // This is simliar to r1.orderByCompositeKey.compareTo(r2.orderByCompositeKey) but stopping somewhere in the middle so that
        // we check the byte that mismatched
        BytesRef br1 = r1.orderByCompositeKey.get();
        BytesRef br2 = r2.orderByCompositeKey.get();
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
                r1.orderByCompositeKey.bytes()[br1.offset + mismatchedByteIndex],
                r2.orderByCompositeKey.bytes()[br2.offset + mismatchedByteIndex]
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
        // rebuild for every page, since blocks can originate from different indices, with different mapping
        rowFactory = new RowFactory(page);
        if (outputTypes == null) {
            outputTypes = Arrays.copyOf(rowFactory.idToType, rowFactory.idToType.length);
        } else {
            for (int i = 0; i < rowFactory.idToType.length; i++) {
                if (outputTypes[i] == ElementType.NULL) { // the type could just be missing in the previous mappings
                    outputTypes[i] = rowFactory.idToType[i];
                }
            }
        }

        Row removed = null;
        for (int i = 0; i < page.getPositionCount(); i++) {
            Row x = rowFactory.row(page, i, removed, sortOrders);
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
        Block.Builder[] builders = null;
        int p = 0;
        int size = 0;
        for (int i = 0; i < list.size(); i++) {
            if (builders == null) {
                size = Math.min(maxPageSize, list.size() - i);
                builders = new Block.Builder[rowFactory.size];
                for (int b = 0; b < builders.length; b++) {
                    builders[b] = outputTypes[b].newBlockBuilder(size);
                }
                p = 0;
            }

            Row row = list.get(i);
            for (int b = 0; b < builders.length; b++) {
                if (row.isNull(b)) {
                    builders[b].appendNull();
                    continue;
                }
                switch (outputTypes[b]) {
                    case BOOLEAN -> {
                        if (row.numberOfValues[b] > 1) {
                            ((BooleanBlock.Builder) builders[b]).beginPositionEntry();
                            for (int j = 0; j < row.numberOfValues[b]; j++) {
                                ((BooleanBlock.Builder) builders[b]).appendBoolean(row.getBoolean(b, j));
                            }
                            ((BooleanBlock.Builder) builders[b]).endPositionEntry();
                        } else {
                            ((BooleanBlock.Builder) builders[b]).appendBoolean(row.getBoolean(b, 0));
                        }
                    }
                    case INT -> {
                        if (row.numberOfValues[b] > 1) {
                            ((IntBlock.Builder) builders[b]).beginPositionEntry();
                            for (int j = 0; j < row.numberOfValues[b]; j++) {
                                ((IntBlock.Builder) builders[b]).appendInt(row.getInt(b, j));
                            }
                            ((IntBlock.Builder) builders[b]).endPositionEntry();
                        } else {
                            ((IntBlock.Builder) builders[b]).appendInt(row.getInt(b, 0));
                        }
                    }
                    case LONG -> {
                        if (row.numberOfValues[b] > 1) {
                            ((LongBlock.Builder) builders[b]).beginPositionEntry();
                            for (int j = 0; j < row.numberOfValues[b]; j++) {
                                ((LongBlock.Builder) builders[b]).appendLong(row.getLong(b, j));
                            }
                            ((LongBlock.Builder) builders[b]).endPositionEntry();
                        } else {
                            ((LongBlock.Builder) builders[b]).appendLong(row.getLong(b, 0));
                        }
                    }
                    case DOUBLE -> {
                        if (row.numberOfValues[b] > 1) {
                            ((DoubleBlock.Builder) builders[b]).beginPositionEntry();
                            for (int j = 0; j < row.numberOfValues[b]; j++) {
                                ((DoubleBlock.Builder) builders[b]).appendDouble(row.getDouble(b, j));
                            }
                            ((DoubleBlock.Builder) builders[b]).endPositionEntry();
                        } else {
                            ((DoubleBlock.Builder) builders[b]).appendDouble(row.getDouble(b, 0));
                        }
                    }
                    case BYTES_REF -> {
                        if (row.numberOfValues[b] > 1) {
                            ((BytesRefBlock.Builder) builders[b]).beginPositionEntry();
                            for (int j = 0; j < row.numberOfValues[b]; j++) {
                                ((BytesRefBlock.Builder) builders[b]).appendBytesRef(row.getBytesRef(b, j));
                            }
                            ((BytesRefBlock.Builder) builders[b]).endPositionEntry();
                        } else {
                            ((BytesRefBlock.Builder) builders[b]).appendBytesRef(row.getBytesRef(b, 0));
                        }
                    }
                    case DOC -> {
                        int dp = row.idToFirstValueIndex[b];
                        int shard = row.docs[dp++];
                        int segment = row.docs[dp++];
                        int doc = row.docs[dp];
                        ((DocBlock.Builder) builders[b]).appendShard(shard).appendSegment(segment).appendDoc(doc);
                    }
                    case NULL -> builders[b].appendNull();
                    default -> throw new IllegalStateException("unsupported type [" + rowFactory.idToType[b] + "]");
                }
            }

            p++;
            if (p == size) {
                result.add(new Page(Arrays.stream(builders).map(Block.Builder::build).toArray(Block[]::new)));
                builders = null;
            }
        }
        assert builders == null;
        return result.iterator();
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

    @Override
    public String toString() {
        return "TopNOperator[" + inputQueue + "]";
    }
}
