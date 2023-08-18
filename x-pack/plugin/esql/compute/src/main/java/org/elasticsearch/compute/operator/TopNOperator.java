/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class TopNOperator implements Operator {

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

        Row row(Page origin, int rowNum, Row spare) {
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
            } else {
                result = spare;
                // idToType has to be set because different pages could have different block types due to different mappings
                result.idToType = idToType;
                Arrays.fill(result.nullValues, false);
            }

            int lastLongFirstValueIndex = 0;
            int lastIntFirstValueIndex = 0;
            int lastDoubleFirstValueIndex = 0;
            int lastBytesRefFirstValueIndex = 0;
            int lastBooleanFirstValueIndex = 0;
            int lastDocFirstValueIndex = 0;

            for (int i = 0; i < size; i++) {
                Block block = origin.getBlock(i);
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
            return result;
        }
    }

    public record SortOrder(int channel, boolean asc, boolean nullsFirst) {}

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

    // these will be inferred at runtime: one input page might not contain all the information needed
    // eg. it could be missing some fields in the mapping, so it could have NULL blocks as placeholders
    private ElementType[] outputTypes;

    private Iterator<Page> output;

    public TopNOperator(int topCount, List<SortOrder> sortOrders, int maxPageSize) {
        this.maxPageSize = maxPageSize;
        if (sortOrders.size() == 1) {
            // avoid looping over sortOrders if there is only one order
            SortOrder order = sortOrders.get(0);
            this.inputQueue = new PriorityQueue<>(topCount) {
                @Override
                protected boolean lessThan(Row a, Row b) {
                    return comparePositions(order.asc, order.nullsFirst, a, b, order.channel) < 0;
                }

                @Override
                public String toString() {
                    return "count = " + size() + "/" + topCount + ", sortOrder = " + order;
                }
            };
        } else {
            this.inputQueue = new PriorityQueue<>(topCount) {
                @Override
                protected boolean lessThan(Row a, Row b) {
                    return TopNOperator.compareTo(sortOrders, a, b) < 0;
                }

                @Override
                public String toString() {
                    return "count = " + size() + "/" + topCount + ", sortOrders = " + sortOrders;
                }
            };
        }
    }

    private static int compareTo(List<SortOrder> orders, Row a, Row b) {
        for (SortOrder order : orders) {
            int cmp = comparePositions(order.asc, order.nullsFirst, a, b, order.channel);
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    static int comparePositions(boolean asc, boolean nullsFirst, Row b1, Row b2, int position) {
        boolean firstIsNull = b1.isNull(position);
        boolean secondIsNull = b2.isNull(position);
        if (firstIsNull || secondIsNull) {
            return Boolean.compare(firstIsNull, secondIsNull) * (nullsFirst ? 1 : -1);
        }
        if (b1.idToType[position] != b2.idToType[position]) {
            throw new IllegalStateException(
                "Blocks have incompatible element types: " + b1.idToType[position] + " != " + b2.idToType[position]
            );
        }
        int cmp = switch (b1.idToType[position]) {
            case INT -> Integer.compare(b1.getInt(position, 0), b2.getInt(position, 0));
            case LONG -> Long.compare(b1.getLong(position, 0), b2.getLong(position, 0));
            case DOUBLE -> Double.compare(b1.getDouble(position, 0), b2.getDouble(position, 0));
            case BOOLEAN -> Boolean.compare(b1.getBoolean(position, 0), b2.getBoolean(position, 0));
            case BYTES_REF -> b1.getBytesRef(position, 0).compareTo(b2.getBytesRef(position, 0));
            case DOC -> throw new UnsupportedOperationException("Block of nulls doesn't support comparison");
            case NULL -> {
                assert false : "Must not occur here as we check nulls above already";
                throw new UnsupportedOperationException("Block of nulls doesn't support comparison");
            }
            case UNKNOWN -> {
                assert false : "Must not occur here as TopN should never receive intermediate blocks";
                throw new UnsupportedOperationException("Block doesn't support retrieving elements");
            }
        };
        return asc ? -cmp : cmp;
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
            Row x = rowFactory.row(page, i, removed);
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
