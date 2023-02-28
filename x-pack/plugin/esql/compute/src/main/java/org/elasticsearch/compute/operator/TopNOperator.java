/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
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

@Experimental
public class TopNOperator implements Operator {

    static final class Row {
        boolean[] booleans;
        int[] ints;
        long[] longs;
        double[] doubles;
        BytesRef[] byteRefs;

        boolean[] nullValues;

        int[] idToPosition;
        ElementType[] idToType;

        boolean isNull(int i) {
            return nullValues[i];
        }

        boolean getBoolean(int i) {
            return booleans[idToPosition[i]];
        }

        int getInt(int i) {
            return ints[idToPosition[i]];
        }

        long getLong(int i) {
            return longs[idToPosition[i]];
        }

        double getDouble(int i) {
            return doubles[idToPosition[i]];
        }

        BytesRef getBytesRef(int i) {
            return byteRefs[idToPosition[i]];
        }
    }

    static final class RowFactory {

        int size;
        int nBooleans;
        int nInts;
        int nLongs;
        int nDoubles;
        int nByteRefs;

        int[] idToPosition;
        ElementType[] idToType;

        RowFactory(Page page) {
            size = page.getBlockCount();
            idToPosition = new int[size];
            idToType = new ElementType[size];
            for (int i = 0; i < size; i++) {
                Block block = page.getBlock(i);
                int idx = switch (block.elementType()) {
                    case LONG -> nLongs++;
                    case INT -> nInts++;
                    case DOUBLE -> nDoubles++;
                    case BYTES_REF -> nByteRefs++;
                    case BOOLEAN -> nBooleans++;
                    case NULL -> -1;
                    case UNKNOWN -> {
                        assert false : "Must not occur here as TopN should never receive intermediate blocks";
                        throw new UnsupportedOperationException("Block doesn't support retrieving elements");
                    }
                };
                idToPosition[i] = idx;
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
                result.idToPosition = idToPosition;
                result.idToType = idToType;
            } else {
                result = spare;
                Arrays.fill(result.nullValues, false);
            }

            for (int i = 0; i < origin.getBlockCount(); i++) {
                Block block = origin.getBlock(i);
                if (block.isNull(rowNum)) {
                    result.nullValues[i] = true;
                } else {
                    switch (block.elementType()) {
                        case LONG -> result.longs[idToPosition[i]] = ((LongBlock) block).getLong(rowNum);
                        case INT -> result.ints[idToPosition[i]] = ((IntBlock) block).getInt(rowNum);
                        case DOUBLE -> result.doubles[idToPosition[i]] = ((DoubleBlock) block).getDouble(rowNum);
                        case BYTES_REF -> {
                            BytesRef b = result.byteRefs[idToPosition[i]];
                            b = ((BytesRefBlock) block).getBytesRef(rowNum, b);
                            result.byteRefs[idToPosition[i]] = b;
                        }
                        case BOOLEAN -> result.booleans[idToPosition[i]] = ((BooleanBlock) block).getBoolean(rowNum);
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

    private final PriorityQueue<Row> inputQueue;

    private Iterator<Page> output;

    public record SortOrder(int channel, boolean asc, boolean nullsFirst) {}

    public record TopNOperatorFactory(int topCount, List<SortOrder> sortOrders) implements OperatorFactory {

        @Override
        public Operator get() {
            return new TopNOperator(topCount, sortOrders);
        }

        @Override
        public String describe() {
            return "TopNOperator(count = " + topCount + ", sortOrders = " + sortOrders + ")";
        }
    }

    public TopNOperator(int topCount, List<SortOrder> sortOrders) {
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
            case INT -> Integer.compare(b1.getInt(position), b2.getInt(position));
            case LONG -> Long.compare(b1.getLong(position), b2.getLong(position));
            case DOUBLE -> Double.compare(b1.getDouble(position), b2.getDouble(position));
            case BOOLEAN -> Boolean.compare(b1.getBoolean(position), b2.getBoolean(position));
            case BYTES_REF -> b1.getBytesRef(position).compareTo(b2.getBytesRef(position));
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
        RowFactory factory = new RowFactory(page);
        Row removed = null;
        for (int i = 0; i < page.getPositionCount(); i++) {
            Row x = factory.row(page, i, removed);
            removed = inputQueue.insertWithOverflow(x);
        }
    }

    @Override
    public void finish() {
        if (output == null) {
            output = toPages(inputQueue);
        }
    }

    protected Iterator<Page> toPages(PriorityQueue<Row> rows) {
        if (rows.size() == 0) {
            return Collections.emptyIterator();
        }
        List<Row> list = new ArrayList<>(rows.size());
        while (inputQueue.size() > 0) {
            list.add(inputQueue.pop());
        }
        Collections.reverse(list);

        // This returns one page per row because ValuesSourceReaderOperator.addInput() does not
        // allow non-non-decreasing "docs" IntVector
        // TODO review this when ValuesSourceReaderOperator can handle this case
        final Iterator<Row> listIterator = list.iterator();
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return listIterator.hasNext();
            }

            @Override
            public Page next() {
                return toPage(listIterator.next());
            }
        };
    }

    private static Page toPage(Row row) {
        Block[] blocks = new Block[row.idToType.length];
        for (int i = 0; i < row.idToType.length; i++) {
            ElementType type = row.idToType[i];
            blocks[i] = switch (type) {
                case BOOLEAN -> row.isNull(i)
                    ? BooleanBlock.newBlockBuilder(1).appendNull().build()
                    : BooleanBlock.newBlockBuilder(1).appendBoolean(row.getBoolean(i)).build();
                case INT -> row.isNull(i)
                    ? IntBlock.newBlockBuilder(1).appendNull().build()
                    : IntBlock.newBlockBuilder(1).appendInt(row.getInt(i)).build();
                case LONG -> row.isNull(i)
                    ? LongBlock.newBlockBuilder(1).appendNull().build()
                    : LongBlock.newBlockBuilder(1).appendLong(row.getLong(i)).build();
                case DOUBLE -> row.isNull(i)
                    ? DoubleBlock.newBlockBuilder(1).appendNull().build()
                    : DoubleBlock.newBlockBuilder(1).appendDouble(row.getDouble(i)).build();
                case BYTES_REF -> row.isNull(i)
                    ? BytesRefBlock.newBlockBuilder(1).appendNull().build()
                    : BytesRefBlock.newBlockBuilder(1).appendBytesRef(row.getBytesRef(i)).build();
                case NULL -> Block.constantNullBlock(1);
                case UNKNOWN -> {
                    assert false : "Must not occur here as TopN should never receive intermediate blocks";
                    throw new UnsupportedOperationException("Block doesn't support retrieving elements");
                }
            };
        }
        return new Page(blocks);
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
        return "TopNOperator(" + inputQueue + ")";
    }
}
