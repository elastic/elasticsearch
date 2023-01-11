/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;

import java.util.Iterator;
import java.util.List;

@Experimental
public class TopNOperator implements Operator {

    protected final PriorityQueue<Page> inputQueue;
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
                protected boolean lessThan(Page a, Page b) {
                    return compareFirstPositionsOfBlocks(
                        order.asc,
                        order.nullsFirst,
                        a.getBlock(order.channel),
                        b.getBlock(order.channel)
                    ) < 0;
                }
            };
        } else {
            this.inputQueue = new PriorityQueue<>(topCount) {
                @Override
                protected boolean lessThan(Page a, Page b) {
                    return TopNOperator.compareTo(sortOrders, a, b) < 0;
                }
            };
        }
    }

    private static int compareTo(List<SortOrder> orders, Page a, Page b) {
        for (SortOrder order : orders) {
            int cmp = compareFirstPositionsOfBlocks(order.asc, order.nullsFirst, a.getBlock(order.channel), b.getBlock(order.channel));
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    /**
     * Since all pages in the PQ are single-row (see {@link #addInput(Page)}, here we only need to compare the first positions of the given
     * blocks.
     */
    static int compareFirstPositionsOfBlocks(boolean asc, boolean nullsFirst, Block b1, Block b2) {
        assert b1.getPositionCount() == 1 : "not a single row block";
        assert b2.getPositionCount() == 1 : "not a single row block";
        boolean firstIsNull = b1.isNull(0);
        boolean secondIsNull = b2.isNull(0);
        if (firstIsNull || secondIsNull) {
            return Boolean.compare(firstIsNull, secondIsNull) * (nullsFirst ? 1 : -1);
        }
        if (b1.elementType() != b2.elementType()) {
            throw new IllegalStateException("Blocks have incompatible element types: " + b1.elementType() + " != " + b2.elementType());
        }
        final int cmp = switch (b1.elementType()) {
            case INT -> Integer.compare(b1.getInt(0), b2.getInt(0));
            case LONG -> Long.compare(b1.getLong(0), b2.getLong(0));
            case DOUBLE -> Double.compare(b1.getDouble(0), b2.getDouble(0));
            case BYTES_REF -> b1.getBytesRef(0, new BytesRef()).compareTo(b2.getBytesRef(0, new BytesRef()));
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
        for (int i = 0; i < page.getPositionCount(); i++) {
            inputQueue.insertWithOverflow(page.getRow(i));
        }
    }

    @Override
    public void finish() {
        if (output == null) {
            // We need to output elements from the input queue in reverse order because
            // the `lessThan` relation of the input queue is reversed to retain only N smallest elements.
            final Page[] pages = new Page[inputQueue.size()];
            for (int i = pages.length - 1; i >= 0; i--) {
                pages[i] = inputQueue.pop();
            }
            output = Iterators.forArray(pages);
        }
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
}
