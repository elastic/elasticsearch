/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.compute.Experimental;
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
                    return TopNOperator.compareTo(order, a, b) < 0;
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
            int compared = compareTo(order, a, b);
            if (compared != 0) {
                return compared;
            }
        }
        return 0;
    }

    private static int compareTo(SortOrder order, Page a, Page b) {
        Block blockA = a.getBlock(order.channel);
        Block blockB = b.getBlock(order.channel);

        boolean aIsNull = blockA.isNull(0);
        boolean bIsNull = blockB.isNull(0);
        if (aIsNull || bIsNull) {
            return Boolean.compare(aIsNull, bIsNull) * (order.nullsFirst ? 1 : -1);
        }

        return Long.compare(blockA.getLong(0), blockB.getLong(0)) * (order.asc ? -1 : 1);
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
