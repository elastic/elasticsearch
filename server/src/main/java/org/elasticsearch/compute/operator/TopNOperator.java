/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.compute.Experimental;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;

@Experimental
public class TopNOperator implements Operator {

    // monotonically increasing state
    private static final int NEEDS_INPUT = 0;
    private static final int HAS_OUTPUT = 1;
    private static final int FINISHED = 2;

    private int state = NEEDS_INPUT;

    protected final PriorityQueue<Page> pq;

    public record TopNOperatorFactory(int sortByChannel, boolean asc, int topCount, boolean nullsFirst) implements OperatorFactory {

        @Override
        public Operator get() {
            return new TopNOperator(sortByChannel, asc, topCount, nullsFirst);
        }

        @Override
        public String describe() {
            return "TopNOperator(count = " + topCount + ", order = " + (asc ? "ascending" : "descending") + ")";
        }
    }

    public TopNOperator(int sortByChannel, boolean asc, int topCount, boolean nullsFirst) {
        this.pq = new PriorityQueue<>(topCount) {
            @Override
            protected boolean lessThan(Page a, Page b) {
                Block blockA = a.getBlock(sortByChannel);
                Block blockB = b.getBlock(sortByChannel);
                if (blockA.isNull(0)) {
                    return nullsFirst;
                } else if (blockB.isNull(0)) {
                    return nullsFirst == false;
                }
                if (asc) {
                    return blockA.getLong(0) > blockB.getLong(0);
                } else {
                    return blockA.getLong(0) < blockB.getLong(0);
                }
            }
        };
    }

    @Override
    public boolean needsInput() {
        return state == NEEDS_INPUT;
    }

    @Override
    public void addInput(Page page) {
        for (int i = 0; i < page.getPositionCount(); i++) {
            pq.insertWithOverflow(page.getRow(i));
        }
    }

    @Override
    public void finish() {
        if (state == NEEDS_INPUT) {
            state = HAS_OUTPUT;
        }
    }

    @Override
    public boolean isFinished() {
        return state == FINISHED;
    }

    @Override
    public Page getOutput() {
        if (state != HAS_OUTPUT) {
            return null;
        }
        Page page = pq.pop();
        if (pq.size() == 0) {
            state = FINISHED;
        }
        return page;
    }

    @Override
    public void close() {

    }
}
