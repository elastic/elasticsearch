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
import org.elasticsearch.compute.data.Page;

import java.util.Iterator;

@Experimental
public class TopNOperator implements Operator {

    protected final PriorityQueue<Page> inputQueue;
    private Iterator<Page> output;

    public record TopNOperatorFactory(int sortByChannel, boolean asc, int topCount) implements OperatorFactory {

        @Override
        public Operator get() {
            return new TopNOperator(sortByChannel, asc, topCount);
        }

        @Override
        public String describe() {
            return "TopNOperator(count = " + topCount + ", order = " + (asc ? "ascending" : "descending") + ")";
        }
    }

    public TopNOperator(int sortByChannel, boolean asc, int topCount) {
        this.inputQueue = new PriorityQueue<>(topCount) {
            @Override
            protected boolean lessThan(Page a, Page b) {
                if (asc) {
                    return a.getBlock(sortByChannel).getLong(0) > b.getBlock(sortByChannel).getLong(0);
                } else {
                    return a.getBlock(sortByChannel).getLong(0) < b.getBlock(sortByChannel).getLong(0);
                }
            }
        };
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
