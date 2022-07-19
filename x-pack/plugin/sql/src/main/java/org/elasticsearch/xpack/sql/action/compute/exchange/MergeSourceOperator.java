/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute.exchange;

import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.xpack.sql.action.compute.Block;
import org.elasticsearch.xpack.sql.action.compute.Operator;
import org.elasticsearch.xpack.sql.action.compute.Page;

import java.util.List;

// merges sorted pages (sort key (long value) is in sortInputChannel)
public class MergeSourceOperator implements Operator {

    private final List<ExchangeSource> sources;
    private final List<Integer> sortInputChannels;

    PriorityQueue<RankedPage> queue;
    private boolean finished;

    // Use priorityQueue to rank pages based on next value
    record RankedPage(int sourceIndex, int rowIndex, Page page) {

        Block block(List<Integer> sortInputChannels) {
            return page.getBlock(sortInputChannels.get(sourceIndex));
        }

        boolean hasValue(List<Integer> sortInputChannels) {
            return rowIndex < block(sortInputChannels).getPositionCount();
        }

        long value(List<Integer> sortInputChannels) {
            return block(sortInputChannels).getLong(rowIndex);
        }
    }

    public MergeSourceOperator(List<ExchangeSource> sources, List<Integer> sortInputChannels) {
        this.sources = sources;
        this.sortInputChannels = sortInputChannels;
        queue = new PriorityQueue<>(sources.size()) {
            @Override
            protected boolean lessThan(RankedPage a, RankedPage b) {
                return false;
            }
        };
    }


    @Override
    public Page getOutput() {
        if (queue.size() == 0) {
            if (sources.stream().allMatch(ExchangeSource::isFinished)) {
                return null;
            } else {
                for (int i = 0; i < sources.size(); i++) {
                    ExchangeSource exchangeSource = sources.get(i);
                    if (exchangeSource.isFinished() == false) {
                        Page page = exchangeSource.removePage();
                        if (page != null) {
                            queue.add(new RankedPage(i, 0, page));
                        }
                    }
                }
            }
        }
        // check if queue has one item from each non-finished source in order to compute next output
        for (int i = 0; i < sources.size(); i++) {
            ExchangeSource exchangeSource = sources.get(i);
            if (exchangeSource.isFinished() == false) {
                boolean found = false;
                // check queue has item
                for (RankedPage rankedPage : queue) {
                    if (rankedPage.rowIndex == i) {
                        found = true;
                        break;
                    }
                }
                if (found == false) {
                    Page page = exchangeSource.removePage();
                    if (page != null) {
                        queue.add(new RankedPage(i, 0, page));
                    }
                    return null;
                }
            }
        }
        // now compute output
        RankedPage rankedPage = queue.pop();
        return null;
    }

    @Override
    public boolean isFinished() {
        return finished;
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public boolean needsInput() {
        return false;
    }

    @Override
    public void addInput(Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {

    }
}
