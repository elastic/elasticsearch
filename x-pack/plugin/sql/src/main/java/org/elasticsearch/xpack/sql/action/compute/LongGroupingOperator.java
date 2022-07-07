/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongHash;

public class LongGroupingOperator implements Operator {

    private final int channel;

    LongHash longHash;
    Page lastPage;
    boolean finished;

    public LongGroupingOperator(int channel, BigArrays bigArrays) {
        this.channel = channel;
        this.longHash = new LongHash(1, bigArrays);
    }

    @Override
    public Page getOutput() {
        Page l = lastPage;
        lastPage = null;
        if (finished) {
            longHash.close();
        }
        return l;
    }

    @Override
    public boolean isFinished() {
        return finished && lastPage == null;
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public boolean needsInput() {
        return finished == false && lastPage == null;
    }

    @Override
    public void addInput(Page page) {
        LongBlock block = (LongBlock) page.getBlock(channel);
        long[] groups = new long[block.getPositionCount()];
        for (int i = 0; i < block.getPositionCount(); i++) {
            long value = block.getLong(i);
            long bucketOrd = longHash.add(value);
            if (bucketOrd < 0) { // already seen
                bucketOrd = -1 - bucketOrd;
            }
            groups[i] = bucketOrd;
        }
        lastPage = page.appendColumn(new LongBlock(groups, block.getPositionCount()));
    }
}
