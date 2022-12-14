/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.compute.Experimental;
import org.elasticsearch.compute.data.LongArrayBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;

/**
 * Group operator that adds grouping information to pages
 * based on a long field.
 */
@Experimental
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
            /*
             * eagerly return our memory to the pool so it can be reused
             * and clear our reference to it so when we are "closed" we
             * don't try to free it again
             */
            longHash.close();
            longHash = null;
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
        LongArrayBlock block = (LongArrayBlock) page.getBlock(channel);
        long[] groups = new long[block.getPositionCount()];
        for (int i = 0; i < block.getPositionCount(); i++) {
            long value = block.getLong(i);
            long bucketOrd = longHash.add(value);
            if (bucketOrd < 0) { // already seen
                bucketOrd = -1 - bucketOrd;
            }
            groups[i] = bucketOrd;
        }
        lastPage = page.appendBlock(new LongArrayBlock(groups, block.getPositionCount()));
    }

    @Override
    public void close() {
        Releasables.close(longHash);
    }
}
