/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;

import java.util.HashMap;

public class RrfScoreEvalOperator implements Operator {

    public record Factory(int forkPosition, int scorePosition) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new RrfScoreEvalOperator(forkPosition, scorePosition);
        }

        @Override
        public String describe() {
            return "RrfScoreEvalOperator";
        }

    }

    private final int scorePosition;
    private final int forkPosition;

    private boolean finished = false;
    private Page prev = null;

    private HashMap<String, Integer> counters = new HashMap<>();

    public RrfScoreEvalOperator(int forkPosition, int scorePosition) {
        this.scorePosition = scorePosition;
        this.forkPosition = forkPosition;
    }

    @Override
    public boolean needsInput() {
        return prev == null && finished == false;
    }

    @Override
    public void addInput(Page page) {
        assert prev == null : "has pending input page";
        prev = page;
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public boolean isFinished() {
        return finished && prev == null;
    }

    @Override
    public Page getOutput() {
        Page page = prev;

        BytesRefBlock forkBlock = (BytesRefBlock) page.getBlock(forkPosition);

        DoubleVector.Builder scores = forkBlock.blockFactory().newDoubleVectorBuilder(forkBlock.getPositionCount());

        for (int i = 0; i < page.getPositionCount(); i++) {
            String fork = forkBlock.getBytesRef(i, new BytesRef()).utf8ToString();

            int rank = counters.getOrDefault(fork, 1);
            counters.put(fork, rank + 1);
            scores.appendDouble(1.0 / (60 + rank));
        }

        Block scoreBlock = scores.build().asBlock();
        page = page.appendBlock(scoreBlock);

        int[] projections = new int[page.getBlockCount() - 1];

        for (int i = 0; i < page.getBlockCount() - 1; i++) {
            projections[i] = i == scorePosition ? page.getBlockCount() - 1 : i;
        }

        page = page.projectBlocks(projections);

        prev = null;
        return page;
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(() -> {
            if (prev != null) {
                prev.releaseBlocks();
            }
        });
    }
}
