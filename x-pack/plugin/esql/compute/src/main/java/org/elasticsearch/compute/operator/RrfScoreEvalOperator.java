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

import java.util.HashMap;

/**
 * Updates the score column with new scores using the RRF formula.
 * Receives the position of the score and fork columns.
 * The new score we assign to each row is equal to {@code 1 / (rank_constant + row_number)}.
 * We use the fork discriminator column to determine the {@code row_number} for each row.
 */
public class RrfScoreEvalOperator extends AbstractPageMappingOperator {

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

    private HashMap<String, Integer> counters = new HashMap<>();

    public RrfScoreEvalOperator(int forkPosition, int scorePosition) {
        this.scorePosition = scorePosition;
        this.forkPosition = forkPosition;
    }

    @Override
    protected Page process(Page page) {
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
        try {
            return page.projectBlocks(projections);
        } finally {
            page.releaseBlocks();
        }
    }

    @Override
    public String toString() {
        return "RrfScoreEvalOperator";
    }
}
