/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.fuse;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AbstractPageMappingOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;

import java.util.HashMap;

/**
 * Updates the score column with new scores using the RRF formula.
 * Receives the position of the score and discriminator columns.
 * The new score we assign to each row is equal to {@code 1 / (rank_constant + row_number)}.
 * We use the discriminator column to determine the {@code row_number} for each row.
 */
public class RrfScoreEvalOperator extends AbstractPageMappingOperator {

    public record Factory(int discriminatorPosition, int scorePosition, RrfConfig rrfConfig) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new RrfScoreEvalOperator(discriminatorPosition, scorePosition, rrfConfig);
        }

        @Override
        public String describe() {
            return "RrfScoreEvalOperator[discriminatorPosition="
                + discriminatorPosition
                + ", scorePosition="
                + scorePosition
                + ", rrfConfig="
                + rrfConfig
                + "]";
        }
    }

    private final int scorePosition;
    private final int discriminatorPosition;
    private final RrfConfig config;

    private HashMap<String, Integer> counters = new HashMap<>();

    public RrfScoreEvalOperator(int discriminatorPosition, int scorePosition, RrfConfig config) {
        this.scorePosition = scorePosition;
        this.discriminatorPosition = discriminatorPosition;
        this.config = config;
    }

    @Override
    protected Page process(Page page) {
        BytesRefBlock discriminatorBlock = (BytesRefBlock) page.getBlock(discriminatorPosition);

        DoubleVector.Builder scores = discriminatorBlock.blockFactory().newDoubleVectorBuilder(discriminatorBlock.getPositionCount());

        for (int i = 0; i < page.getPositionCount(); i++) {
            String discriminator = discriminatorBlock.getBytesRef(i, new BytesRef()).utf8ToString();

            int rank = counters.getOrDefault(discriminator, 1);
            counters.put(discriminator, rank + 1);

            var weight = config.weights().getOrDefault(discriminator, 1.0);

            scores.appendDouble(1.0 / (config.rankConstant() + rank) * weight);
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
