/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.fuse;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AbstractPageMappingOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.core.Releasables;

import java.util.HashMap;
import java.util.List;

/**
 * Updates the score column with new scores using the RRF formula.
 * Receives the position of the score and discriminator columns.
 * The new score we assign to each row is equal to {@code 1 / (rank_constant + row_number)}.
 * We use the discriminator column to determine the {@code row_number} for each row.
 */
public class RrfScoreEvalOperator extends AbstractPageMappingOperator {

    public record Factory(
        int discriminatorPosition,
        int scorePosition,
        RrfConfig rrfConfig,
        String sourceText,
        int sourceLine,
        int sourceColumn
    ) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new RrfScoreEvalOperator(
                driverContext,
                discriminatorPosition,
                scorePosition,
                rrfConfig,
                sourceText,
                sourceLine,
                sourceColumn
            );
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
    private Warnings warnings;
    private final DriverContext driverContext;
    private final String sourceText;
    private final int sourceLine;
    private final int sourceColumn;

    private HashMap<String, Integer> counters = new HashMap<>();

    public RrfScoreEvalOperator(
        DriverContext driverContext,
        int discriminatorPosition,
        int scorePosition,
        RrfConfig config,
        String sourceText,
        int sourceLine,
        int sourceColumn
    ) {
        this.scorePosition = scorePosition;
        this.discriminatorPosition = discriminatorPosition;
        this.config = config;
        this.driverContext = driverContext;
        this.sourceText = sourceText;
        this.sourceLine = sourceLine;
        this.sourceColumn = sourceColumn;
    }

    @Override
    protected Page process(Page page) {
        BytesRefBlock discriminatorBlock = page.getBlock(discriminatorPosition);
        DoubleBlock.Builder scores = discriminatorBlock.blockFactory().newDoubleBlockBuilder(discriminatorBlock.getPositionCount());

        for (int i = 0; i < page.getPositionCount(); i++) {
            Object value = BlockUtils.toJavaObject(discriminatorBlock, i);

            if (value == null) {
                warnings().registerException(new IllegalArgumentException("group column has null values; assigning null scores"));
                scores.appendNull();
            } else if (value instanceof List<?>) {
                warnings().registerException(
                    new IllegalArgumentException("group column contains multivalued entries; assigning null scores")
                );
                scores.appendNull();
            } else {
                String discriminator = ((BytesRef) value).utf8ToString();
                int rank = counters.getOrDefault(discriminator, 1);
                var weight = config.weights().getOrDefault(discriminator, 1.0);
                scores.appendDouble(1.0 / (config.rankConstant() + rank) * weight);
                counters.put(discriminator, rank + 1);
            }
        }

        Page newPage = null;
        Block scoreBlock = null;

        try {
            scoreBlock = scores.build();
            newPage = page.appendBlock(scoreBlock);

            int[] projections = new int[newPage.getBlockCount() - 1];

            for (int i = 0; i < newPage.getBlockCount() - 1; i++) {
                projections[i] = i == scorePosition ? newPage.getBlockCount() - 1 : i;
            }
            return newPage.projectBlocks(projections);
        } finally {
            if (newPage != null) {
                newPage.releaseBlocks();
            } else {
                // we never got to a point where the new page was constructed, so we need to release the initial one
                page.releaseBlocks();
            }
            if (scoreBlock == null) {
                // we never built scoreBlock, so we need to release the scores builder
                Releasables.close(scores);
            }
        }
    }

    @Override
    public String toString() {
        return "RrfScoreEvalOperator";
    }

    private Warnings warnings() {
        if (warnings == null) {
            this.warnings = Warnings.createWarnings(driverContext.warningsMode(), sourceLine, sourceColumn, sourceText);
        }

        return warnings;
    }
}
