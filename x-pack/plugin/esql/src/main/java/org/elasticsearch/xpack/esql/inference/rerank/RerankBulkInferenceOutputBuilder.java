/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.rerank;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.esql.inference.bulk.BulkInferenceOutputBuilder;

import java.util.stream.Stream;

public class RerankBulkInferenceOutputBuilder extends BulkInferenceOutputBuilder<RankedDocsResults, Page> {
    private final Page inputPage;
    private final DoubleBlock.Builder scoreBlockBuilder;
    private final int scoreChannel;

    public RerankBulkInferenceOutputBuilder(BlockFactory blockFactory, Page inputPage, int scoreChannel) {
        this.scoreBlockBuilder = blockFactory.newDoubleBlockBuilder(inputPage.getPositionCount());
        this.inputPage = inputPage;
        this.scoreChannel = scoreChannel;
    }

    @Override
    protected Class<RankedDocsResults> inferenceResultsClass() {
        return RankedDocsResults.class;
    }

    @Override
    protected Page buildOutput() {
        int blockCount = Integer.max(inputPage.getBlockCount(), scoreChannel + 1);
        Block[] blocks = new Block[blockCount];

        try {
            for (int b = 0; b < blockCount; b++) {
                if (b == scoreChannel) {
                    blocks[b] = scoreBlockBuilder.build();
                } else {
                    blocks[b] = inputPage.getBlock(b);
                    blocks[b].incRef();
                }
            }
            return new Page(blocks);
        } catch (Exception e) {
            Stream.of(blocks).forEach(Block::allowPassingToDifferentDriver);
            Releasables.closeExpectNoException(blocks);
            throw (e);
        }
    }

    @Override
    public void close() {
        inputPage.allowPassingToDifferentDriver();
        inputPage.releaseBlocks();

        Releasables.closeExpectNoException(scoreBlockBuilder);
    }

    @Override
    public void onInferenceResults(RankedDocsResults results) {
        Double[] sortedRankedDocsScores = sortedRankedDocsScores(results);

        for (int pos = 0; pos < inputPage.getPositionCount(); pos++) {
            if (sortedRankedDocsScores[pos] != null) {
                scoreBlockBuilder.appendDouble(sortedRankedDocsScores[pos]);
            } else {
                scoreBlockBuilder.appendNull();
            }
        }
    }

    private Double[] sortedRankedDocsScores(RankedDocsResults results) {
        Double[] sortedRankedDocsScores = new Double[inputPage.getPositionCount()];

        for (RankedDocsResults.RankedDoc rankedDoc : results.getRankedDocs()) {
            sortedRankedDocsScores[rankedDoc.index()] = (double) rankedDoc.relevanceScore();
        }

        return sortedRankedDocsScores;
    }
}
