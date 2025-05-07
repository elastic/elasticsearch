/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.rerank;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.esql.inference.InferenceOperator;

import java.util.Comparator;

public class RerankOperatorOutputBuilder extends InferenceOperator.OutputBuilder<RankedDocsResults> {

    private final Page inputPage;
    private final DoubleBlock.Builder scoreBlockBuilder;
    private final int scoreChannel;

    public RerankOperatorOutputBuilder(DoubleBlock.Builder scoreBlockBuilder, Page inputPage, int scoreChannel) {
        this.inputPage = inputPage;
        this.scoreBlockBuilder = scoreBlockBuilder;
        this.scoreChannel = scoreChannel;
    }

    @Override
    public void close() {
        releasePageOnAnyThread(inputPage);
        Releasables.close(scoreBlockBuilder);
    }

    @Override
    public Page buildOutput() {
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
            Releasables.close(blocks);
            throw (e);
        }
    }

    @Override
    public void addInferenceResult(RankedDocsResults results) {
        results.getRankedDocs()
            .stream()
            .sorted(Comparator.comparingInt(RankedDocsResults.RankedDoc::index))
            .mapToDouble(RankedDocsResults.RankedDoc::relevanceScore)
            .forEach(scoreBlockBuilder::appendDouble);
    }
}
