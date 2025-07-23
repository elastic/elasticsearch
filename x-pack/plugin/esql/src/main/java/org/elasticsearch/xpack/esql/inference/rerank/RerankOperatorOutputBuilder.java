/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.rerank;

import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.esql.inference.InferenceOperator;

import java.util.Comparator;
import java.util.Iterator;

/**
 * Builds the output block for the {@link RerankOperator} by converting reranked relevance scores into a DoubleBlock.
 */

public class RerankOperatorOutputBuilder implements InferenceOperator.OutputBuilder {

    private final DoubleBlock.Builder scoreBlockBuilder;

    public RerankOperatorOutputBuilder(DoubleBlock.Builder scoreBlockBuilder) {
        this.scoreBlockBuilder = scoreBlockBuilder;
    }

    @Override
    public void close() {
        Releasables.close(scoreBlockBuilder);
    }

    /**
     * Builds the rerank scores output block.
     */
    @Override
    public DoubleBlock buildOutput() {
        return scoreBlockBuilder.build();
    }

    /**
     * Extracts the ranked document results from the inference response and appends their relevance scores to the score block builder.
     * <p>
     * If the response is not of type {@link RankedDocsResults} an {@link IllegalStateException} is thrown.
     * </p>
     * <p>
     * The responses must be added in the same order as the corresponding inference requests were generated.
     * Failing to preserve order may lead to incorrect or misaligned output rows.
     * </p>
     */
    @Override
    public void addInferenceResponse(InferenceAction.Response inferenceResponse) {
        Iterator<RankedDocsResults.RankedDoc> sortedRankedDocIterator = inferenceResults(inferenceResponse).getRankedDocs()
            .stream()
            .sorted(Comparator.comparingInt(RankedDocsResults.RankedDoc::index))
            .iterator();
        while (sortedRankedDocIterator.hasNext()) {
            scoreBlockBuilder.appendDouble(sortedRankedDocIterator.next().relevanceScore());
        }
    }

    private RankedDocsResults inferenceResults(InferenceAction.Response inferenceResponse) {
        return InferenceOperator.OutputBuilder.inferenceResults(inferenceResponse, RankedDocsResults.class);
    }
}
