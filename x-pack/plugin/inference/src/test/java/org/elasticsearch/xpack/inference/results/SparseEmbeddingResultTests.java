/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.results;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;

import java.util.ArrayList;
import java.util.List;

public class SparseEmbeddingResultTests extends AbstractWireSerializingTestCase<SparseEmbeddingResult> {

    public static SparseEmbeddingResult createRandomResult() {
        int numTokens = randomIntBetween(1, 20);
        List<TextExpansionResults.WeightedToken> tokenList = new ArrayList<>();
        for (int i = 0; i < numTokens; i++) {
            tokenList.add(new TextExpansionResults.WeightedToken(Integer.toString(i), (float) randomDoubleBetween(0.0, 5.0, false)));
        }
        return new SparseEmbeddingResult(tokenList);
    }

    @Override
    protected Writeable.Reader<SparseEmbeddingResult> instanceReader() {
        return SparseEmbeddingResult::new;
    }

    @Override
    protected SparseEmbeddingResult createTestInstance() {
        return createRandomResult();
    }

    @Override
    protected SparseEmbeddingResult mutateInstance(SparseEmbeddingResult instance) {
        if (instance.getWeightedTokens().size() > 0) {
            var tokens = instance.getWeightedTokens();
            return new SparseEmbeddingResult(tokens.subList(0, tokens.size() - 1));
        } else {
            return new SparseEmbeddingResult(List.of(new TextExpansionResults.WeightedToken("a", 1.0f)));
        }
    }
}
