/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.search.WeightedToken;

import java.io.IOException;
import java.util.ArrayList;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig.DEFAULT_RESULTS_FIELD;

public class InferenceChunkedTextExpansionResultsTests extends AbstractWireSerializingTestCase<InferenceChunkedTextExpansionResults> {

    public static InferenceChunkedTextExpansionResults createRandomResults() {
        var chunks = new ArrayList<InferenceChunkedTextExpansionResults.ChunkedResult>();
        int numChunks = randomIntBetween(1, 5);

        for (int i = 0; i < numChunks; i++) {
            var tokenWeights = new ArrayList<WeightedToken>();
            int numTokens = randomIntBetween(1, 8);
            for (int j = 0; j < numTokens; j++) {
                tokenWeights.add(new WeightedToken(Integer.toString(j), (float) randomDoubleBetween(0.0, 5.0, false)));
            }
            chunks.add(new InferenceChunkedTextExpansionResults.ChunkedResult(randomAlphaOfLength(6), tokenWeights));
        }

        return new InferenceChunkedTextExpansionResults(DEFAULT_RESULTS_FIELD, chunks, randomBoolean());
    }

    @Override
    protected Writeable.Reader<InferenceChunkedTextExpansionResults> instanceReader() {
        return InferenceChunkedTextExpansionResults::new;
    }

    @Override
    protected InferenceChunkedTextExpansionResults createTestInstance() {
        return createRandomResults();
    }

    @Override
    protected InferenceChunkedTextExpansionResults mutateInstance(InferenceChunkedTextExpansionResults instance) throws IOException {
        return null;
    }
}
