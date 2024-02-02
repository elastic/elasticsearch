/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;

import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig.DEFAULT_RESULTS_FIELD;

public class ChunkedTextExpansionResultsTests extends AbstractWireSerializingTestCase<ChunkedTextExpansionResults> {

    public static ChunkedTextExpansionResults createRandomResults() {
        var chunks = new ArrayList<ChunkedTextExpansionResults.ChunkedResult>();
        int numChunks = randomIntBetween(1, 5);

        for (int i = 0; i < numChunks; i++) {
            var tokenWeights = new ArrayList<TextExpansionResults.WeightedToken>();
            int numTokens = randomIntBetween(1, 8);
            for (int j = 0; j < numTokens; j++) {
                tokenWeights.add(new TextExpansionResults.WeightedToken(Integer.toString(j), (float) randomDoubleBetween(0.0, 5.0, false)));
            }
            chunks.add(new ChunkedTextExpansionResults.ChunkedResult(randomAlphaOfLength(6), tokenWeights));
        }

        return new ChunkedTextExpansionResults(DEFAULT_RESULTS_FIELD, chunks, randomBoolean());
    }

    @Override
    protected Writeable.Reader<ChunkedTextExpansionResults> instanceReader() {
        return ChunkedTextExpansionResults::new;
    }

    @Override
    protected ChunkedTextExpansionResults createTestInstance() {
        return createRandomResults();
    }

    @Override
    protected ChunkedTextExpansionResults mutateInstance(ChunkedTextExpansionResults instance) throws IOException {
        return null;
    }
}
