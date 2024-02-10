/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.results;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.inference.results.ChunkedTextEmbeddingResults;

import java.io.IOException;
import java.util.ArrayList;

public class ChunkedTextEmbeddingResultsTests extends AbstractWireSerializingTestCase<ChunkedTextEmbeddingResults> {

    public static ChunkedTextEmbeddingResults createRandomResults() {
        var chunks = new ArrayList<org.elasticsearch.xpack.core.ml.inference.results.ChunkedTextEmbeddingResults.EmbeddingChunk>();
        int columns = randomIntBetween(5, 10);
        int numChunks = randomIntBetween(1, 5);

        for (int i = 0; i < numChunks; i++) {
            double[] arr = new double[columns];
            for (int j = 0; j < columns; j++) {
                arr[j] = randomDouble();
            }
            chunks.add(
                new org.elasticsearch.xpack.core.ml.inference.results.ChunkedTextEmbeddingResults.EmbeddingChunk(
                    randomAlphaOfLength(6),
                    arr
                )
            );
        }

        return new ChunkedTextEmbeddingResults(chunks);
    }

    @Override
    protected Writeable.Reader<ChunkedTextEmbeddingResults> instanceReader() {
        return ChunkedTextEmbeddingResults::new;
    }

    @Override
    protected ChunkedTextEmbeddingResults createTestInstance() {
        return createRandomResults();
    }

    @Override
    protected ChunkedTextEmbeddingResults mutateInstance(ChunkedTextEmbeddingResults instance) throws IOException {
        return null;
    }
}
