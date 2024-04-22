/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.results;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.inference.results.ChunkedTextEmbeddingFloatResults;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ChunkedTextEmbeddingFloatResultsTests extends AbstractWireSerializingTestCase<ChunkedTextEmbeddingFloatResults> {

    public static ChunkedTextEmbeddingFloatResults createRandomResults() {
        int numChunks = randomIntBetween(1, 5);
        var chunks = new ArrayList<ChunkedTextEmbeddingFloatResults.EmbeddingChunk>(numChunks);

        for (int i = 0; i < numChunks; i++) {
            chunks.add(createRandomChunk());
        }

        return new ChunkedTextEmbeddingFloatResults(chunks);
    }

    private static ChunkedTextEmbeddingFloatResults.EmbeddingChunk createRandomChunk() {
        int columns = randomIntBetween(1, 10);
        List<Float> floats = new ArrayList<>(columns);

        for (int i = 0; i < columns; i++) {
            floats.add(randomFloat());
        }

        return new ChunkedTextEmbeddingFloatResults.EmbeddingChunk(randomAlphaOfLength(6), floats);
    }

    @Override
    protected Writeable.Reader<ChunkedTextEmbeddingFloatResults> instanceReader() {
        return ChunkedTextEmbeddingFloatResults::new;
    }

    @Override
    protected ChunkedTextEmbeddingFloatResults createTestInstance() {
        return createRandomResults();
    }

    @Override
    protected ChunkedTextEmbeddingFloatResults mutateInstance(ChunkedTextEmbeddingFloatResults instance) throws IOException {
        return null;
    }
}
