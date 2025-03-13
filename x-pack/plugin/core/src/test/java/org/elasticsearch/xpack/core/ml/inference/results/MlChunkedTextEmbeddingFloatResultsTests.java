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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ml.inference.results.ChunkedNlpInferenceResults.INFERENCE;
import static org.elasticsearch.xpack.core.ml.inference.results.ChunkedNlpInferenceResults.TEXT;
import static org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig.DEFAULT_RESULTS_FIELD;

public class MlChunkedTextEmbeddingFloatResultsTests extends AbstractWireSerializingTestCase<MlChunkedTextEmbeddingFloatResults> {

    public static MlChunkedTextEmbeddingFloatResults createRandomResults() {
        var chunks = new ArrayList<MlChunkedTextEmbeddingFloatResults.EmbeddingChunk>();
        int columns = randomIntBetween(5, 10);
        int numChunks = randomIntBetween(1, 5);

        for (int i = 0; i < numChunks; i++) {
            double[] arr = new double[columns];
            for (int j = 0; j < columns; j++) {
                arr[j] = randomDouble();
            }
            chunks.add(new MlChunkedTextEmbeddingFloatResults.EmbeddingChunk(randomAlphaOfLength(6), arr));
        }

        return new MlChunkedTextEmbeddingFloatResults(DEFAULT_RESULTS_FIELD, chunks, randomBoolean());
    }

    /**
     * Similar to {@link MlChunkedTextEmbeddingFloatResults.EmbeddingChunk#asMap()} but it converts the double array into a list of doubles
     * to make testing equality easier.
     */
    public static Map<String, Object> asMapWithListsInsteadOfArrays(MlChunkedTextEmbeddingFloatResults.EmbeddingChunk chunk) {
        var map = new HashMap<String, Object>();
        map.put(TEXT, chunk.matchedText());
        map.put(INFERENCE, Arrays.stream(chunk.embedding()).boxed().collect(Collectors.toList()));
        return map;
    }

    @Override
    protected Writeable.Reader<MlChunkedTextEmbeddingFloatResults> instanceReader() {
        return MlChunkedTextEmbeddingFloatResults::new;
    }

    @Override
    protected MlChunkedTextEmbeddingFloatResults createTestInstance() {
        return createRandomResults();
    }

    @Override
    protected MlChunkedTextEmbeddingFloatResults mutateInstance(MlChunkedTextEmbeddingFloatResults instance) throws IOException {
        return switch (randomIntBetween(0, 1)) {
            case 0 -> new MlChunkedTextEmbeddingFloatResults(
                instance.getResultsField() + "foo",
                instance.getChunks(),
                instance.isTruncated
            );
            case 1 -> new MlChunkedTextEmbeddingFloatResults(
                instance.getResultsField(),
                instance.getChunks(),
                instance.isTruncated == false
            );
            default -> throw new IllegalArgumentException("unexpected case");
        };
    }
}
