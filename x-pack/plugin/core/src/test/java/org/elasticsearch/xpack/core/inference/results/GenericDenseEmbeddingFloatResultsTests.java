/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.inference.results.MlDenseEmbeddingResults;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class GenericDenseEmbeddingFloatResultsTests extends AbstractWireSerializingTestCase<GenericDenseEmbeddingFloatResults> {
    public static GenericDenseEmbeddingFloatResults createRandomResults() {
        int embeddings = randomIntBetween(1, 10);
        List<GenericDenseEmbeddingFloatResults.Embedding> embeddingResults = new ArrayList<>(embeddings);

        for (int i = 0; i < embeddings; i++) {
            embeddingResults.add(createRandomEmbedding());
        }

        return new GenericDenseEmbeddingFloatResults(embeddingResults);
    }

    private static GenericDenseEmbeddingFloatResults.Embedding createRandomEmbedding() {
        int columns = randomIntBetween(1, 10);
        float[] floats = new float[columns];
        for (int i = 0; i < columns; i++) {
            floats[i] = randomFloat();
        }

        return new GenericDenseEmbeddingFloatResults.Embedding(floats);
    }

    public void testToXContent_CreatesTheRightFormatForASingleEmbedding() throws IOException {
        var entity = new GenericDenseEmbeddingFloatResults(List.of(new GenericDenseEmbeddingFloatResults.Embedding(new float[] { 0.1F })));

        String xContentResult = Strings.toString(entity, true, true);
        assertThat(xContentResult, is("""
            {
              "embeddings" : [
                {
                  "embedding" : [
                    0.1
                  ]
                }
              ]
            }"""));
    }

    public void testToXContent_CreatesTheRightFormatForMultipleEmbeddings() throws IOException {
        var entity = new GenericDenseEmbeddingFloatResults(
            List.of(
                new GenericDenseEmbeddingFloatResults.Embedding(new float[] { 0.1F }),
                new GenericDenseEmbeddingFloatResults.Embedding(new float[] { 0.2F })
            )

        );

        String xContentResult = Strings.toString(entity, true, true);
        assertThat(xContentResult, is("""
            {
              "embeddings" : [
                {
                  "embedding" : [
                    0.1
                  ]
                },
                {
                  "embedding" : [
                    0.2
                  ]
                }
              ]
            }"""));
    }

    public void testTransformToCoordinationFormat() {
        var results = new GenericDenseEmbeddingFloatResults(
            List.of(
                new GenericDenseEmbeddingFloatResults.Embedding(new float[] { 0.1F, 0.2F }),
                new GenericDenseEmbeddingFloatResults.Embedding(new float[] { 0.3F, 0.4F })
            )
        ).transformToCoordinationFormat();

        assertThat(
            results,
            is(
                List.of(
                    new MlDenseEmbeddingResults(GenericDenseEmbeddingFloatResults.EMBEDDINGS, new double[] { 0.1F, 0.2F }, false),
                    new MlDenseEmbeddingResults(GenericDenseEmbeddingFloatResults.EMBEDDINGS, new double[] { 0.3F, 0.4F }, false)
                )
            )
        );
    }

    public void testGetFirstEmbeddingSize() {
        var firstEmbeddingSize = new GenericDenseEmbeddingFloatResults(
            List.of(
                new GenericDenseEmbeddingFloatResults.Embedding(new float[] { 0.1F, 0.2F }),
                new GenericDenseEmbeddingFloatResults.Embedding(new float[] { 0.3F, 0.4F })
            )
        ).getFirstEmbeddingSize();

        assertThat(firstEmbeddingSize, is(2));
    }

    public void testEmbeddingMerge() {
        GenericDenseEmbeddingFloatResults.Embedding embedding1 = new GenericDenseEmbeddingFloatResults.Embedding(
            new float[] { 0.1f, 0.2f, 0.3f, 0.4f }
        );
        GenericDenseEmbeddingFloatResults.Embedding embedding2 = new GenericDenseEmbeddingFloatResults.Embedding(
            new float[] { 0.0f, 0.4f, 0.1f, 1.0f }
        );
        GenericDenseEmbeddingFloatResults.Embedding embedding3 = new GenericDenseEmbeddingFloatResults.Embedding(
            new float[] { 0.2f, 0.9f, 0.8f, 0.1f }
        );
        GenericDenseEmbeddingFloatResults.Embedding mergedEmbedding = embedding1.merge(embedding2);
        assertThat(mergedEmbedding, equalTo(new GenericDenseEmbeddingFloatResults.Embedding(new float[] { 0.05f, 0.3f, 0.2f, 0.7f })));
        mergedEmbedding = mergedEmbedding.merge(embedding3);
        assertThat(mergedEmbedding, equalTo(new GenericDenseEmbeddingFloatResults.Embedding(new float[] { 0.1f, 0.5f, 0.4f, 0.5f })));
    }

    @Override
    protected Writeable.Reader<GenericDenseEmbeddingFloatResults> instanceReader() {
        return GenericDenseEmbeddingFloatResults::new;
    }

    @Override
    protected GenericDenseEmbeddingFloatResults createTestInstance() {
        return createRandomResults();
    }

    @Override
    protected GenericDenseEmbeddingFloatResults mutateInstance(GenericDenseEmbeddingFloatResults instance) throws IOException {
        // if true we reduce the embeddings list by a random amount, if false we add an embedding to the list
        if (randomBoolean()) {
            // -1 to remove at least one item from the list
            int end = randomInt(instance.embeddings().size() - 1);
            return new GenericDenseEmbeddingFloatResults(instance.embeddings().subList(0, end));
        } else {
            List<GenericDenseEmbeddingFloatResults.Embedding> embeddings = new ArrayList<>(instance.embeddings());
            embeddings.add(createRandomEmbedding());
            return new GenericDenseEmbeddingFloatResults(embeddings);
        }
    }

    public static Map<String, Object> buildExpectationFloat(List<float[]> embeddings) {
        return Map.of(
            GenericDenseEmbeddingFloatResults.EMBEDDINGS,
            embeddings.stream().map(GenericDenseEmbeddingFloatResults.Embedding::new).toList()
        );
    }

    public static Map<String, Object> buildExpectationByte(List<byte[]> embeddings) {
        return Map.of(
            GenericDenseEmbeddingByteResults.EMBEDDINGS_BYTES,
            embeddings.stream().map(GenericDenseEmbeddingByteResults.Embedding::new).toList()
        );
    }

    public static Map<String, Object> buildExpectationBinary(List<byte[]> embeddings) {
        return Map.of(
            GenericDenseEmbeddingBitResults.EMBEDDINGS_BITS,
            embeddings.stream().map(DenseEmbeddingByteResults.Embedding::new).toList()
        );
    }
}
