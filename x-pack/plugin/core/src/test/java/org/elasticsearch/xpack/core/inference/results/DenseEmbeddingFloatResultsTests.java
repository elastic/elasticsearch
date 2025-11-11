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

public class DenseEmbeddingFloatResultsTests extends AbstractWireSerializingTestCase<DenseEmbeddingFloatResults> {
    public static DenseEmbeddingFloatResults createRandomResults() {
        int embeddings = randomIntBetween(1, 10);
        List<DenseEmbeddingFloatResults.Embedding> embeddingResults = new ArrayList<>(embeddings);

        for (int i = 0; i < embeddings; i++) {
            embeddingResults.add(createRandomEmbedding());
        }

        return new DenseEmbeddingFloatResults(embeddingResults);
    }

    private static DenseEmbeddingFloatResults.Embedding createRandomEmbedding() {
        int columns = randomIntBetween(1, 10);
        float[] floats = new float[columns];
        for (int i = 0; i < columns; i++) {
            floats[i] = randomFloat();
        }

        return new DenseEmbeddingFloatResults.Embedding(floats);
    }

    public void testToXContent_CreatesTheRightFormatForASingleEmbedding() throws IOException {
        var entity = new DenseEmbeddingFloatResults(List.of(new DenseEmbeddingFloatResults.Embedding(new float[] { 0.1F })));

        String xContentResult = Strings.toString(entity, true, true);
        assertThat(xContentResult, is("""
            {
              "text_embedding" : [
                {
                  "embedding" : [
                    0.1
                  ]
                }
              ]
            }"""));
    }

    public void testToXContent_CreatesTheRightFormatForMultipleEmbeddings() throws IOException {
        var entity = new DenseEmbeddingFloatResults(
            List.of(
                new DenseEmbeddingFloatResults.Embedding(new float[] { 0.1F }),
                new DenseEmbeddingFloatResults.Embedding(new float[] { 0.2F })
            )

        );

        String xContentResult = Strings.toString(entity, true, true);
        assertThat(xContentResult, is("""
            {
              "text_embedding" : [
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
        var results = new DenseEmbeddingFloatResults(
            List.of(
                new DenseEmbeddingFloatResults.Embedding(new float[] { 0.1F, 0.2F }),
                new DenseEmbeddingFloatResults.Embedding(new float[] { 0.3F, 0.4F })
            )
        ).transformToCoordinationFormat();

        assertThat(
            results,
            is(
                List.of(
                    new MlDenseEmbeddingResults(DenseEmbeddingFloatResults.TEXT_EMBEDDING, new double[] { 0.1F, 0.2F }, false),
                    new MlDenseEmbeddingResults(DenseEmbeddingFloatResults.TEXT_EMBEDDING, new double[] { 0.3F, 0.4F }, false)
                )
            )
        );
    }

    public void testGetFirstEmbeddingSize() {
        var firstEmbeddingSize = new DenseEmbeddingFloatResults(
            List.of(
                new DenseEmbeddingFloatResults.Embedding(new float[] { 0.1F, 0.2F }),
                new DenseEmbeddingFloatResults.Embedding(new float[] { 0.3F, 0.4F })
            )
        ).getFirstEmbeddingSize();

        assertThat(firstEmbeddingSize, is(2));
    }

    public void testEmbeddingMerge() {
        DenseEmbeddingFloatResults.Embedding embedding1 = new DenseEmbeddingFloatResults.Embedding(new float[] { 0.1f, 0.2f, 0.3f, 0.4f });
        DenseEmbeddingFloatResults.Embedding embedding2 = new DenseEmbeddingFloatResults.Embedding(new float[] { 0.0f, 0.4f, 0.1f, 1.0f });
        DenseEmbeddingFloatResults.Embedding embedding3 = new DenseEmbeddingFloatResults.Embedding(new float[] { 0.2f, 0.9f, 0.8f, 0.1f });
        DenseEmbeddingFloatResults.Embedding mergedEmbedding = embedding1.merge(embedding2);
        assertThat(mergedEmbedding, equalTo(new DenseEmbeddingFloatResults.Embedding(new float[] { 0.05f, 0.3f, 0.2f, 0.7f })));
        mergedEmbedding = mergedEmbedding.merge(embedding3);
        assertThat(mergedEmbedding, equalTo(new DenseEmbeddingFloatResults.Embedding(new float[] { 0.1f, 0.5f, 0.4f, 0.5f })));
    }

    @Override
    protected Writeable.Reader<DenseEmbeddingFloatResults> instanceReader() {
        return DenseEmbeddingFloatResults::new;
    }

    @Override
    protected DenseEmbeddingFloatResults createTestInstance() {
        return createRandomResults();
    }

    @Override
    protected DenseEmbeddingFloatResults mutateInstance(DenseEmbeddingFloatResults instance) throws IOException {
        // if true we reduce the embeddings list by a random amount, if false we add an embedding to the list
        if (randomBoolean()) {
            // -1 to remove at least one item from the list
            int end = randomInt(instance.embeddings().size() - 1);
            return new DenseEmbeddingFloatResults(instance.embeddings().subList(0, end));
        } else {
            List<DenseEmbeddingFloatResults.Embedding> embeddings = new ArrayList<>(instance.embeddings());
            embeddings.add(createRandomEmbedding());
            return new DenseEmbeddingFloatResults(embeddings);
        }
    }

    public static Map<String, Object> buildExpectationFloat(List<float[]> embeddings) {
        return Map.of(
            DenseEmbeddingFloatResults.TEXT_EMBEDDING,
            embeddings.stream().map(DenseEmbeddingFloatResults.Embedding::new).toList()
        );
    }

    public static Map<String, Object> buildExpectationByte(List<byte[]> embeddings) {
        return Map.of(
            DenseEmbeddingByteResults.TEXT_EMBEDDING_BYTES,
            embeddings.stream().map(DenseEmbeddingByteResults.Embedding::new).toList()
        );
    }

    public static Map<String, Object> buildExpectationBinary(List<byte[]> embeddings) {
        return Map.of(
            DenseEmbeddingBitResults.TEXT_EMBEDDING_BITS,
            embeddings.stream().map(DenseEmbeddingByteResults.Embedding::new).toList()
        );
    }
}
