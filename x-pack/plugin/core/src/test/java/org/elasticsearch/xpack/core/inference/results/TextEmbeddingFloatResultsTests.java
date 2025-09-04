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
import org.elasticsearch.xpack.core.ml.inference.results.MlTextEmbeddingResults;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class TextEmbeddingFloatResultsTests extends AbstractWireSerializingTestCase<TextEmbeddingFloatResults> {
    public static TextEmbeddingFloatResults createRandomResults() {
        int embeddings = randomIntBetween(1, 10);
        List<TextEmbeddingFloatResults.Embedding> embeddingResults = new ArrayList<>(embeddings);

        for (int i = 0; i < embeddings; i++) {
            embeddingResults.add(createRandomEmbedding());
        }

        return new TextEmbeddingFloatResults(embeddingResults);
    }

    private static TextEmbeddingFloatResults.Embedding createRandomEmbedding() {
        int columns = randomIntBetween(1, 10);
        float[] floats = new float[columns];
        for (int i = 0; i < columns; i++) {
            floats[i] = randomFloat();
        }

        return new TextEmbeddingFloatResults.Embedding(floats);
    }

    public void testToXContent_CreatesTheRightFormatForASingleEmbedding() throws IOException {
        var entity = new TextEmbeddingFloatResults(List.of(new TextEmbeddingFloatResults.Embedding(new float[] { 0.1F })));

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
        var entity = new TextEmbeddingFloatResults(
            List.of(
                new TextEmbeddingFloatResults.Embedding(new float[] { 0.1F }),
                new TextEmbeddingFloatResults.Embedding(new float[] { 0.2F })
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
        var results = new TextEmbeddingFloatResults(
            List.of(
                new TextEmbeddingFloatResults.Embedding(new float[] { 0.1F, 0.2F }),
                new TextEmbeddingFloatResults.Embedding(new float[] { 0.3F, 0.4F })
            )
        ).transformToCoordinationFormat();

        assertThat(
            results,
            is(
                List.of(
                    new MlTextEmbeddingResults(TextEmbeddingFloatResults.TEXT_EMBEDDING, new double[] { 0.1F, 0.2F }, false),
                    new MlTextEmbeddingResults(TextEmbeddingFloatResults.TEXT_EMBEDDING, new double[] { 0.3F, 0.4F }, false)
                )
            )
        );
    }

    public void testGetFirstEmbeddingSize() {
        var firstEmbeddingSize = new TextEmbeddingFloatResults(
            List.of(
                new TextEmbeddingFloatResults.Embedding(new float[] { 0.1F, 0.2F }),
                new TextEmbeddingFloatResults.Embedding(new float[] { 0.3F, 0.4F })
            )
        ).getFirstEmbeddingSize();

        assertThat(firstEmbeddingSize, is(2));
    }

    public void testEmbeddingMerge() {
        TextEmbeddingFloatResults.Embedding embedding1 = new TextEmbeddingFloatResults.Embedding(new float[] { 0.1f, 0.2f, 0.3f, 0.4f });
        TextEmbeddingFloatResults.Embedding embedding2 = new TextEmbeddingFloatResults.Embedding(new float[] { 0.0f, 0.4f, 0.1f, 1.0f });
        TextEmbeddingFloatResults.Embedding embedding3 = new TextEmbeddingFloatResults.Embedding(new float[] { 0.2f, 0.9f, 0.8f, 0.1f });
        TextEmbeddingFloatResults.Embedding mergedEmbedding = embedding1.merge(embedding2);
        assertThat(mergedEmbedding, equalTo(new TextEmbeddingFloatResults.Embedding(new float[] { 0.05f, 0.3f, 0.2f, 0.7f })));
        mergedEmbedding = mergedEmbedding.merge(embedding3);
        assertThat(mergedEmbedding, equalTo(new TextEmbeddingFloatResults.Embedding(new float[] { 0.1f, 0.5f, 0.4f, 0.5f })));
    }

    @Override
    protected Writeable.Reader<TextEmbeddingFloatResults> instanceReader() {
        return TextEmbeddingFloatResults::new;
    }

    @Override
    protected TextEmbeddingFloatResults createTestInstance() {
        return createRandomResults();
    }

    @Override
    protected TextEmbeddingFloatResults mutateInstance(TextEmbeddingFloatResults instance) throws IOException {
        // if true we reduce the embeddings list by a random amount, if false we add an embedding to the list
        if (randomBoolean()) {
            // -1 to remove at least one item from the list
            int end = randomInt(instance.embeddings().size() - 1);
            return new TextEmbeddingFloatResults(instance.embeddings().subList(0, end));
        } else {
            List<TextEmbeddingFloatResults.Embedding> embeddings = new ArrayList<>(instance.embeddings());
            embeddings.add(createRandomEmbedding());
            return new TextEmbeddingFloatResults(embeddings);
        }
    }

    public static Map<String, Object> buildExpectationFloat(List<float[]> embeddings) {
        return Map.of(TextEmbeddingFloatResults.TEXT_EMBEDDING, embeddings.stream().map(TextEmbeddingFloatResults.Embedding::new).toList());
    }

    public static Map<String, Object> buildExpectationByte(List<byte[]> embeddings) {
        return Map.of(
            TextEmbeddingByteResults.TEXT_EMBEDDING_BYTES,
            embeddings.stream().map(TextEmbeddingByteResults.Embedding::new).toList()
        );
    }

    public static Map<String, Object> buildExpectationBinary(List<byte[]> embeddings) {
        return Map.of("text_embedding_bits", embeddings.stream().map(TextEmbeddingByteResults.Embedding::new).toList());
    }
}
