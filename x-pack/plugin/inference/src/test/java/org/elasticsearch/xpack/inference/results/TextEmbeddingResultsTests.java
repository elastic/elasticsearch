/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.results;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.inference.results.InferenceByteEmbedding;
import org.elasticsearch.xpack.core.inference.results.InferenceTextEmbeddingByteResults;
import org.elasticsearch.xpack.core.inference.results.InferenceTextEmbeddingFloatResults;
import org.elasticsearch.xpack.core.ml.inference.results.MlTextEmbeddingResults;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class TextEmbeddingResultsTests extends AbstractWireSerializingTestCase<InferenceTextEmbeddingFloatResults> {
    public static InferenceTextEmbeddingFloatResults createRandomResults() {
        int embeddings = randomIntBetween(1, 10);
        List<InferenceTextEmbeddingFloatResults.InferenceFloatEmbedding> embeddingResults = new ArrayList<>(embeddings);

        for (int i = 0; i < embeddings; i++) {
            embeddingResults.add(createRandomEmbedding());
        }

        return new InferenceTextEmbeddingFloatResults(embeddingResults);
    }

    private static InferenceTextEmbeddingFloatResults.InferenceFloatEmbedding createRandomEmbedding() {
        int columns = randomIntBetween(1, 10);
        float[] floats = new float[columns];
        for (int i = 0; i < columns; i++) {
            floats[i] = randomFloat();
        }

        return new InferenceTextEmbeddingFloatResults.InferenceFloatEmbedding(floats);
    }

    public void testToXContent_CreatesTheRightFormatForASingleEmbedding() throws IOException {
        var entity = new InferenceTextEmbeddingFloatResults(
            List.of(new InferenceTextEmbeddingFloatResults.InferenceFloatEmbedding(new float[] { 0.1F }))
        );

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
        var entity = new InferenceTextEmbeddingFloatResults(
            List.of(
                new InferenceTextEmbeddingFloatResults.InferenceFloatEmbedding(new float[] { 0.1F }),
                new InferenceTextEmbeddingFloatResults.InferenceFloatEmbedding(new float[] { 0.2F })
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
        var results = new InferenceTextEmbeddingFloatResults(
            List.of(
                new InferenceTextEmbeddingFloatResults.InferenceFloatEmbedding(new float[] { 0.1F, 0.2F }),
                new InferenceTextEmbeddingFloatResults.InferenceFloatEmbedding(new float[] { 0.3F, 0.4F })
            )
        ).transformToCoordinationFormat();

        assertThat(
            results,
            is(
                List.of(
                    new MlTextEmbeddingResults(InferenceTextEmbeddingFloatResults.TEXT_EMBEDDING, new double[] { 0.1F, 0.2F }, false),
                    new MlTextEmbeddingResults(InferenceTextEmbeddingFloatResults.TEXT_EMBEDDING, new double[] { 0.3F, 0.4F }, false)
                )
            )
        );
    }

    @Override
    protected Writeable.Reader<InferenceTextEmbeddingFloatResults> instanceReader() {
        return InferenceTextEmbeddingFloatResults::new;
    }

    @Override
    protected InferenceTextEmbeddingFloatResults createTestInstance() {
        return createRandomResults();
    }

    @Override
    protected InferenceTextEmbeddingFloatResults mutateInstance(InferenceTextEmbeddingFloatResults instance) throws IOException {
        // if true we reduce the embeddings list by a random amount, if false we add an embedding to the list
        if (randomBoolean()) {
            // -1 to remove at least one item from the list
            int end = randomInt(instance.embeddings().size() - 1);
            return new InferenceTextEmbeddingFloatResults(instance.embeddings().subList(0, end));
        } else {
            List<InferenceTextEmbeddingFloatResults.InferenceFloatEmbedding> embeddings = new ArrayList<>(instance.embeddings());
            embeddings.add(createRandomEmbedding());
            return new InferenceTextEmbeddingFloatResults(embeddings);
        }
    }

    public static Map<String, Object> buildExpectationFloat(List<float[]> embeddings) {
        return Map.of(
            InferenceTextEmbeddingFloatResults.TEXT_EMBEDDING,
            embeddings.stream().map(InferenceTextEmbeddingFloatResults.InferenceFloatEmbedding::new).toList()
        );
    }

    public static Map<String, Object> buildExpectationByte(List<byte[]> embeddings) {
        return Map.of(
            InferenceTextEmbeddingByteResults.TEXT_EMBEDDING_BYTES,
            embeddings.stream().map(InferenceByteEmbedding::new).toList()
        );
    }

}
