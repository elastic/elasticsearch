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
import org.elasticsearch.xpack.core.ml.inference.results.MlTextEmbeddingResults;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class InferenceTextEmbeddingByteResultsTests extends AbstractWireSerializingTestCase<InferenceTextEmbeddingByteResults> {
    public static InferenceTextEmbeddingByteResults createRandomResults() {
        int embeddings = randomIntBetween(1, 10);
        List<InferenceByteEmbedding> embeddingResults = new ArrayList<>(embeddings);

        for (int i = 0; i < embeddings; i++) {
            embeddingResults.add(createRandomEmbedding());
        }

        return new InferenceTextEmbeddingByteResults(embeddingResults);
    }

    private static InferenceByteEmbedding createRandomEmbedding() {
        int columns = randomIntBetween(1, 10);
        byte[] bytes = new byte[columns];

        for (int i = 0; i < columns; i++) {
            bytes[i] = randomByte();
        }

        return new InferenceByteEmbedding(bytes);
    }

    public void testToXContent_CreatesTheRightFormatForASingleEmbedding() throws IOException {
        var entity = new InferenceTextEmbeddingByteResults(List.of(new InferenceByteEmbedding(new byte[] { (byte) 23 })));

        String xContentResult = Strings.toString(entity, true, true);
        assertThat(xContentResult, is("""
            {
              "text_embedding_bytes" : [
                {
                  "embedding" : [
                    23
                  ]
                }
              ]
            }"""));
    }

    public void testToXContent_CreatesTheRightFormatForMultipleEmbeddings() throws IOException {
        var entity = new InferenceTextEmbeddingByteResults(
            List.of(new InferenceByteEmbedding(new byte[] { (byte) 23 }), new InferenceByteEmbedding(new byte[] { (byte) 24 }))
        );

        String xContentResult = Strings.toString(entity, true, true);
        assertThat(xContentResult, is("""
            {
              "text_embedding_bytes" : [
                {
                  "embedding" : [
                    23
                  ]
                },
                {
                  "embedding" : [
                    24
                  ]
                }
              ]
            }"""));
    }

    public void testTransformToCoordinationFormat() {
        var results = new InferenceTextEmbeddingByteResults(
            List.of(
                new InferenceByteEmbedding(new byte[] { (byte) 23, (byte) 24 }),
                new InferenceByteEmbedding(new byte[] { (byte) 25, (byte) 26 })
            )
        ).transformToCoordinationFormat();

        assertThat(
            results,
            is(
                List.of(
                    new MlTextEmbeddingResults(InferenceTextEmbeddingByteResults.TEXT_EMBEDDING_BYTES, new double[] { 23F, 24F }, false),
                    new MlTextEmbeddingResults(InferenceTextEmbeddingByteResults.TEXT_EMBEDDING_BYTES, new double[] { 25F, 26F }, false)
                )
            )
        );
    }

    @Override
    protected Writeable.Reader<InferenceTextEmbeddingByteResults> instanceReader() {
        return InferenceTextEmbeddingByteResults::new;
    }

    @Override
    protected InferenceTextEmbeddingByteResults createTestInstance() {
        return createRandomResults();
    }

    @Override
    protected InferenceTextEmbeddingByteResults mutateInstance(InferenceTextEmbeddingByteResults instance) throws IOException {
        // if true we reduce the embeddings list by a random amount, if false we add an embedding to the list
        if (randomBoolean()) {
            // -1 to remove at least one item from the list
            int end = randomInt(instance.embeddings().size() - 1);
            return new InferenceTextEmbeddingByteResults(instance.embeddings().subList(0, end));
        } else {
            List<InferenceByteEmbedding> embeddings = new ArrayList<>(instance.embeddings());
            embeddings.add(createRandomEmbedding());
            return new InferenceTextEmbeddingByteResults(embeddings);
        }
    }

    public static Map<String, Object> buildExpectationByte(List<List<Byte>> embeddings) {
        return Map.of(
            InferenceTextEmbeddingByteResults.TEXT_EMBEDDING_BYTES,
            embeddings.stream().map(embedding -> Map.of(InferenceByteEmbedding.EMBEDDING, embedding)).toList()
        );
    }
}
