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
import org.elasticsearch.xpack.core.inference.results.ByteEmbedding;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingByteResults;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class TextEmbeddingByteResultsTests extends AbstractWireSerializingTestCase<TextEmbeddingByteResults> {
    public static TextEmbeddingByteResults createRandomResults() {
        int embeddings = randomIntBetween(1, 10);
        List<ByteEmbedding> embeddingResults = new ArrayList<>(embeddings);

        for (int i = 0; i < embeddings; i++) {
            embeddingResults.add(createRandomEmbedding());
        }

        return new TextEmbeddingByteResults(embeddingResults);
    }

    private static ByteEmbedding createRandomEmbedding() {
        int columns = randomIntBetween(1, 10);
        byte[] bytes = new byte[columns];

        for (int i = 0; i < columns; i++) {
            bytes[i] = randomByte();
        }

<<<<<<< HEAD
        return new ByteEmbedding(floats);
    }

    public void testToXContent_CreatesTheRightFormatForASingleEmbedding() throws IOException {
        var entity = new TextEmbeddingByteResults(List.of(new ByteEmbedding(List.of((byte) 23))));

        assertThat(
            entity.asMap(),
            is(Map.of(TextEmbeddingByteResults.TEXT_EMBEDDING_BYTES, List.of(Map.of(ByteEmbedding.EMBEDDING, List.of((byte) 23)))))
        );
=======
        return new TextEmbeddingByteResults.Embedding(bytes);
    }

    public void testToXContent_CreatesTheRightFormatForASingleEmbedding() throws IOException {
        var entity = new TextEmbeddingByteResults(List.of(new TextEmbeddingByteResults.Embedding(new byte[] { (byte) 23 })));
>>>>>>> main

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
        var entity = new TextEmbeddingByteResults(
<<<<<<< HEAD
            List.of(new ByteEmbedding(List.of((byte) 23)), new ByteEmbedding(List.of((byte) 24)))

        );

        assertThat(
            entity.asMap(),
            is(
                Map.of(
                    TextEmbeddingByteResults.TEXT_EMBEDDING_BYTES,
                    List.of(Map.of(ByteEmbedding.EMBEDDING, List.of((byte) 23)), Map.of(ByteEmbedding.EMBEDDING, List.of((byte) 24)))
                )
=======
            List.of(
                new TextEmbeddingByteResults.Embedding(new byte[] { (byte) 23 }),
                new TextEmbeddingByteResults.Embedding(new byte[] { (byte) 24 })
>>>>>>> main
            )
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
        var results = new TextEmbeddingByteResults(
<<<<<<< HEAD
            List.of(new ByteEmbedding(List.of((byte) 23, (byte) 24)), new ByteEmbedding(List.of((byte) 25, (byte) 26)))
=======
            List.of(
                new TextEmbeddingByteResults.Embedding(new byte[] { (byte) 23, (byte) 24 }),
                new TextEmbeddingByteResults.Embedding(new byte[] { (byte) 25, (byte) 26 })
            )
>>>>>>> main
        ).transformToCoordinationFormat();

        assertThat(
            results,
            is(
                List.of(
                    new org.elasticsearch.xpack.core.ml.inference.results.TextEmbeddingResults(
                        TextEmbeddingByteResults.TEXT_EMBEDDING_BYTES,
                        new double[] { 23F, 24F },
                        false
                    ),
                    new org.elasticsearch.xpack.core.ml.inference.results.TextEmbeddingResults(
                        TextEmbeddingByteResults.TEXT_EMBEDDING_BYTES,
                        new double[] { 25F, 26F },
                        false
                    )
                )
            )
        );
    }

    @Override
    protected Writeable.Reader<TextEmbeddingByteResults> instanceReader() {
        return TextEmbeddingByteResults::new;
    }

    @Override
    protected TextEmbeddingByteResults createTestInstance() {
        return createRandomResults();
    }

    @Override
    protected TextEmbeddingByteResults mutateInstance(TextEmbeddingByteResults instance) throws IOException {
        // if true we reduce the embeddings list by a random amount, if false we add an embedding to the list
        if (randomBoolean()) {
            // -1 to remove at least one item from the list
            int end = randomInt(instance.embeddings().size() - 1);
            return new TextEmbeddingByteResults(instance.embeddings().subList(0, end));
        } else {
            List<ByteEmbedding> embeddings = new ArrayList<>(instance.embeddings());
            embeddings.add(createRandomEmbedding());
            return new TextEmbeddingByteResults(embeddings);
        }
    }

    public static Map<String, Object> buildExpectation(List<List<Byte>> embeddings) {
        return Map.of(
            TextEmbeddingByteResults.TEXT_EMBEDDING_BYTES,
            embeddings.stream().map(embedding -> Map.of(ByteEmbedding.EMBEDDING, embedding)).toList()
        );
    }
}
