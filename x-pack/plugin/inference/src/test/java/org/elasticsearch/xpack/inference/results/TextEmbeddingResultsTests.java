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
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingResults;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class TextEmbeddingResultsTests extends AbstractWireSerializingTestCase<TextEmbeddingResults> {
    public static TextEmbeddingResults createRandomResults() {
        int embeddings = randomIntBetween(1, 10);
        List<TextEmbeddingResults.Embedding> embeddingResults = new ArrayList<>(embeddings);

        for (int i = 0; i < embeddings; i++) {
            embeddingResults.add(createRandomEmbedding());
        }

        return new TextEmbeddingResults(embeddingResults);
    }

    private static TextEmbeddingResults.Embedding createRandomEmbedding() {
        int columns = randomIntBetween(1, 10);
        List<Float> floats = new ArrayList<>(columns);

        for (int i = 0; i < columns; i++) {
            floats.add(randomFloat());
        }

        return new TextEmbeddingResults.Embedding(floats);
    }

    public void testToXContent_CreatesTheRightFormatForASingleEmbedding() throws IOException {
        var entity = new TextEmbeddingResults(List.of(new TextEmbeddingResults.Embedding(List.of(0.1F))));

        assertThat(
            entity.asMap(),
            is(Map.of(TextEmbeddingResults.TEXT_EMBEDDING, List.of(Map.of(TextEmbeddingResults.Embedding.EMBEDDING, List.of(0.1F)))))
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
        var entity = new TextEmbeddingResults(
            List.of(new TextEmbeddingResults.Embedding(List.of(0.1F)), new TextEmbeddingResults.Embedding(List.of(0.2F)))

        );

        assertThat(
            entity.asMap(),
            is(
                Map.of(
                    TextEmbeddingResults.TEXT_EMBEDDING,
                    List.of(
                        Map.of(TextEmbeddingResults.Embedding.EMBEDDING, List.of(0.1F)),
                        Map.of(TextEmbeddingResults.Embedding.EMBEDDING, List.of(0.2F))
                    )
                )
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
        var results = new TextEmbeddingResults(
            List.of(new TextEmbeddingResults.Embedding(List.of(0.1F, 0.2F)), new TextEmbeddingResults.Embedding(List.of(0.3F, 0.4F)))
        ).transformToCoordinationFormat();

        assertThat(
            results,
            is(
                List.of(
                    new org.elasticsearch.xpack.core.ml.inference.results.TextEmbeddingResults(
                        TextEmbeddingResults.TEXT_EMBEDDING,
                        new double[] { 0.1F, 0.2F },
                        false
                    ),
                    new org.elasticsearch.xpack.core.ml.inference.results.TextEmbeddingResults(
                        TextEmbeddingResults.TEXT_EMBEDDING,
                        new double[] { 0.3F, 0.4F },
                        false
                    )
                )
            )
        );
    }

    @Override
    protected Writeable.Reader<TextEmbeddingResults> instanceReader() {
        return TextEmbeddingResults::new;
    }

    @Override
    protected TextEmbeddingResults createTestInstance() {
        return createRandomResults();
    }

    @Override
    protected TextEmbeddingResults mutateInstance(TextEmbeddingResults instance) throws IOException {
        // if true we reduce the embeddings list by a random amount, if false we add an embedding to the list
        if (randomBoolean()) {
            // -1 to remove at least one item from the list
            int end = randomInt(instance.embeddings().size() - 1);
            return new TextEmbeddingResults(instance.embeddings().subList(0, end));
        } else {
            List<TextEmbeddingResults.Embedding> embeddings = new ArrayList<>(instance.embeddings());
            embeddings.add(createRandomEmbedding());
            return new TextEmbeddingResults(embeddings);
        }
    }

    public static Map<String, Object> buildExpectation(List<List<Float>> embeddings) {
        return Map.of(
            TextEmbeddingResults.TEXT_EMBEDDING,
            embeddings.stream().map(embedding -> Map.of(TextEmbeddingResults.Embedding.EMBEDDING, embedding)).toList()
        );
    }
}
