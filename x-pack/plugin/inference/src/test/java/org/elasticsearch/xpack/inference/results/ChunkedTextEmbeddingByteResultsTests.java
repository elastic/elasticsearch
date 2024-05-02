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
import org.elasticsearch.xpack.core.inference.results.ChunkedTextEmbeddingByteResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingByteResults;
import org.elasticsearch.xpack.core.ml.inference.results.ChunkedNlpInferenceResults;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class ChunkedTextEmbeddingByteResultsTests extends AbstractWireSerializingTestCase<ChunkedTextEmbeddingByteResults> {

    public static ChunkedTextEmbeddingByteResults createRandomResults() {
        int numChunks = randomIntBetween(1, 5);
        var chunks = new ArrayList<ChunkedTextEmbeddingByteResults.EmbeddingChunk>(numChunks);

        for (int i = 0; i < numChunks; i++) {
            chunks.add(createRandomChunk());
        }

        return new ChunkedTextEmbeddingByteResults(chunks, randomBoolean());
    }

    private static ChunkedTextEmbeddingByteResults.EmbeddingChunk createRandomChunk() {
        int columns = randomIntBetween(1, 10);
        List<Byte> bytes = new ArrayList<>(columns);

        for (int i = 0; i < columns; i++) {
            bytes.add(randomByte());
        }

        return new ChunkedTextEmbeddingByteResults.EmbeddingChunk(randomAlphaOfLength(6), bytes);
    }

    public void testToXContent_CreatesTheRightJsonForASingleChunk() {
        var entity = new ChunkedTextEmbeddingByteResults(
            List.of(new ChunkedTextEmbeddingByteResults.EmbeddingChunk("text", List.of((byte) 1))),
            false
        );

        assertThat(
            entity.asMap(),
            is(
                Map.of(
                    ChunkedTextEmbeddingByteResults.FIELD_NAME,
                    List.of(Map.of(ChunkedNlpInferenceResults.TEXT, "text", ChunkedNlpInferenceResults.INFERENCE, List.of((byte) 1)))
                )
            )
        );
        String xContentResult = Strings.toString(entity, true, true);
        assertThat(xContentResult, is("""
            {
              "text_embedding_byte_chunk" : [
                {
                  "text" : "text",
                  "inference" : [
                    1
                  ]
                }
              ]
            }"""));
    }

    public void testToXContent_CreatesTheRightJsonForASingleChunk_ForTextEmbeddingByteResults() {
        var entity = ChunkedTextEmbeddingByteResults.of(
            List.of("text"),
            new TextEmbeddingByteResults(List.of(new TextEmbeddingByteResults.Embedding(List.of((byte) 1))))
        );

        assertThat(entity.size(), is(1));

        var firstEntry = entity.get(0);

        assertThat(
            firstEntry.asMap(),
            is(
                Map.of(
                    ChunkedTextEmbeddingByteResults.FIELD_NAME,
                    List.of(Map.of(ChunkedNlpInferenceResults.TEXT, "text", ChunkedNlpInferenceResults.INFERENCE, List.of((byte) 1)))
                )
            )
        );
        String xContentResult = Strings.toString(firstEntry, true, true);
        assertThat(xContentResult, is("""
            {
              "text_embedding_byte_chunk" : [
                {
                  "text" : "text",
                  "inference" : [
                    1
                  ]
                }
              ]
            }"""));
    }

    public void testToXContent_ThrowsWhenInputSizeIsDifferentThanEmbeddings() {
        var exception = expectThrows(
            IllegalArgumentException.class,
            () -> ChunkedTextEmbeddingByteResults.of(
                List.of("text", "text2"),
                new TextEmbeddingByteResults(List.of(new TextEmbeddingByteResults.Embedding(List.of((byte) 1))))
            )
        );

        assertThat(exception.getMessage(), is("The number of inputs [2] does not match the embeddings [1]"));
    }

    @Override
    protected Writeable.Reader<ChunkedTextEmbeddingByteResults> instanceReader() {
        return ChunkedTextEmbeddingByteResults::new;
    }

    @Override
    protected ChunkedTextEmbeddingByteResults createTestInstance() {
        return createRandomResults();
    }

    @Override
    protected ChunkedTextEmbeddingByteResults mutateInstance(ChunkedTextEmbeddingByteResults instance) throws IOException {
        return null;
    }
}
