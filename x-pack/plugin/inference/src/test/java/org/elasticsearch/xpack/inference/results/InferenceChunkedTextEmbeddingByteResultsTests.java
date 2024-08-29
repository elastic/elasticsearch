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
import org.elasticsearch.xpack.core.inference.results.InferenceChunkedTextEmbeddingByteResults;
import org.elasticsearch.xpack.core.inference.results.InferenceTextEmbeddingByteResults;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class InferenceChunkedTextEmbeddingByteResultsTests extends AbstractWireSerializingTestCase<
    InferenceChunkedTextEmbeddingByteResults> {

    public static InferenceChunkedTextEmbeddingByteResults createRandomResults() {
        int numChunks = randomIntBetween(1, 5);
        var chunks = new ArrayList<InferenceChunkedTextEmbeddingByteResults.InferenceByteEmbeddingChunk>(numChunks);

        for (int i = 0; i < numChunks; i++) {
            chunks.add(createRandomChunk());
        }

        return new InferenceChunkedTextEmbeddingByteResults(chunks, randomBoolean());
    }

    private static InferenceChunkedTextEmbeddingByteResults.InferenceByteEmbeddingChunk createRandomChunk() {
        int columns = randomIntBetween(1, 10);
        byte[] bytes = new byte[columns];
        for (int i = 0; i < columns; i++) {
            bytes[i] = randomByte();
        }

        return new InferenceChunkedTextEmbeddingByteResults.InferenceByteEmbeddingChunk(randomAlphaOfLength(6), bytes);
    }

    public void testToXContent_CreatesTheRightJsonForASingleChunk() {
        var entity = new InferenceChunkedTextEmbeddingByteResults(
            List.of(new InferenceChunkedTextEmbeddingByteResults.InferenceByteEmbeddingChunk("text", new byte[] { (byte) 1 })),
            false
        );

        assertThat(
            entity.asMap(),
            is(
                Map.of(
                    InferenceChunkedTextEmbeddingByteResults.FIELD_NAME,
                    List.of(new InferenceChunkedTextEmbeddingByteResults.InferenceByteEmbeddingChunk("text", new byte[] { (byte) 1 }))
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
        var entity = InferenceChunkedTextEmbeddingByteResults.listOf(
            List.of("text"),
            new InferenceTextEmbeddingByteResults(
                List.of(new InferenceTextEmbeddingByteResults.InferenceByteEmbedding(new byte[] { (byte) 1 }))
            )
        );

        assertThat(entity.size(), is(1));

        var firstEntry = entity.get(0);

        assertThat(
            firstEntry.asMap(),
            is(
                Map.of(
                    InferenceChunkedTextEmbeddingByteResults.FIELD_NAME,
                    List.of(new InferenceChunkedTextEmbeddingByteResults.InferenceByteEmbeddingChunk("text", new byte[] { (byte) 1 }))
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
            () -> InferenceChunkedTextEmbeddingByteResults.listOf(
                List.of("text", "text2"),
                new InferenceTextEmbeddingByteResults(
                    List.of(new InferenceTextEmbeddingByteResults.InferenceByteEmbedding(new byte[] { (byte) 1 }))
                )
            )
        );

        assertThat(exception.getMessage(), is("The number of inputs [2] does not match the embeddings [1]"));
    }

    @Override
    protected Writeable.Reader<InferenceChunkedTextEmbeddingByteResults> instanceReader() {
        return InferenceChunkedTextEmbeddingByteResults::new;
    }

    @Override
    protected InferenceChunkedTextEmbeddingByteResults createTestInstance() {
        return createRandomResults();
    }

    @Override
    protected InferenceChunkedTextEmbeddingByteResults mutateInstance(InferenceChunkedTextEmbeddingByteResults instance)
        throws IOException {
        return randomValueOtherThan(instance, InferenceChunkedTextEmbeddingByteResultsTests::createRandomResults);
    }
}
