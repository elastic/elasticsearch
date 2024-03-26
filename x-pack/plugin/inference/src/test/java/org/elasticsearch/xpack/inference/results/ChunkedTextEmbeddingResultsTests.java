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
import org.elasticsearch.xpack.core.inference.results.ChunkedTextEmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingResults;
import org.elasticsearch.xpack.core.ml.inference.results.ChunkedNlpInferenceResults;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class ChunkedTextEmbeddingResultsTests extends AbstractWireSerializingTestCase<ChunkedTextEmbeddingResults> {

    public static ChunkedTextEmbeddingResults createRandomResults() {
        var chunks = new ArrayList<org.elasticsearch.xpack.core.ml.inference.results.ChunkedTextEmbeddingResults.EmbeddingChunk>();
        int columns = randomIntBetween(5, 10);
        int numChunks = randomIntBetween(1, 5);

        for (int i = 0; i < numChunks; i++) {
            double[] arr = new double[columns];
            for (int j = 0; j < columns; j++) {
                arr[j] = randomDouble();
            }
            chunks.add(
                new org.elasticsearch.xpack.core.ml.inference.results.ChunkedTextEmbeddingResults.EmbeddingChunk(
                    randomAlphaOfLength(6),
                    arr
                )
            );
        }

        return new ChunkedTextEmbeddingResults(chunks);
    }

    public void testToXContent_CreatesTheRightJsonForASingleChunk() {
        var entity = new ChunkedTextEmbeddingResults(
            List.of(
                new org.elasticsearch.xpack.core.ml.inference.results.ChunkedTextEmbeddingResults.EmbeddingChunk(
                    "text",
                    new double[] { 0.1, 0.2 }
                )
            )
        );

        assertThat(
            entity.asMap(),
            is(
                Map.of(
                    ChunkedTextEmbeddingResults.FIELD_NAME,
                    List.of(Map.of(ChunkedNlpInferenceResults.TEXT, "text", ChunkedNlpInferenceResults.INFERENCE, List.of(0.1, 0.2)))
                )
            )
        );
        String xContentResult = Strings.toString(entity, true, true);
        assertThat(xContentResult, is("""
            {
              "text_embedding_chunk" : [
                {
                  "text" : "text",
                  "inference" : [
                    0.1,
                    0.2
                  ]
                }
              ]
            }"""));
    }

    public void testToXContent_CreatesTheRightJsonForASingleChunk_FromTextEmbeddingResults() {
        var entity = ChunkedTextEmbeddingResults.of(
            List.of("text"),
            new TextEmbeddingResults(List.of(new TextEmbeddingResults.Embedding(List.of(0.1f, 0.2f))))
        );

        assertThat(entity.size(), is(1));

        var firstEntry = entity.get(0);
        assertThat(
            firstEntry.asMap(),
            is(
                Map.of(
                    ChunkedTextEmbeddingResults.FIELD_NAME,
                    List.of(
                        Map.of(
                            ChunkedNlpInferenceResults.TEXT,
                            "text",
                            ChunkedNlpInferenceResults.INFERENCE,
                            List.of((double) 0.1f, (double) 0.2f)
                        )
                    )
                )
            )
        );
        String xContentResult = Strings.toString(firstEntry, true, true);
        assertThat(xContentResult, is(Strings.format("""
            {
              "text_embedding_chunk" : [
                {
                  "text" : "text",
                  "inference" : [
                    %s,
                    %s
                  ]
                }
              ]
            }""", (double) 0.1f, (double) 0.2f)));
    }

    public void testToXContent_ThrowsWhenInputSizeIsDifferentThanEmbeddings() {
        var exception = expectThrows(
            IllegalArgumentException.class,
            () -> ChunkedTextEmbeddingResults.of(
                List.of("text", "text2"),
                new TextEmbeddingResults(List.of(new TextEmbeddingResults.Embedding(List.of(0.1f, 0.2f))))
            )
        );

        assertThat(exception.getMessage(), is("The number of inputs [2] does not match the embeddings [1]"));
    }

    @Override
    protected Writeable.Reader<ChunkedTextEmbeddingResults> instanceReader() {
        return ChunkedTextEmbeddingResults::new;
    }

    @Override
    protected ChunkedTextEmbeddingResults createTestInstance() {
        return createRandomResults();
    }

    @Override
    protected ChunkedTextEmbeddingResults mutateInstance(ChunkedTextEmbeddingResults instance) throws IOException {
        return null;
    }
}
