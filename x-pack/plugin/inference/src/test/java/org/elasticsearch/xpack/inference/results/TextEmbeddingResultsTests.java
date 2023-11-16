/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.results;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class TextEmbeddingResultsTests extends ESTestCase {
    public void testToXContent_CreatesTheRightFormatForASingleEmbedding() throws IOException {
        var entity = new TextEmbeddingResults(List.of(new TextEmbeddingResults.Embedding(List.of(0.1F))));

        assertThat(
            entity.asMap(),
            is(Map.of(TextEmbeddingResults.TEXT_EMBEDDING, List.of(Map.of(TextEmbeddingResults.Embedding.EMBEDDING, List.of(0.1F)))))
        );

        String xContentResult = toJsonString(entity);
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

        String xContentResult = toJsonString(entity);
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

    private static String toJsonString(ToXContentFragment entity) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON).prettyPrint();
        builder.startObject();
        entity.toXContent(builder, null);
        builder.endObject();

        return Strings.toString(builder);
    }
}
