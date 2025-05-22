/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.response;

import org.apache.http.HttpResponse;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class HuggingFaceEmbeddingsResponseEntityTests extends ESTestCase {
    public void testFromResponse_CreatesResultsForASingleItem_ArrayFormat() throws IOException {
        String responseJson = """
            [
                  [
                      0.014539449,
                      -0.015288644
                  ]
            ]
            """;

        TextEmbeddingFloatResults parsedResults = HuggingFaceEmbeddingsResponseEntity.fromResponse(
            mock(Request.class),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            parsedResults.embeddings(),
            is(List.of(new TextEmbeddingFloatResults.Embedding(new float[] { 0.014539449F, -0.015288644F })))
        );
    }

    public void testFromResponse_CreatesResultsForASingleItem_ObjectFormat() throws IOException {
        String responseJson = """
            {
              "embeddings": [
                  [
                      0.014539449,
                      -0.015288644
                  ]
              ]
            }
            """;

        TextEmbeddingFloatResults parsedResults = HuggingFaceEmbeddingsResponseEntity.fromResponse(
            mock(Request.class),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            parsedResults.embeddings(),
            is(List.of(new TextEmbeddingFloatResults.Embedding(new float[] { 0.014539449F, -0.015288644F })))
        );
    }

    public void testFromResponse_CreatesResultsForMultipleItems_ArrayFormat() throws IOException {
        String responseJson = """
            [
                  [
                      0.014539449,
                      -0.015288644
                  ],
                  [
                      0.0123,
                      -0.0123
                  ]
            ]
            """;

        TextEmbeddingFloatResults parsedResults = HuggingFaceEmbeddingsResponseEntity.fromResponse(
            mock(Request.class),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            parsedResults.embeddings(),
            is(
                List.of(
                    new TextEmbeddingFloatResults.Embedding(new float[] { 0.014539449F, -0.015288644F }),
                    new TextEmbeddingFloatResults.Embedding(new float[] { 0.0123F, -0.0123F })
                )
            )
        );
    }

    public void testFromResponse_CreatesResultsForMultipleItems_ObjectFormat() throws IOException {
        String responseJson = """
            {
              "embeddings": [
                  [
                      0.014539449,
                      -0.015288644
                  ],
                  [
                      0.0123,
                      -0.0123
                  ]
              ]
            }
            """;

        TextEmbeddingFloatResults parsedResults = HuggingFaceEmbeddingsResponseEntity.fromResponse(
            mock(Request.class),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            parsedResults.embeddings(),
            is(
                List.of(
                    new TextEmbeddingFloatResults.Embedding(new float[] { 0.014539449F, -0.015288644F }),
                    new TextEmbeddingFloatResults.Embedding(new float[] { 0.0123F, -0.0123F })
                )
            )
        );
    }

    public void testFromResponse_FailsWhenArrayOfObjects() {
        String responseJson = """
            [
                  {}
            ]
            """;

        var thrownException = expectThrows(
            ParsingException.class,
            () -> HuggingFaceEmbeddingsResponseEntity.fromResponse(
                mock(Request.class),
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(
            thrownException.getMessage(),
            is("Failed to parse object: expecting token of type [START_ARRAY] but found [START_OBJECT]")
        );
    }

    public void testFromResponse_FailsWhenEmbeddingsFieldIsNotPresent() {
        String responseJson = """
            {
              "not_embeddings": [
                  [
                      0.014539449,
                      -0.015288644
                  ]
              ]
            }
            """;

        var thrownException = expectThrows(
            IllegalStateException.class,
            () -> HuggingFaceEmbeddingsResponseEntity.fromResponse(
                mock(Request.class),
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(thrownException.getMessage(), is("Failed to find required field [embeddings] in Hugging Face embeddings response"));
    }

    public void testFromResponse_FailsWhenEmbeddingsFieldNotAnArray() {
        String responseJson = """
            {
              "embeddings": {
                  "a": [
                      0.014539449,
                      -0.015288644
                  ]
              }
            }
            """;

        var thrownException = expectThrows(
            ParsingException.class,
            () -> HuggingFaceEmbeddingsResponseEntity.fromResponse(
                mock(Request.class),
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(
            thrownException.getMessage(),
            is("Failed to parse object: expecting token of type [START_ARRAY] but found [START_OBJECT]")
        );
    }

    public void testFromResponse_FailsWhenEmbeddingValueIsAString_ArrayFormat() {
        String responseJson = """
            [
                  [
                      "abc"
                  ]
            ]
            """;

        var thrownException = expectThrows(
            ParsingException.class,
            () -> HuggingFaceEmbeddingsResponseEntity.fromResponse(
                mock(Request.class),
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(
            thrownException.getMessage(),
            is("Failed to parse object: expecting token of type [VALUE_NUMBER] but found [VALUE_STRING]")
        );
    }

    public void testFromResponse_FailsWhenEmbeddingValueIsAString_ObjectFormat() {
        String responseJson = """
            {
              "embeddings": [
                  [
                      "abc"
                  ]
              ]
            }
            """;

        var thrownException = expectThrows(
            ParsingException.class,
            () -> HuggingFaceEmbeddingsResponseEntity.fromResponse(
                mock(Request.class),
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(
            thrownException.getMessage(),
            is("Failed to parse object: expecting token of type [VALUE_NUMBER] but found [VALUE_STRING]")
        );
    }

    public void testFromResponse_SucceedsWhenEmbeddingValueIsInt_ArrayFormat() throws IOException {
        String responseJson = """
            [
                  [
                      1
                  ]
            ]
            """;

        TextEmbeddingFloatResults parsedResults = HuggingFaceEmbeddingsResponseEntity.fromResponse(
            mock(Request.class),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(parsedResults.embeddings(), is(List.of(new TextEmbeddingFloatResults.Embedding(new float[] { 1.0F }))));
    }

    public void testFromResponse_SucceedsWhenEmbeddingValueIsInt_ObjectFormat() throws IOException {
        String responseJson = """
            {
              "embeddings": [
                  [
                      1
                  ]
              ]
            }
            """;

        TextEmbeddingFloatResults parsedResults = HuggingFaceEmbeddingsResponseEntity.fromResponse(
            mock(Request.class),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(parsedResults.embeddings(), is(List.of(new TextEmbeddingFloatResults.Embedding(new float[] { 1.0F }))));
    }

    public void testFromResponse_SucceedsWhenEmbeddingValueIsLong_ArrayFormat() throws IOException {
        String responseJson = """
            [
                  [
                      40294967295
                  ]
            ]
            """;

        TextEmbeddingFloatResults parsedResults = HuggingFaceEmbeddingsResponseEntity.fromResponse(
            mock(Request.class),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(parsedResults.embeddings(), is(List.of(new TextEmbeddingFloatResults.Embedding(new float[] { 4.0294965E10F }))));
    }

    public void testFromResponse_SucceedsWhenEmbeddingValueIsLong_ObjectFormat() throws IOException {
        String responseJson = """
            {
              "embeddings": [
                  [
                      40294967295
                  ]
              ]
            }
            """;

        TextEmbeddingFloatResults parsedResults = HuggingFaceEmbeddingsResponseEntity.fromResponse(
            mock(Request.class),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(parsedResults.embeddings(), is(List.of(new TextEmbeddingFloatResults.Embedding(new float[] { 4.0294965E10F }))));
    }

    public void testFromResponse_FailsWhenEmbeddingValueIsAnObject_ObjectFormat() {
        String responseJson = """
            {
              "embeddings": [
                  [
                      {}
                  ]
              ]
            }
            """;

        var thrownException = expectThrows(
            ParsingException.class,
            () -> HuggingFaceEmbeddingsResponseEntity.fromResponse(
                mock(Request.class),
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(
            thrownException.getMessage(),
            is("Failed to parse object: expecting token of type [VALUE_NUMBER] but found [START_OBJECT]")
        );
    }

    public void testFromResponse_FailsWithUnknownToken() {
        String responseJson = """
            "super"
            """;

        var thrownException = expectThrows(
            ParsingException.class,
            () -> HuggingFaceEmbeddingsResponseEntity.fromResponse(
                mock(Request.class),
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(thrownException.getMessage(), is("Failed to parse object: unexpected token [VALUE_STRING] found"));
    }
}
