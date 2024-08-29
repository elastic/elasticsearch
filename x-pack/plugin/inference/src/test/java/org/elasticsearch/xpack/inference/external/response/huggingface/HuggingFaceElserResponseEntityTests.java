/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.huggingface;

import org.apache.http.HttpResponse;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentEOFException;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.results.SparseEmbeddingResultsTests;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.results.SparseEmbeddingResultsTests.buildExpectationSparseEmbeddings;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HuggingFaceElserResponseEntityTests extends ESTestCase {
    public void testFromResponse_CreatesTextExpansionResults() throws IOException {
        String responseJson = """
            [
                {
                    ".": 0.133155956864357,
                    "the": 0.6747211217880249
                }
            ]""";

        SparseEmbeddingResults parsedResults = HuggingFaceElserResponseEntity.fromResponse(
            mock(Request.class),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            parsedResults.asMap(),
            is(
                buildExpectationSparseEmbeddings(
                    List.of(new SparseEmbeddingResultsTests.EmbeddingExpectation(Map.of(".", 0.13315596f, "the", 0.67472112f), false))
                )
            )
        );
    }

    public void testFromResponse_CreatesTextExpansionResults_ThatAreTruncated() throws IOException {
        var request = mock(Request.class);
        when(request.getTruncationInfo()).thenReturn(new boolean[] { true });

        String responseJson = """
            [
                {
                    ".": 0.133155956864357,
                    "the": 0.6747211217880249
                }
            ]""";

        SparseEmbeddingResults parsedResults = HuggingFaceElserResponseEntity.fromResponse(
            request,
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            parsedResults.asMap(),
            is(
                buildExpectationSparseEmbeddings(
                    List.of(new SparseEmbeddingResultsTests.EmbeddingExpectation(Map.of(".", 0.13315596f, "the", 0.67472112f), true))
                )
            )
        );
    }

    public void testFromResponse_CreatesTextExpansionResultsForMultipleItems_TruncationIsNull() throws IOException {
        String responseJson = """
            [
                {
                    ".": 0.133155956864357,
                    "the": 0.6747211217880249
                },
                {
                    "hi": 0.133155956864357,
                    "super": 0.6747211217880249
                }
            ]""";

        SparseEmbeddingResults parsedResults = HuggingFaceElserResponseEntity.fromResponse(
            mock(Request.class),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            parsedResults.asMap(),
            is(
                buildExpectationSparseEmbeddings(
                    List.of(
                        new SparseEmbeddingResultsTests.EmbeddingExpectation(Map.of(".", 0.13315596f, "the", 0.67472112f), false),
                        new SparseEmbeddingResultsTests.EmbeddingExpectation(Map.of("hi", 0.13315596f, "super", 0.67472112f), false)
                    )
                )
            )
        );
    }

    public void testFromResponse_CreatesTextExpansionResults_WithTruncation() throws IOException {
        String responseJson = """
            [
                {
                    ".": 0.133155956864357,
                    "the": 0.6747211217880249
                },
                {
                    "hi": 0.133155956864357,
                    "super": 0.6747211217880249
                }
            ]""";

        var request = mock(Request.class);
        when(request.getTruncationInfo()).thenReturn(new boolean[] { true, false });

        SparseEmbeddingResults parsedResults = HuggingFaceElserResponseEntity.fromResponse(
            request,
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            parsedResults.asMap(),
            is(
                buildExpectationSparseEmbeddings(
                    List.of(
                        new SparseEmbeddingResultsTests.EmbeddingExpectation(Map.of(".", 0.13315596f, "the", 0.67472112f), true),
                        new SparseEmbeddingResultsTests.EmbeddingExpectation(Map.of("hi", 0.13315596f, "super", 0.67472112f), false)
                    )
                )
            )
        );
    }

    public void testFromResponse_CreatesTextExpansionResults_WithTruncationLessArrayLessThanExpected() throws IOException {
        String responseJson = """
            [
                {
                    ".": 0.133155956864357,
                    "the": 0.6747211217880249
                },
                {
                    "hi": 0.133155956864357,
                    "super": 0.6747211217880249
                }
            ]""";

        var request = mock(Request.class);
        when(request.getTruncationInfo()).thenReturn(new boolean[] {});

        SparseEmbeddingResults parsedResults = HuggingFaceElserResponseEntity.fromResponse(
            mock(Request.class),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            parsedResults.asMap(),
            is(
                buildExpectationSparseEmbeddings(
                    List.of(
                        new SparseEmbeddingResultsTests.EmbeddingExpectation(Map.of(".", 0.13315596f, "the", 0.67472112f), false),
                        new SparseEmbeddingResultsTests.EmbeddingExpectation(Map.of("hi", 0.13315596f, "super", 0.67472112f), false)
                    )
                )
            )
        );
    }

    public void testFails_NotAnArray() {
        String responseJson = """
            {
              "field": "abc"
            }
            """;

        var thrownException = expectThrows(
            ParsingException.class,
            () -> HuggingFaceElserResponseEntity.fromResponse(
                mock(Request.class),
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(
            thrownException.getMessage(),
            is("Failed to parse object: expecting token of type [START_ARRAY] but found [START_OBJECT]")
        );
    }

    public void testFails_ValueString() {
        String responseJson = """
            [
              {
                "field": "abc"
              }
            ]
            """;

        var thrownException = expectThrows(
            ParsingException.class,
            () -> HuggingFaceElserResponseEntity.fromResponse(
                mock(Request.class),
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(
            thrownException.getMessage(),
            is("Failed to parse object: expecting token of type [VALUE_NUMBER] but found [VALUE_STRING]")
        );
    }

    public void testFromResponse_CreatesResultsWithValueInt() throws IOException {
        String responseJson = """
            [
              {
                "field": 1
              }
            ]
            """;

        SparseEmbeddingResults parsedResults = HuggingFaceElserResponseEntity.fromResponse(
            mock(Request.class),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            parsedResults.asMap(),
            is(
                buildExpectationSparseEmbeddings(
                    List.of(new SparseEmbeddingResultsTests.EmbeddingExpectation(Map.of("field", 1.0f), false))
                )
            )
        );
    }

    public void testFromResponse_CreatesResultsWithValueLong() throws IOException {
        String responseJson = """
            [
              {
                "field": 40294967295
              }
            ]
            """;

        SparseEmbeddingResults parsedResults = HuggingFaceElserResponseEntity.fromResponse(
            mock(Request.class),
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            parsedResults.asMap(),
            is(
                buildExpectationSparseEmbeddings(
                    List.of(new SparseEmbeddingResultsTests.EmbeddingExpectation(Map.of("field", 4.0294965E10F), false))
                )
            )
        );
    }

    public void testFails_ValueObject() {
        String responseJson = """
            [
              {
                "field": {}
              }
            ]
            """;

        var thrownException = expectThrows(
            ParsingException.class,
            () -> HuggingFaceElserResponseEntity.fromResponse(
                mock(Request.class),
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(
            thrownException.getMessage(),
            is("Failed to parse object: expecting token of type [VALUE_NUMBER] but found [START_OBJECT]")
        );
    }

    public void testFails_ResponseIsInvalidJson_MissingSquareBracket() {
        String responseJson = """
            [
              {
                "field": 0.1
              }
            """;

        var thrownException = expectThrows(
            XContentEOFException.class,
            () -> HuggingFaceElserResponseEntity.fromResponse(
                mock(Request.class),
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(thrownException.getMessage(), containsString("expected close marker for Array (start marker at"));
    }

    public void testFails_ResponseIsInvalidJson_MissingField() {
        String responseJson = """
            [
              {
                : 0.1
              }
            ]
            """;

        var thrownException = expectThrows(
            XContentParseException.class,
            () -> HuggingFaceElserResponseEntity.fromResponse(
                mock(Request.class),
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(thrownException.getMessage(), containsString("Unexpected character"));
    }
}
