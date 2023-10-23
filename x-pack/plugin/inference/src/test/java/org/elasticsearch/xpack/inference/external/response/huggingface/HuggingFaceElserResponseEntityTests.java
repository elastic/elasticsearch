/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.huggingface;

import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;

import org.apache.http.HttpResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class HuggingFaceElserResponseEntityTests extends ESTestCase {
    public void testFromResponse_CreatesTextExpansionResults() throws IOException {
        String responseJson = """
            [
              {
                "outputs": [
                  [
                    [
                      ".",
                      0.133155956864357
                    ],
                    [
                      "the",
                      0.6747211217880249
                    ]
                  ]
                ]
              }
            ]
            """;
        TextExpansionResults parsedResults = HuggingFaceElserResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );
        Map<String, Float> tokenWeightMap = parsedResults.getWeightedTokens()
            .stream()
            .collect(Collectors.toMap(TextExpansionResults.WeightedToken::token, TextExpansionResults.WeightedToken::weight));

        // the results get truncated because weighted token stores them as a float
        assertThat(tokenWeightMap.get("."), is(0.13315596f));
        assertThat(tokenWeightMap.get("the"), is(0.67472112f));
        assertFalse(parsedResults.isTruncated());
    }

    public void testFails_NotAnArray() {
        String responseJson = """
            {
              "field": "abc"
            }
            """;

        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> HuggingFaceElserResponseEntity.fromResponse(
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(thrownException.getMessage(), is("Expected ELSER Hugging Face response to be an array of objects"));
    }

    public void testFails_ArrayOfArrays() {
        String responseJson = """
            [
              [
                {
                  "outputs": "abc"
                }
              ]
            ]
            """;

        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> HuggingFaceElserResponseEntity.fromResponse(
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(thrownException.getMessage(), is("Expected ELSER Hugging Face response to have an [outputs] field"));
    }

    public void testFails_NoOutputsField() {
        String responseJson = """
            [
              {
                "field": "abc"
              }
            ]
            """;

        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> HuggingFaceElserResponseEntity.fromResponse(
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(thrownException.getMessage(), is("Expected ELSER Hugging Face response to have an [outputs] field"));
    }

    public void testFails_OutputsNotArray() {
        String responseJson = """
            [
              {
                "outputs": {
                  "field": "abc"
                }
              }
            ]
            """;

        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> HuggingFaceElserResponseEntity.fromResponse(
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(thrownException.getMessage(), is("Expected ELSER Hugging Face response node [outputs] to be an array"));
    }

    public void testFails_OutputsInternalNodeNotArray() {
        String responseJson = """
            [
              {
                "outputs": [
                  {
                    "field": "abc"
                  }
                ]
              }
            ]
            """;

        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> HuggingFaceElserResponseEntity.fromResponse(
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(thrownException.getMessage(), is("Expected ELSER Hugging Face response node [outputs] internal node to be an array"));
    }

    public void testFails_TupleNotArray() {
        String responseJson = """
            [
              {
                "outputs": [
                  [
                    {
                      ".": 0.133155956864357
                    }
                  ]
                ]
              }
            ]
            """;

        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> HuggingFaceElserResponseEntity.fromResponse(
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(thrownException.getMessage(), is("Expected ELSER Hugging Face response result tuple to be an array"));
    }

    public void testFails_TupleArraySize3() {
        String responseJson = """
            [
              {
                "outputs": [
                  [
                    [
                      ".",
                      "b",
                      0.133155956864357
                    ]
                  ]
                ]
              }
            ]
            """;

        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> HuggingFaceElserResponseEntity.fromResponse(
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(thrownException.getMessage(), is("Expected Elser Hugging Face response result tuple to be of size two"));
    }

    public void testFails_TupleArraySize1() {
        String responseJson = """
            [
              {
                "outputs": [
                  [
                    [
                      0.133155956864357
                    ]
                  ]
                ]
              }
            ]
            """;

        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> HuggingFaceElserResponseEntity.fromResponse(
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(thrownException.getMessage(), is("Expected Elser Hugging Face response result tuple to be of size two"));
    }

    public void testFails_TupleArrayFloatThenString() {
        String responseJson = """
            [
              {
                "outputs": [
                  [
                    [
                      0.133155956864357,
                      "."
                    ]
                  ]
                ]
              }
            ]
            """;

        var thrownException = expectThrows(
            InvalidFormatException.class,
            () -> HuggingFaceElserResponseEntity.fromResponse(
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(
            thrownException.getMessage(),
            containsString("Cannot deserialize value of type `float` from String \".\": not a valid `float` value")
        );
    }

    public void testFails_TupleArrayStringObject() {
        String responseJson = """
            [
              {
                "outputs": [
                  [
                    [
                      ".",
                      {}
                    ]
                  ]
                ]
              }
            ]
            """;

        var thrownException = expectThrows(
            MismatchedInputException.class,
            () -> HuggingFaceElserResponseEntity.fromResponse(
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(
            thrownException.getMessage(),
            containsString("Cannot deserialize value of type `float` from Object value (token `JsonToken.START_OBJECT`)")
        );
    }
}
