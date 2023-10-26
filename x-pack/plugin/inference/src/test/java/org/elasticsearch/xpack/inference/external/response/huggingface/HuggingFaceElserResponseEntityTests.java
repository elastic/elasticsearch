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
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class HuggingFaceElserResponseEntityTests extends ESTestCase {
    public void testFromResponse_CreatesTextExpansionResults() throws IOException {
        String responseJson = """
            [
                {
                    ".": 0.133155956864357,
                    "the": 0.6747211217880249
                }
            ]""";

        TextExpansionResults parsedResults = HuggingFaceElserResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );
        Map<String, Float> tokenWeightMap = parsedResults.getWeightedTokens()
            .stream()
            .collect(Collectors.toMap(TextExpansionResults.WeightedToken::token, TextExpansionResults.WeightedToken::weight));

        // the results get truncated because weighted token stores them as a float
        assertThat(tokenWeightMap.size(), is(2));
        assertThat(tokenWeightMap.get("."), is(0.13315596f));
        assertThat(tokenWeightMap.get("the"), is(0.67472112f));
        assertFalse(parsedResults.isTruncated());
    }

    public void testFromResponse_CreatesTextExpansionResultsForFirstItem() throws IOException {
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

        TextExpansionResults parsedResults = HuggingFaceElserResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );
        Map<String, Float> tokenWeightMap = parsedResults.getWeightedTokens()
            .stream()
            .collect(Collectors.toMap(TextExpansionResults.WeightedToken::token, TextExpansionResults.WeightedToken::weight));

        // the results get truncated because weighted token stores them as a float
        assertThat(tokenWeightMap.size(), is(2));
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
            ParsingException.class,
            () -> HuggingFaceElserResponseEntity.fromResponse(
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
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(
            thrownException.getMessage(),
            is("Failed to parse object: expecting token of type [VALUE_NUMBER] but found [VALUE_STRING]")
        );
    }

    public void testFails_ValueInt() {
        String responseJson = """
            [
              {
                "field": 1
              }
            ]
            """;

        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> HuggingFaceElserResponseEntity.fromResponse(
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(
            thrownException.getMessage(),
            is("Failed to parse object: expecting number token of type float or double but found [INT]")
        );
    }

    public void testFails_ValueLong() {
        String responseJson = """
            [
              {
                "field": 40294967295
              }
            ]
            """;

        var thrownException = expectThrows(
            IllegalArgumentException.class,
            () -> HuggingFaceElserResponseEntity.fromResponse(
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(
            thrownException.getMessage(),
            is("Failed to parse object: expecting number token of type float or double but found [LONG]")
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
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(
            thrownException.getMessage(),
            is("Failed to parse object: expecting token of type [VALUE_NUMBER] but found [START_OBJECT]")
        );
    }
}
