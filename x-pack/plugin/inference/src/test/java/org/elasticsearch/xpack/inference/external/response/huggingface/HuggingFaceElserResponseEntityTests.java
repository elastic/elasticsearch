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
import org.elasticsearch.xpack.core.ml.inference.results.TextExpansionResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
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
                    ".": 0.133155956864357,
                    "the": 0.6747211217880249
                }
            ]""";

        List<TextExpansionResults> parsedResults = HuggingFaceElserResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );
        Map<String, Float> tokenWeightMap = parsedResults.get(0)
            .getWeightedTokens()
            .stream()
            .collect(Collectors.toMap(TextExpansionResults.WeightedToken::token, TextExpansionResults.WeightedToken::weight));

        // the results get truncated because weighted token stores them as a float
        assertThat(tokenWeightMap.size(), is(2));
        assertThat(tokenWeightMap.get("."), is(0.13315596f));
        assertThat(tokenWeightMap.get("the"), is(0.67472112f));
        assertFalse(parsedResults.get(0).isTruncated());
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

        List<TextExpansionResults> parsedResults = HuggingFaceElserResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );
        {
            var parsedResult = parsedResults.get(0);
            Map<String, Float> tokenWeightMap = parsedResult.getWeightedTokens()
                .stream()
                .collect(Collectors.toMap(TextExpansionResults.WeightedToken::token, TextExpansionResults.WeightedToken::weight));

            // the results get truncated because weighted token stores them as a float
            assertThat(tokenWeightMap.size(), is(2));
            assertThat(tokenWeightMap.get("."), is(0.13315596f));
            assertThat(tokenWeightMap.get("the"), is(0.67472112f));
            assertFalse(parsedResult.isTruncated());
        }
        {
            var parsedResult = parsedResults.get(1);
            Map<String, Float> tokenWeightMap = parsedResult.getWeightedTokens()
                .stream()
                .collect(Collectors.toMap(TextExpansionResults.WeightedToken::token, TextExpansionResults.WeightedToken::weight));

            // the results get truncated because weighted token stores them as a float
            assertThat(tokenWeightMap.size(), is(2));
            assertThat(tokenWeightMap.get("hi"), is(0.13315596f));
            assertThat(tokenWeightMap.get("super"), is(0.67472112f));
            assertFalse(parsedResult.isTruncated());
        }
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

    public void testFromResponse_CreatesResultsWithValueInt() throws IOException {
        String responseJson = """
            [
              {
                "field": 1
              }
            ]
            """;

        TextExpansionResults parsedResults = HuggingFaceElserResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        ).get(0);
        Map<String, Float> tokenWeightMap = parsedResults.getWeightedTokens()
            .stream()
            .collect(Collectors.toMap(TextExpansionResults.WeightedToken::token, TextExpansionResults.WeightedToken::weight));

        assertThat(tokenWeightMap.size(), is(1));
        assertThat(tokenWeightMap.get("field"), is(1.0f));
        assertFalse(parsedResults.isTruncated());
    }

    public void testFromResponse_CreatesResultsWithValueLong() throws IOException {
        String responseJson = """
            [
              {
                "field": 40294967295
              }
            ]
            """;

        TextExpansionResults parsedResults = HuggingFaceElserResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        ).get(0);
        Map<String, Float> tokenWeightMap = parsedResults.getWeightedTokens()
            .stream()
            .collect(Collectors.toMap(TextExpansionResults.WeightedToken::token, TextExpansionResults.WeightedToken::weight));

        assertThat(tokenWeightMap.size(), is(1));
        assertThat(tokenWeightMap.get("field"), is(4.0294965E10F));
        assertFalse(parsedResults.isTruncated());
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
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(thrownException.getMessage(), containsString("expected close marker for Array (start marker at [Source: (byte[])"));
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
                new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
            )
        );

        assertThat(thrownException.getMessage(), containsString("Unexpected character"));
    }
}
