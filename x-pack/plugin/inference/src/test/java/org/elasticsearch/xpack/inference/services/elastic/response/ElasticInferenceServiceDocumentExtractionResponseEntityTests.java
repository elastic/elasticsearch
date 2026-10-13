/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.response;

import org.apache.http.HttpResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.inference.results.DocumentExtractionResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class ElasticInferenceServiceDocumentExtractionResponseEntityTests extends ESTestCase {

    public void testFromResponse_CreatesResultsForASingleItem() throws IOException {
        String responseJson = """
            {
                "results": [
                    {
                        "content": "# Annual Report 2025",
                        "format": "markdown",
                        "metadata": {
                            "title": "Annual Report 2025"
                        }
                    }
                ]
            }
            """;

        var parsedResults = ElasticInferenceServiceDocumentExtractionResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(parsedResults, instanceOf(DocumentExtractionResults.class));
        var results = (DocumentExtractionResults) parsedResults;
        assertThat(
            results.results(),
            is(List.of(new DocumentExtractionResults.Result("# Annual Report 2025", "markdown", Map.of("title", "Annual Report 2025"))))
        );
    }

    public void testFromResponse_CreatesResultsForMultipleItems() throws IOException {
        String responseJson = """
            {
                "results": [
                    {
                        "content": "content 1",
                        "format": "markdown"
                    },
                    {
                        "content": "content 2",
                        "format": "markdown"
                    }
                ]
            }
            """;

        var parsedResults = ElasticInferenceServiceDocumentExtractionResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(parsedResults, instanceOf(DocumentExtractionResults.class));
        var results = (DocumentExtractionResults) parsedResults;
        assertThat(
            results.results(),
            is(
                List.of(
                    new DocumentExtractionResults.Result("content 1", "markdown", Map.of()),
                    new DocumentExtractionResults.Result("content 2", "markdown", Map.of())
                )
            )
        );
    }

    public void testFromResponse_WithoutFormat_DefaultsToMarkdown() throws IOException {
        String responseJson = """
            {
                "results": [
                    {
                        "content": "some content"
                    }
                ]
            }
            """;

        var parsedResults = ElasticInferenceServiceDocumentExtractionResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(parsedResults, instanceOf(DocumentExtractionResults.class));
        var results = (DocumentExtractionResults) parsedResults;
        assertThat(results.results(), is(List.of(new DocumentExtractionResults.Result("some content", "markdown", Map.of()))));
    }

    public void testFromResponse_IgnoresUnknownFields() throws IOException {
        String responseJson = """
            {
                "results": [
                    {
                        "content": "some content",
                        "format": "markdown",
                        "unknown_field": "unknown_value"
                    }
                ],
                "another_unknown_field": 123
            }
            """;

        var parsedResults = ElasticInferenceServiceDocumentExtractionResponseEntity.fromResponse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(parsedResults, instanceOf(DocumentExtractionResults.class));
        var results = (DocumentExtractionResults) parsedResults;
        assertThat(results.results(), is(List.of(new DocumentExtractionResults.Result("some content", "markdown", Map.of()))));
    }
}
