/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.request;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.voyageai.rerank.VoyageAIRerankTaskSettings;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;

public class VoyageAIRerankRequestEntityTests extends ESTestCase {
    public void testXContent_SingleRequest_WritesAllFieldsIfDefined() throws IOException {
        var entity = new VoyageAIRerankRequestEntity(
            "query",
            List.of("abc"),
            Boolean.TRUE,
            12,
            new VoyageAIRerankTaskSettings(8, Boolean.FALSE, null),
            "model"
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "model": "model",
                "query": "query",
                "documents": [
                    "abc"
                ],
                "return_documents": true,
                "top_k": 12
            }
            """));
    }

    public void testXContent_SingleRequest_WritesMinimalFields() throws IOException {
        var entity = new VoyageAIRerankRequestEntity(
            "query",
            List.of("abc"),
            null,
            null,
            new VoyageAIRerankTaskSettings(null, true, null),
            "model"
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "model": "model",
                "query": "query",
                "documents": [
                    "abc"
                ],
                "return_documents": true
            }
            """));
    }

    public void testXContent_SingleRequest_WritesModelAndTopKIfDefined_TruncationTrue() throws IOException {
        var entity = new VoyageAIRerankRequestEntity(
            "query",
            List.of("abc"),
            null,
            null,
            new VoyageAIRerankTaskSettings(8, false, true),
            "model"
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "model": "model",
                "query": "query",
                "documents": [
                    "abc"
                ],
                "return_documents": false,
                "top_k": 8,
                "truncation": true
            }
            """));
    }

    public void testXContent_SingleRequest_WritesModelAndTopKIfDefined_TruncationFalse() throws IOException {
        var entity = new VoyageAIRerankRequestEntity(
            "query",
            List.of("abc"),
            null,
            null,
            new VoyageAIRerankTaskSettings(8, false, false),
            "model"
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "model": "model",
                "query": "query",
                "documents": [
                    "abc"
                ],
                "return_documents": false,
                "top_k": 8,
                "truncation": false
            }
            """));
    }

    public void testXContent_MultipleRequests_WritesAllFieldsIfDefined() throws IOException {
        var entity = new VoyageAIRerankRequestEntity(
            "query",
            List.of("abc", "def"),
            Boolean.FALSE,
            11,
            new VoyageAIRerankTaskSettings(8, null, null),
            "model"
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "model": "model",
                "query": "query",
                "documents": [
                    "abc",
                    "def"
                ],
                "return_documents": false,
                "top_k": 11
            }
            """));
    }

    public void testXContent_MultipleRequests_DoesNotWriteTopKIfNull() throws IOException {
        var entity = new VoyageAIRerankRequestEntity("query", List.of("abc", "def"), null, null, null, "model");

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "model": "model",
                "query": "query",
                "documents": [
                   "abc",
                   "def"
                ]
            }
            """));
    }

    public void testXContent_UsesTaskSettingsTopNIfRootIsNotDefined() throws IOException {
        var entity = new VoyageAIRerankRequestEntity(
            "query",
            List.of("abc"),
            null,
            null,
            new VoyageAIRerankTaskSettings(8, Boolean.FALSE, null),
            "model"
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "model": "model",
                "query": "query",
                "documents": [
                    "abc"
                ],
                "return_documents": false,
                "top_k": 8
            }
            """));
    }

}
