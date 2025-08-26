/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.jinaai.request;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.jinaai.rerank.JinaAIRerankTaskSettings;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;
import static org.hamcrest.MatcherAssert.assertThat;

public class JinaAIRerankRequestEntityTests extends ESTestCase {
    public void testXContent_SingleRequest_WritesAllFieldsIfDefined() throws IOException {
        var entity = new JinaAIRerankRequestEntity(
            "query",
            List.of("abc"),
            Boolean.TRUE,
            12,
            new JinaAIRerankTaskSettings(8, Boolean.FALSE),
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
                "top_n": 12,
                "return_documents": true
            }
            """));
    }

    public void testXContent_SingleRequest_WritesMinimalFields() throws IOException {
        var entity = new JinaAIRerankRequestEntity("query", List.of("abc"), null, null, new JinaAIRerankTaskSettings(null, null), "model");

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "model": "model",
                "query": "query",
                "documents": [
                    "abc"
                ]
            }
            """));
    }

    public void testXContent_MultipleRequests_WritesAllFieldsIfDefined() throws IOException {
        var entity = new JinaAIRerankRequestEntity(
            "query",
            List.of("abc", "def"),
            Boolean.FALSE,
            12,
            new JinaAIRerankTaskSettings(8, Boolean.TRUE),
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
                "top_n": 12,
                "return_documents": false
            }
            """));
    }

    public void testXContent_MultipleRequests_WritesMinimalFields() throws IOException {
        var entity = new JinaAIRerankRequestEntity("query", List.of("abc", "def"), null, null, null, "model");

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

    public void testXContent_SingleRequest_UsesTaskSettingsTopNIfRootIsNotDefined() throws IOException {
        var entity = new JinaAIRerankRequestEntity(
            "query",
            List.of("abc"),
            null,
            null,
            new JinaAIRerankTaskSettings(8, Boolean.FALSE),
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
                "top_n": 8,
                "return_documents": false
            }
            """));
    }

}
