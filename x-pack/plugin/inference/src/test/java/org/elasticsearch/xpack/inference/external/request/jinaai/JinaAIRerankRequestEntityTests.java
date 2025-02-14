/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.jinaai;

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
    public void testXContent_SingleRequest_WritesModelAndTopNIfDefined() throws IOException {
        var entity = new JinaAIRerankRequestEntity("query", List.of("abc"), new JinaAIRerankTaskSettings(8, null), "model");

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
                "top_n": 8
            }
            """));
    }

    public void testXContent_SingleRequest_WritesModelAndTopNIfDefined_ReturnDocumentsTrue() throws IOException {
        var entity = new JinaAIRerankRequestEntity("query", List.of("abc"), new JinaAIRerankTaskSettings(8, true), "model");

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
                "return_documents": true
            }
            """));
    }

    public void testXContent_SingleRequest_WritesModelAndTopNIfDefined_ReturnDocumentsFalse() throws IOException {
        var entity = new JinaAIRerankRequestEntity("query", List.of("abc"), new JinaAIRerankTaskSettings(8, false), "model");

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

    public void testXContent_SingleRequest_DoesNotWriteTopNIfNull() throws IOException {
        var entity = new JinaAIRerankRequestEntity("query", List.of("abc"), null, "model");

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

    public void testXContent_MultipleRequests_WritesModelAndTopNIfDefined() throws IOException {
        var entity = new JinaAIRerankRequestEntity("query", List.of("abc", "def"), new JinaAIRerankTaskSettings(8, null), "model");

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
                "top_n": 8
            }
            """));
    }

    public void testXContent_MultipleRequests_DoesNotWriteTopNIfNull() throws IOException {
        var entity = new JinaAIRerankRequestEntity("query", List.of("abc", "def"), null, "model");

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

}
