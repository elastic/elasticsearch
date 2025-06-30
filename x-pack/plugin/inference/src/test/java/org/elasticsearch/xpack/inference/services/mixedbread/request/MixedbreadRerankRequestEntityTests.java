/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.request;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.mixedbread.rerank.MixedbreadRerankTaskSettings;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;

public class MixedbreadRerankRequestEntityTests extends ESTestCase {

    public void testXContent_SingleRequest_WritesAllFieldsIfDefined() throws IOException {
        var entity = new MixedbreadRerankRequestEntity(
            "model",
            "query",
            List.of("abc"),
            2,
            Boolean.TRUE,
            new MixedbreadRerankTaskSettings(null, null)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "model": "model",
                "query": "query",
                "input": [
                    "abc"
                ],
                "top_k": 2,
                "return_documents": true
            }
            """));
    }

    public void testXContent_SingleRequest_WritesMinimalFields() throws IOException {
        var entity = new MixedbreadRerankRequestEntity(
            "model",
            "query",
            List.of("abc"),
            null,
            null,
            new MixedbreadRerankTaskSettings(null, null)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "model": "model",
                "query": "query",
                "input": [
                    "abc"
                ]
            }
            """));
    }

    public void testXContent_SingleRequest_OverridesTopKField() throws IOException {
        var entity = new MixedbreadRerankRequestEntity(
            "model",
            "query",
            List.of("abc"),
            null,
            null,
            new MixedbreadRerankTaskSettings(null, 2)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "model": "model",
                "query": "query",
                "input": [
                    "abc"
                ],
                "top_k": 2
            }
            """));
    }

    public void testXContent_SingleRequest_OverridesReturnDocumentsField() throws IOException {
        var entity = new MixedbreadRerankRequestEntity(
            "model",
            "query",
            List.of("abc"),
            null,
            null,
            new MixedbreadRerankTaskSettings(Boolean.TRUE, null)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "model": "model",
                "query": "query",
                "input": [
                    "abc"
                ],
                "return_documents": true
            }
            """));
    }

    public void testXContent_MultipleRequests_WritesAllFieldsIfDefined() throws IOException {
        var entity = new MixedbreadRerankRequestEntity(
            "model",
            "query",
            List.of("abc", "def"),
            2,
            Boolean.TRUE,
            new MixedbreadRerankTaskSettings(null, null)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "model": "model",
                "query": "query",
                "input": [
                    "abc",
                    "def"
                ],
                "top_k": 2,
                "return_documents": true
            }
            """));
    }
}
