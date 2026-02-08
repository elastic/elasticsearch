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
import org.elasticsearch.xpack.inference.services.mixedbread.request.rerank.MixedbreadRerankRequestEntity;
import org.elasticsearch.xpack.inference.services.mixedbread.rerank.MixedbreadRerankTaskSettings;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;

public class MixedbreadRerankRequestEntityTests extends ESTestCase {

    public static final String MODEL = "model";
    public static final String QUERY = "query";

    public void testXContent_SingleRequest_WritesAllFieldsIfDefined() throws IOException {
        var entity = new MixedbreadRerankRequestEntity(
            MODEL,
            QUERY,
            List.of("abc"),
            12,
            Boolean.TRUE,
            new MixedbreadRerankTaskSettings(8, Boolean.FALSE)
        );

        assertThat(getXContentResult(entity), equalToIgnoringWhitespaceInJsonString("""
            {
                "model": "model",
                "query": "query",
                "input": [
                    "abc"
                ],
                "top_k": 12,
                "return_input": true
            }
            """));
    }

    public void testXContent_SingleRequest_WritesMinimalFields() throws IOException {
        var entity = new MixedbreadRerankRequestEntity(
            MODEL,
            QUERY,
            List.of("abc"),
            null,
            null,
            new MixedbreadRerankTaskSettings(null, null)
        );

        assertThat(getXContentResult(entity), equalToIgnoringWhitespaceInJsonString("""
            {
                "model": "model",
                "query": "query",
                "input": [
                    "abc"
                ]
            }
            """));
    }

    public void testXContent_MultipleRequests_WritesAllFieldsIfDefined() throws IOException {
        var entity = new MixedbreadRerankRequestEntity(
            MODEL,
            QUERY,
            List.of("abc", "def"),
            12,
            Boolean.FALSE,
            new MixedbreadRerankTaskSettings(8, Boolean.TRUE)
        );

        assertThat(getXContentResult(entity), equalToIgnoringWhitespaceInJsonString("""
            {
                "model": "model",
                "query": "query",
                "input": [
                    "abc",
                    "def"
                ],
                "top_k": 12,
                "return_input": false
            }
            """));
    }

    public void testXContent_MultipleRequests_WritesMinimalFields() throws IOException {
        var entity = new MixedbreadRerankRequestEntity(
            MODEL,
            QUERY,
            List.of("abc", "def"),
            null,
            null,
            new MixedbreadRerankTaskSettings(null, null)
        );

        assertThat(getXContentResult(entity), equalToIgnoringWhitespaceInJsonString("""
            {
                "model": "model",
                "query": "query",
                "input": [
                   "abc",
                   "def"
                ]
            }
            """));
    }

    public void testXContent_SingleRequest_UsesTaskSettingsTopNIfRootIsNotDefined() throws IOException {
        var entity = new MixedbreadRerankRequestEntity(
            MODEL,
            QUERY,
            List.of("abc"),
            null,
            null,
            new MixedbreadRerankTaskSettings(8, Boolean.FALSE)
        );

        assertThat(getXContentResult(entity), equalToIgnoringWhitespaceInJsonString("""
            {
                "model": "model",
                "query": "query",
                "input": [
                    "abc"
                ],
                "top_k": 8,
                "return_input": false
            }
            """));
    }

    public void testXContent_SingleRequest_UsesTaskSettingsReturnDocumentsIfRootIsNotDefined() throws IOException {
        var entity = new MixedbreadRerankRequestEntity(
            MODEL,
            QUERY,
            List.of("abc"),
            null,
            null,
            new MixedbreadRerankTaskSettings(8, Boolean.TRUE)
        );

        assertThat(getXContentResult(entity), equalToIgnoringWhitespaceInJsonString("""
            {
                "model": "model",
                "query": "query",
                "input": [
                    "abc"
                ],
                "top_k": 8,
                "return_input": true
            }
            """));
    }

    private String getXContentResult(MixedbreadRerankRequestEntity entity) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        return Strings.toString(builder);
    }
}
