/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.googlevertexai;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;

public class GoogleVertexAiEmbeddingsRequestEntityTests extends ESTestCase {

    public void testToXContent_SingleEmbeddingRequest_WritesAutoTruncationIfDefined() throws IOException {
        var entity = new GoogleVertexAiEmbeddingsRequestEntity(List.of("abc"), true);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "instances": [
                    {
                        "content": "abc"
                    }
                ],
                "parameters": {
                    "autoTruncate": true
                }
            }
            """));
    }

    public void testToXContent_SingleEmbeddingRequest_DoesNotWriteAutoTruncationIfNotDefined() throws IOException {
        var entity = new GoogleVertexAiEmbeddingsRequestEntity(List.of("abc"), null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "instances": [
                    {
                        "content": "abc"
                    }
                ]
            }
            """));
    }

    public void testToXContent_MultipleEmbeddingsRequest_WritesAutoTruncationIfDefined() throws IOException {
        var entity = new GoogleVertexAiEmbeddingsRequestEntity(List.of("abc", "def"), true);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "instances": [
                    {
                        "content": "abc"
                    },
                    {
                        "content": "def"
                    }
                ],
                "parameters": {
                    "autoTruncate": true
                }
            }
            """));
    }

    public void testToXContent_MultipleEmbeddingsRequest_DoesNotWriteAutoTruncationIfNotDefined() throws IOException {
        var entity = new GoogleVertexAiEmbeddingsRequestEntity(List.of("abc", "def"), null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "instances": [
                    {
                        "content": "abc"
                    },
                    {
                        "content": "def"
                    }
                ]
            }
            """));
    }
}
