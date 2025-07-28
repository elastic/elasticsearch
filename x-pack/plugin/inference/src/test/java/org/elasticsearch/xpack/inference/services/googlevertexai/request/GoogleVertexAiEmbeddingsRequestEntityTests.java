/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.request;

import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.googlevertexai.embeddings.GoogleVertexAiEmbeddingsTaskSettings;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;

public class GoogleVertexAiEmbeddingsRequestEntityTests extends ESTestCase {

    public void testToXContent_SingleEmbeddingRequest_WritesAllFields() throws IOException {
        var entity = new GoogleVertexAiEmbeddingsRequestEntity(
            List.of("abc"),
            null,
            new GoogleVertexAiEmbeddingsTaskSettings(true, InputType.CLUSTERING)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "instances": [
                    {
                        "content": "abc",
                        "task_type": "CLUSTERING"
                    }
                ],
                "parameters": {
                    "autoTruncate": true
                }
            }
            """));
    }

    public void testToXContent_SingleEmbeddingRequest_DoesNotWriteAutoTruncationIfNotDefined() throws IOException {
        var entity = new GoogleVertexAiEmbeddingsRequestEntity(
            List.of("abc"),
            InputType.INTERNAL_INGEST,
            new GoogleVertexAiEmbeddingsTaskSettings(null, null)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "instances": [
                    {
                        "content": "abc",
                        "task_type": "RETRIEVAL_DOCUMENT"
                    }
                ]
            }
            """));
    }

    public void testToXContent_SingleEmbeddingRequest_DoesNotWriteInputTypeIfNotDefined() throws IOException {
        var entity = new GoogleVertexAiEmbeddingsRequestEntity(List.of("abc"), null, new GoogleVertexAiEmbeddingsTaskSettings(false, null));

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
                    "autoTruncate": false
                }
            }
            """));
    }

    public void testToXContent_MultipleEmbeddingsRequest_WritesAllFields() throws IOException {
        var entity = new GoogleVertexAiEmbeddingsRequestEntity(
            List.of("abc", "def"),
            InputType.INTERNAL_SEARCH,
            new GoogleVertexAiEmbeddingsTaskSettings(true, InputType.CLUSTERING)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "instances": [
                    {
                        "content": "abc",
                        "task_type": "RETRIEVAL_QUERY"
                    },
                    {
                        "content": "def",
                        "task_type": "RETRIEVAL_QUERY"
                    }
                ],
                "parameters": {
                    "autoTruncate": true
                }
            }
            """));
    }

    public void testToXContent_MultipleEmbeddingsRequest_DoesNotWriteInputTypeIfNotDefined() throws IOException {
        var entity = new GoogleVertexAiEmbeddingsRequestEntity(
            List.of("abc", "def"),
            null,
            new GoogleVertexAiEmbeddingsTaskSettings(true, null)
        );

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
        var entity = new GoogleVertexAiEmbeddingsRequestEntity(
            List.of("abc", "def"),
            null,
            new GoogleVertexAiEmbeddingsTaskSettings(null, InputType.CLASSIFICATION)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "instances": [
                    {
                        "content": "abc",
                        "task_type": "CLASSIFICATION"
                    },
                    {
                        "content": "def",
                        "task_type": "CLASSIFICATION"
                    }
                ]
            }
            """));
    }

    public void testToXContent_ThrowsIfTaskSettingsIsNull() {
        expectThrows(NullPointerException.class, () -> new GoogleVertexAiEmbeddingsRequestEntity(List.of("abc", "def"), null, null));
    }
}
