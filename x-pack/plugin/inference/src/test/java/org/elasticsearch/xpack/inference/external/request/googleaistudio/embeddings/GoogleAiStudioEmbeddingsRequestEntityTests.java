/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.googleaistudio.embeddings;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.request.googleaistudio.GoogleAiStudioEmbeddingsRequestEntity;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;

public class GoogleAiStudioEmbeddingsRequestEntityTests extends ESTestCase {

    public void testXContent_SingleRequest_WritesDimensionsIfDefined() throws IOException {
        var entity = new GoogleAiStudioEmbeddingsRequestEntity(List.of("abc"), "model", 8);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "requests": [
                    {
                        "model": "models/model",
                        "content": {
                            "parts": [
                                {
                                    "text": "abc"
                                }
                            ]
                        },
                        "outputDimensionality": 8
                    }
                ]
            }
            """));
    }

    public void testXContent_SingleRequest_DoesNotWriteDimensionsIfNull() throws IOException {
        var entity = new GoogleAiStudioEmbeddingsRequestEntity(List.of("abc"), "model", null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "requests": [
                    {
                        "model": "models/model",
                        "content": {
                            "parts": [
                                {
                                    "text": "abc"
                                }
                            ]
                        }
                    }
                ]
            }
            """));
    }

    public void testXContent_MultipleRequests_WritesDimensionsIfDefined() throws IOException {
        var entity = new GoogleAiStudioEmbeddingsRequestEntity(List.of("abc", "def"), "model", 8);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "requests": [
                    {
                        "model": "models/model",
                        "content": {
                            "parts": [
                                {
                                    "text": "abc"
                                }
                            ]
                        },
                        "outputDimensionality": 8
                    },
                    {
                        "model": "models/model",
                        "content": {
                            "parts": [
                                {
                                    "text": "def"
                                }
                            ]
                        },
                        "outputDimensionality": 8
                    }
                ]
            }
            """));
    }

    public void testXContent_MultipleRequests_DoesNotWriteDimensionsIfNull() throws IOException {
        var entity = new GoogleAiStudioEmbeddingsRequestEntity(List.of("abc", "def"), "model", null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, equalToIgnoringWhitespaceInJsonString("""
            {
                "requests": [
                    {
                        "model": "models/model",
                        "content": {
                            "parts": [
                                {
                                    "text": "abc"
                                }
                            ]
                        }
                    },
                    {
                        "model": "models/model",
                        "content": {
                            "parts": [
                                {
                                    "text": "def"
                                }
                            ]
                        }
                    }
                ]
            }
            """));
    }
}
