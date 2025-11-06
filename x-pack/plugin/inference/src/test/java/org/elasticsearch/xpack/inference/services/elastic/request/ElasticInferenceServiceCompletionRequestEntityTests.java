/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.request;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;

public class ElasticInferenceServiceCompletionRequestEntityTests extends ESTestCase {

    public void testToXContent_SingleInput() throws IOException {
        var entity = new ElasticInferenceServiceCompletionRequestEntity(List.of("What is 2+2?"), "my-model-id");
        String xContentString = xContentEntityToString(entity);
        assertThat(xContentString, equalToIgnoringWhitespaceInJsonString("""
            {
                "model": "my-model-id",
                "messages": [
                    {
                        "role": "user",
                        "content": "What is 2+2?"
                    }
                ],
                "stream": false,
                "n": 1
            }"""));
    }

    public void testToXContent_MultipleInputs() throws IOException {
        var entity = new ElasticInferenceServiceCompletionRequestEntity(
            List.of("What is 2+2?", "What is the capital of France?"),
            "my-model-id"
        );
        String xContentString = xContentEntityToString(entity);
        assertThat(xContentString, equalToIgnoringWhitespaceInJsonString("""
            {
                "model": "my-model-id",
                "messages": [
                    {
                        "role": "user",
                        "content": "What is 2+2?"
                    },
                    {
                        "role": "user",
                        "content": "What is the capital of France?"
                    }
                ],
                "stream": false,
                "n": 1
            }
            """));
    }

    public void testToXContent_EmptyInput() throws IOException {
        var entity = new ElasticInferenceServiceCompletionRequestEntity(List.of(""), "my-model-id");
        String xContentString = xContentEntityToString(entity);
        assertThat(xContentString, equalToIgnoringWhitespaceInJsonString("""
            {
                "model": "my-model-id",
                "messages": [
                    {
                        "role": "user",
                        "content": ""
                    }
                ],
                "stream": false,
                "n": 1
            }
            """));
    }

    public void testToXContent_AlwaysNonStreaming() throws IOException {
        var entity = new ElasticInferenceServiceCompletionRequestEntity(List.of("test input"), "my-model-id");
        String xContentString = xContentEntityToString(entity);

        // Verify stream is always false
        assertThat(xContentString, equalToIgnoringWhitespaceInJsonString("""
            {
                "model": "my-model-id",
                "messages": [
                    {
                        "role": "user",
                        "content": "test input"
                    }
                ],
                "stream": false,
                "n": 1
            }
            """));
    }

    public void testToXContent_AlwaysSetsNToOne() throws IOException {
        var entity = new ElasticInferenceServiceCompletionRequestEntity(List.of("input1", "input2", "input3"), "my-model-id");
        String xContentString = xContentEntityToString(entity);

        // Verify n is always 1 regardless of number of inputs
        assertThat(xContentString, equalToIgnoringWhitespaceInJsonString("""
            {
                "model": "my-model-id",
                "messages": [
                    {
                        "role": "user",
                        "content": "input1"
                    },
                    {
                        "role": "user",
                        "content": "input2"
                    },
                    {
                        "role": "user",
                        "content": "input3"
                    }
                ],
                "stream": false,
                "n": 1
            }
            """));
    }

    public void testToXContent_AllMessagesHaveUserRole() throws IOException {
        var entity = new ElasticInferenceServiceCompletionRequestEntity(List.of("first", "second", "third"), "test-model");
        String xContentString = xContentEntityToString(entity);

        // Verify all messages have "user" role
        assertThat(xContentString, equalToIgnoringWhitespaceInJsonString("""
            {
                "model": "test-model",
                "messages": [
                    {
                        "role": "user",
                        "content": "first"
                    },
                    {
                        "role": "user",
                        "content": "second"
                    },
                    {
                        "role": "user",
                        "content": "third"
                    }
                ],
                "stream": false,
                "n": 1
            }
            """));
    }

    private String xContentEntityToString(ElasticInferenceServiceCompletionRequestEntity entity) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        return Strings.toString(builder);
    }
}
