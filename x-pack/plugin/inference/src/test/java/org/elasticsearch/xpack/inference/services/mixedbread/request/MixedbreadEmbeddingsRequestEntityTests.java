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
import org.elasticsearch.xpack.inference.services.mixedbread.TestUtils;
import org.elasticsearch.xpack.inference.services.mixedbread.request.embeddings.MixedbreadEmbeddingsRequestEntity;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;

public class MixedbreadEmbeddingsRequestEntityTests extends ESTestCase {
    public void testXContent_SingleRequest_WritesAllFieldsIfDefined() throws IOException {
        var entity = new MixedbreadEmbeddingsRequestEntity(
            List.of("abc"),
            TestUtils.MODEL_ID,
            TestUtils.DIMENSIONS,
            TestUtils.PROMPT_INITIAL_VALUE,
            TestUtils.NORMALIZED,
            TestUtils.ENCODING_VALUE
        );

        assertThat(getXContentResult(entity), equalToIgnoringWhitespaceInJsonString("""
            {
                "input": ["abc"],
                "model": "model_id_value",
                "dimensions": 3,
                "prompt": "prompt_initial_value",
                "normalized": false,
                "encoding_format": "float"
            }
            """));
    }

    public void testXContent_SingleRequest_WritesMinimalFields() throws IOException {
        var entity = new MixedbreadEmbeddingsRequestEntity(List.of("abc"), TestUtils.MODEL_ID, null, null, null, null);

        assertThat(getXContentResult(entity), equalToIgnoringWhitespaceInJsonString("""
            {
                "input": ["abc"],
                "model": "model_id_value"
            }
            """));
    }

    public void testXContent_MultipleRequests_WritesAllFieldsIfDefined() throws IOException {
        var entity = new MixedbreadEmbeddingsRequestEntity(List.of("abc", "def"), TestUtils.MODEL_ID, null, null, null, null);

        assertThat(getXContentResult(entity), equalToIgnoringWhitespaceInJsonString("""
            {
                "input": ["abc", "def"],
                "model": "model_id_value"
            }
            """));
    }

    public void testXContent_MultipleRequests_WritesMinimalFields() throws IOException {
        var entity = new MixedbreadEmbeddingsRequestEntity(List.of("abc", "def"), TestUtils.MODEL_ID, null, null, null, null);

        assertThat(getXContentResult(entity), equalToIgnoringWhitespaceInJsonString("""
            {
                "input": ["abc", "def"],
                "model": "model_id_value"
            }
            """));
    }

    private String getXContentResult(MixedbreadEmbeddingsRequestEntity entity) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        return Strings.toString(builder);
    }
}
