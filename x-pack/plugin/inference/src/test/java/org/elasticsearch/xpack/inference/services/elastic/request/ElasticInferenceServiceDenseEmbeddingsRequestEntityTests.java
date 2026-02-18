/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.request;

import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.InferenceStringGroup;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceUsageContext;
import org.elasticsearch.xpack.inference.services.elastic.denseembeddings.ElasticInferenceServiceDenseEmbeddingsServiceSettings;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;
import static org.elasticsearch.xpack.inference.services.elastic.denseembeddings.ElasticInferenceServiceDenseEmbeddingsModelTests.createEmbeddingModel;
import static org.elasticsearch.xpack.inference.services.elastic.denseembeddings.ElasticInferenceServiceDenseEmbeddingsModelTests.createTextEmbeddingModel;

public class ElasticInferenceServiceDenseEmbeddingsRequestEntityTests extends ESTestCase {

    public void testToXContent_SingleInput_UnspecifiedUsageContext_TextEmbeddingModel() throws IOException {
        var entity = new ElasticInferenceServiceDenseEmbeddingsRequestEntity(
            List.of(new InferenceStringGroup("abc")),
            createTextEmbeddingModel("", "my-model-id"),
            ElasticInferenceServiceUsageContext.UNSPECIFIED
        );
        String xContentString = xContentEntityToString(entity);
        assertThat(xContentString, equalToIgnoringWhitespaceInJsonString("""
            {
                "input": ["abc"],
                "model": "my-model-id"
            }"""));
    }

    public void testToXContent_SingleInput_UnspecifiedUsageContext_EmbeddingModel() throws IOException {
        var entity = new ElasticInferenceServiceDenseEmbeddingsRequestEntity(
            List.of(new InferenceStringGroup("abc")),
            createEmbeddingModel("", "my-model-id"),
            ElasticInferenceServiceUsageContext.UNSPECIFIED
        );
        String xContentString = xContentEntityToString(entity);
        assertThat(xContentString, equalToIgnoringWhitespaceInJsonString("""
            {
                "input": [{"content":[{"type": "text", "format": "text", "value": "abc"}]}],
                "model": "my-model-id"
            }"""));
    }

    public void testToXContent_MultipleInputs_UnspecifiedUsageContext_TextEmbeddingModel() throws IOException {
        var entity = new ElasticInferenceServiceDenseEmbeddingsRequestEntity(
            List.of(new InferenceStringGroup("abc"), new InferenceStringGroup("def")),
            createTextEmbeddingModel("", "my-model-id"),
            ElasticInferenceServiceUsageContext.UNSPECIFIED
        );
        String xContentString = xContentEntityToString(entity);
        assertThat(xContentString, equalToIgnoringWhitespaceInJsonString("""
            {
                "input": [
                    "abc",
                    "def"
                ],
                "model": "my-model-id"
            }
            """));
    }

    public void testToXContent_MultipleInputs_UnspecifiedUsageContext_EmbeddingModel() throws IOException {
        var entity = new ElasticInferenceServiceDenseEmbeddingsRequestEntity(
            List.of(
                new InferenceStringGroup("abc"),
                new InferenceStringGroup(new InferenceString(InferenceString.DataType.IMAGE, InferenceString.DataFormat.BASE64, "def"))
            ),
            createEmbeddingModel("", "my-model-id"),
            ElasticInferenceServiceUsageContext.UNSPECIFIED
        );
        String xContentString = xContentEntityToString(entity);
        assertThat(xContentString, equalToIgnoringWhitespaceInJsonString("""
            {
                "input": [
                    {"content":[{"type": "text", "format": "text", "value": "abc"}]},
                    {"content":[{"type": "image", "format": "base64", "value": "def"}]}
                ],
                "model": "my-model-id"
            }
            """));
    }

    public void testToXContent_SingleInput_UsageContextSpecified() throws IOException {
        ElasticInferenceServiceUsageContext usageContext = randomFrom(
            ElasticInferenceServiceUsageContext.SEARCH,
            ElasticInferenceServiceUsageContext.INGEST
        );
        var entity = new ElasticInferenceServiceDenseEmbeddingsRequestEntity(
            List.of(new InferenceStringGroup("abc")),
            createTextEmbeddingModel("", "my-model-id"),
            usageContext
        );
        String xContentString = xContentEntityToString(entity);
        assertThat(xContentString, equalToIgnoringWhitespaceInJsonString(Strings.format("""
            {
                "input": ["abc"],
                "model": "my-model-id",
                "usage_context": "%s"
            }
            """, usageContext)));
    }

    public void testToXContent_SingleInput_DimensionsSpecified() throws IOException {
        var serviceSettings = new ElasticInferenceServiceDenseEmbeddingsServiceSettings("my-model-id", null, 100, null);
        var entity = new ElasticInferenceServiceDenseEmbeddingsRequestEntity(
            List.of(new InferenceStringGroup("abc")),
            createTextEmbeddingModel("", serviceSettings, null),
            ElasticInferenceServiceUsageContext.UNSPECIFIED
        );
        String xContentString = xContentEntityToString(entity);
        assertThat(xContentString, equalToIgnoringWhitespaceInJsonString("""
            {
                "input": ["abc"],
                "model": "my-model-id",
                "dimensions": 100
            }
            """));
    }

    public void testToXContent_EmptyInput_TextEmbedding() throws IOException {
        var entity = new ElasticInferenceServiceDenseEmbeddingsRequestEntity(
            List.of(new InferenceStringGroup("")),
            createTextEmbeddingModel("", "my-model-id"),
            ElasticInferenceServiceUsageContext.UNSPECIFIED
        );
        String xContentString = xContentEntityToString(entity);
        assertThat(xContentString, equalToIgnoringWhitespaceInJsonString("""
            {
                "input": [""],
                "model": "my-model-id"
            }
            """));
    }

    public void testToXContent_EmptyInput_Embedding() throws IOException {
        var entity = new ElasticInferenceServiceDenseEmbeddingsRequestEntity(
            List.of(new InferenceStringGroup("")),
            createEmbeddingModel("", "my-model-id"),
            ElasticInferenceServiceUsageContext.UNSPECIFIED
        );
        String xContentString = xContentEntityToString(entity);
        assertThat(xContentString, equalToIgnoringWhitespaceInJsonString("""
            {
                "input": [{"content":[{"type": "text", "format": "text", "value": ""}]}],
                "model": "my-model-id"
            }
            """));
    }

    private String xContentEntityToString(ElasticInferenceServiceDenseEmbeddingsRequestEntity entity) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        return Strings.toString(builder);
    }
}
