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
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceUsageContext;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;

public class ElasticInferenceServiceDenseTextEmbeddingsRequestEntityTests extends ESTestCase {

    public void testToXContent_SingleInput_UnspecifiedUsageContext() throws IOException {
        var entity = new ElasticInferenceServiceDenseTextEmbeddingsRequestEntity(
            List.of("abc"),
            "my-model-id",
            ElasticInferenceServiceUsageContext.UNSPECIFIED
        );
        String xContentString = xContentEntityToString(entity);
        assertThat(xContentString, equalToIgnoringWhitespaceInJsonString("""
            {
                "input": ["abc"],
                "model": "my-model-id"
            }"""));
    }

    public void testToXContent_MultipleInputs_UnspecifiedUsageContext() throws IOException {
        var entity = new ElasticInferenceServiceDenseTextEmbeddingsRequestEntity(
            List.of("abc", "def"),
            "my-model-id",
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

    public void testToXContent_SingleInput_SearchUsageContext() throws IOException {
        var entity = new ElasticInferenceServiceDenseTextEmbeddingsRequestEntity(
            List.of("abc"),
            "my-model-id",
            ElasticInferenceServiceUsageContext.SEARCH
        );
        String xContentString = xContentEntityToString(entity);
        assertThat(xContentString, equalToIgnoringWhitespaceInJsonString("""
            {
                "input": ["abc"],
                "model": "my-model-id",
                "usage_context": "search"
            }
            """));
    }

    public void testToXContent_SingleInput_IngestUsageContext() throws IOException {
        var entity = new ElasticInferenceServiceDenseTextEmbeddingsRequestEntity(
            List.of("abc"),
            "my-model-id",
            ElasticInferenceServiceUsageContext.INGEST
        );
        String xContentString = xContentEntityToString(entity);
        assertThat(xContentString, equalToIgnoringWhitespaceInJsonString("""
            {
                "input": ["abc"],
                "model": "my-model-id",
                "usage_context": "ingest"
            }
            """));
    }

    public void testToXContent_MultipleInputs_SearchUsageContext() throws IOException {
        var entity = new ElasticInferenceServiceDenseTextEmbeddingsRequestEntity(
            List.of("first input", "second input", "third input"),
            "my-dense-model",
            ElasticInferenceServiceUsageContext.SEARCH
        );
        String xContentString = xContentEntityToString(entity);
        assertThat(xContentString, equalToIgnoringWhitespaceInJsonString("""
            {
                "input": [
                    "first input",
                    "second input",
                    "third input"
                ],
                "model": "my-dense-model",
                "usage_context": "search"
            }
            """));
    }

    public void testToXContent_MultipleInputs_IngestUsageContext() throws IOException {
        var entity = new ElasticInferenceServiceDenseTextEmbeddingsRequestEntity(
            List.of("document one", "document two"),
            "embedding-model-v2",
            ElasticInferenceServiceUsageContext.INGEST
        );
        String xContentString = xContentEntityToString(entity);
        assertThat(xContentString, equalToIgnoringWhitespaceInJsonString("""
            {
                "input": [
                    "document one",
                    "document two"
                ],
                "model": "embedding-model-v2",
                "usage_context": "ingest"
            }
            """));
    }

    public void testToXContent_EmptyInput_UnspecifiedUsageContext() throws IOException {
        var entity = new ElasticInferenceServiceDenseTextEmbeddingsRequestEntity(
            List.of(""),
            "my-model-id",
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

    private String xContentEntityToString(ElasticInferenceServiceDenseTextEmbeddingsRequestEntity entity) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        return Strings.toString(builder);
    }
}
