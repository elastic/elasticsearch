/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.elastic;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.request.elastic.rerank.ElasticInferenceServiceRerankRequestEntity;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;

public class ElasticInferenceServiceRerankRequestEntityTests extends ESTestCase {

    public void testToXContent_SingleDocument_NoTopN() throws IOException {
        var entity = new ElasticInferenceServiceRerankRequestEntity("query", List.of("document 1"), "rerank-model-id", null);
        String xContentString = xContentEntityToString(entity);
        assertThat(xContentString, equalToIgnoringWhitespaceInJsonString("""
            {
                "query": "query",
                "model": "rerank-model-id",
                "documents": ["document 1"]
            }"""));
    }

    public void testToXContent_MultipleDocuments_NoTopN() throws IOException {
        var entity = new ElasticInferenceServiceRerankRequestEntity(
            "query",
            List.of("document 1", "document 2", "document 3"),
            "rerank-model-id",
            null
        );
        String xContentString = xContentEntityToString(entity);
        assertThat(xContentString, equalToIgnoringWhitespaceInJsonString("""
            {
                "query": "query",
                "model": "rerank-model-id",
                "documents": [
                    "document 1",
                    "document 2",
                    "document 3"
                ]
            }
            """));
    }

    public void testToXContent_SingleDocument_WithTopN() throws IOException {
        var entity = new ElasticInferenceServiceRerankRequestEntity("query", List.of("document 1"), "rerank-model-id", 3);
        String xContentString = xContentEntityToString(entity);
        assertThat(xContentString, equalToIgnoringWhitespaceInJsonString("""
            {
                "query": "query",
                "model": "rerank-model-id",
                "top_n": 3,
                "documents": ["document 1"]
            }
            """));
    }

    public void testToXContent_MultipleDocuments_WithTopN() throws IOException {
        var entity = new ElasticInferenceServiceRerankRequestEntity(
            "query",
            List.of("document 1", "document 2", "document 3", "document 4", "document 5"),
            "rerank-model-id",
            3
        );
        String xContentString = xContentEntityToString(entity);
        assertThat(xContentString, equalToIgnoringWhitespaceInJsonString("""
            {
                "query": "query",
                "model": "rerank-model-id",
                "top_n": 3,
                "documents": [
                    "document 1",
                    "document 2",
                    "document 3",
                    "document 4",
                    "document 5"
                ]
            }
            """));
    }

    public void testNullQueryThrowsException() {
        NullPointerException e = expectThrows(
            NullPointerException.class,
            () -> new ElasticInferenceServiceRerankRequestEntity(null, List.of("document 1"), "model-id", null)
        );
        assertNotNull(e);
    }

    public void testNullDocumentsThrowsException() {
        NullPointerException e = expectThrows(
            NullPointerException.class,
            () -> new ElasticInferenceServiceRerankRequestEntity("query", null, "model-id", null)
        );
        assertNotNull(e);
    }

    public void testNullModelIdThrowsException() {
        NullPointerException e = expectThrows(
            NullPointerException.class,
            () -> new ElasticInferenceServiceRerankRequestEntity("query", List.of("document 1"), null, null)
        );
        assertNotNull(e);
    }

    private String xContentEntityToString(ElasticInferenceServiceRerankRequestEntity entity) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        return Strings.toString(builder);
    }
}
