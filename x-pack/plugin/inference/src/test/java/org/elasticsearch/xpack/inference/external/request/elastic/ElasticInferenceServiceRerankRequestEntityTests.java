/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.elastic;

import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.DataFormat;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.InferenceStringTests;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.elastic.request.ElasticInferenceServiceRerankRequestEntity;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.inference.InferenceString.ofText;
import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;

public class ElasticInferenceServiceRerankRequestEntityTests extends ESTestCase {

    public void testToXContent_SingleDocument_NoTopN() throws IOException {
        var entity = new ElasticInferenceServiceRerankRequestEntity(
            ofText("query"),
            List.of(ofText("document 1")),
            "rerank-model-id",
            null
        );
        String xContentString = xContentEntityToString(entity);
        assertThat(xContentString, equalToIgnoringWhitespaceInJsonString("""
            {
                "query": {"type":"text","format":"text","value":"query"},
                "model": "rerank-model-id",
                "documents": [{"type":"text","format":"text","value":"document 1"}]
            }"""));
    }

    public void testToXContent_MultipleDocuments_NoTopN() throws IOException {
        var entity = new ElasticInferenceServiceRerankRequestEntity(
            ofText("query"),
            InferenceString.fromStringList(List.of("document 1", "document 2", "document 3")),
            "rerank-model-id",
            null
        );
        String xContentString = xContentEntityToString(entity);
        assertThat(xContentString, equalToIgnoringWhitespaceInJsonString("""
            {
                "query": {"type":"text","format":"text","value":"query"},
                "model": "rerank-model-id",
                "documents": [
                    {"type":"text","format":"text","value":"document 1"},
                    {"type":"text","format":"text","value":"document 2"},
                    {"type":"text","format":"text","value":"document 3"}
                ]
            }
            """));
    }

    public void testToXContent_SingleDocument_WithTopN() throws IOException {
        var entity = new ElasticInferenceServiceRerankRequestEntity(ofText("query"), List.of(ofText("document 1")), "rerank-model-id", 3);
        String xContentString = xContentEntityToString(entity);
        assertThat(xContentString, equalToIgnoringWhitespaceInJsonString("""
            {
                "query": {"type":"text","format":"text","value":"query"},
                "model": "rerank-model-id",
                "top_n": 3,
                "documents": [{"type":"text","format":"text","value":"document 1"}]
            }
            """));
    }

    public void testToXContent_MultipleDocuments_WithTopN() throws IOException {
        var entity = new ElasticInferenceServiceRerankRequestEntity(
            ofText("query"),
            InferenceString.fromStringList(List.of("document 1", "document 2", "document 3", "document 4", "document 5")),
            "rerank-model-id",
            3
        );
        String xContentString = xContentEntityToString(entity);
        assertThat(xContentString, equalToIgnoringWhitespaceInJsonString("""
            {
                "query": {"type":"text","format":"text","value":"query"},
                "model": "rerank-model-id",
                "top_n": 3,
                "documents": [
                    {"type":"text","format":"text","value":"document 1"},
                    {"type":"text","format":"text","value":"document 2"},
                    {"type":"text","format":"text","value":"document 3"},
                    {"type":"text","format":"text","value":"document 4"},
                    {"type":"text","format":"text","value":"document 5"}
                ]
            }
            """));
    }

    public void testToXContent_Multimodal() throws IOException {
        var queryValue = InferenceStringTests.randomDataURI();
        var firstDocValue = "document 1";
        var secondDocValue = InferenceStringTests.randomDataURI();
        var thirdDocValue = "document 3";
        var documents = List.of(
            ofText(firstDocValue),
            new InferenceString(DataType.IMAGE, DataFormat.BASE64, secondDocValue),
            ofText(thirdDocValue)
        );
        var entity = new ElasticInferenceServiceRerankRequestEntity(
            new InferenceString(DataType.IMAGE, DataFormat.BASE64, queryValue),
            documents,
            "rerank-model-id",
            3
        );
        String xContentString = xContentEntityToString(entity);
        assertThat(xContentString, equalToIgnoringWhitespaceInJsonString(Strings.format("""
            {
                "query": {"type":"image","format":"base64","value":"%s"},
                "model": "rerank-model-id",
                "top_n": 3,
                "documents": [
                    {"type":"text","format":"text","value":"%s"},
                    {"type":"image","format":"base64","value":"%s"},
                    {"type":"text","format":"text","value":"%s"}
                ]
            }
            """, queryValue, firstDocValue, secondDocValue, thirdDocValue)));
    }

    public void testNullQueryThrowsException() {
        NullPointerException e = expectThrows(
            NullPointerException.class,
            () -> new ElasticInferenceServiceRerankRequestEntity(null, List.of(ofText("document 1")), "model-id", null)
        );
        assertNotNull(e);
    }

    public void testNullDocumentsThrowsException() {
        NullPointerException e = expectThrows(
            NullPointerException.class,
            () -> new ElasticInferenceServiceRerankRequestEntity(ofText("query"), null, "model-id", null)
        );
        assertNotNull(e);
    }

    public void testNullModelIdThrowsException() {
        NullPointerException e = expectThrows(
            NullPointerException.class,
            () -> new ElasticInferenceServiceRerankRequestEntity(ofText("query"), List.of(ofText("document 1")), null, null)
        );
        assertNotNull(e);
    }

    private String xContentEntityToString(ElasticInferenceServiceRerankRequestEntity entity) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        return Strings.toString(builder);
    }
}
