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
import org.junit.Before;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.inference.InferenceString.ofText;
import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;

public class ElasticInferenceServiceRerankRequestEntityTests extends ESTestCase {

    private String textQueryValue;
    private String imageQueryValue;
    private String modelId;
    private String textDocValue1;
    private String textDocValue2;
    private String textDocValue3;
    private int topNValue;
    private String imageDocValue;

    @Before
    public void init() {
        textQueryValue = randomAlphanumericOfLength(8);
        imageQueryValue = InferenceStringTests.randomDataURI();
        modelId = randomAlphanumericOfLength(8);
        textDocValue1 = randomAlphanumericOfLength(8);
        textDocValue2 = randomAlphanumericOfLength(8);
        textDocValue3 = randomAlphanumericOfLength(8);
        imageDocValue = InferenceStringTests.randomDataURI();
        topNValue = randomIntBetween(1, 128);
    }

    public void testToXContent_SingleDocument_NoTopN() throws IOException {
        var entity = new ElasticInferenceServiceRerankRequestEntity(ofText(textQueryValue), List.of(ofText(textDocValue1)), modelId, null);
        String xContentString = xContentEntityToString(entity);
        assertThat(xContentString, equalToIgnoringWhitespaceInJsonString(Strings.format("""
            {
                "query": {"type":"text","format":"text","value":"%s"},
                "model": "%s",
                "documents": [{"type":"text","format":"text","value":"%s"}]
            }""", textQueryValue, modelId, textDocValue1)));
    }

    public void testToXContent_MultipleDocuments_NoTopN() throws IOException {
        var entity = new ElasticInferenceServiceRerankRequestEntity(
            ofText(textQueryValue),
            InferenceString.fromStringList(List.of(textDocValue1, textDocValue2, textDocValue3)),
            modelId,
            null
        );
        String xContentString = xContentEntityToString(entity);
        assertThat(xContentString, equalToIgnoringWhitespaceInJsonString(Strings.format("""
            {
                "query": {"type":"text","format":"text","value":"%s"},
                "model": "%s",
                "documents": [
                    {"type":"text","format":"text","value":"%s"},
                    {"type":"text","format":"text","value":"%s"},
                    {"type":"text","format":"text","value":"%s"}
                ]
            }
            """, textQueryValue, modelId, textDocValue1, textDocValue2, textDocValue3)));
    }

    public void testToXContent_SingleDocument_WithTopN() throws IOException {
        var entity = new ElasticInferenceServiceRerankRequestEntity(
            ofText(textQueryValue),
            List.of(ofText(textDocValue1)),
            modelId,
            topNValue
        );
        String xContentString = xContentEntityToString(entity);
        assertThat(xContentString, equalToIgnoringWhitespaceInJsonString(Strings.format("""
            {
                "query": {"type":"text","format":"text","value":"%s"},
                "model": "%s",
                "top_n": %d,
                "documents": [{"type":"text","format":"text","value":"%s"}]
            }
            """, textQueryValue, modelId, topNValue, textDocValue1)));
    }

    public void testToXContent_MultipleDocuments_WithTopN() throws IOException {
        var entity = new ElasticInferenceServiceRerankRequestEntity(
            ofText(textQueryValue),
            InferenceString.fromStringList(List.of(textDocValue1, textDocValue2, textDocValue3)),
            modelId,
            topNValue
        );
        String xContentString = xContentEntityToString(entity);
        assertThat(xContentString, equalToIgnoringWhitespaceInJsonString(Strings.format("""
            {
                "query": {"type":"text","format":"text","value":"%s"},
                "model": "%s",
                "top_n": %d,
                "documents": [
                    {"type":"text","format":"text","value":"%s"},
                    {"type":"text","format":"text","value":"%s"},
                    {"type":"text","format":"text","value":"%s"}
                ]
            }
            """, textQueryValue, modelId, topNValue, textDocValue1, textDocValue2, textDocValue3)));
    }

    public void testToXContent_Multimodal() throws IOException {
        var documents = List.of(
            ofText(textDocValue1),
            new InferenceString(DataType.IMAGE, DataFormat.BASE64, imageDocValue),
            ofText(textDocValue3)
        );
        var entity = new ElasticInferenceServiceRerankRequestEntity(
            new InferenceString(DataType.IMAGE, DataFormat.BASE64, imageQueryValue),
            documents,
            modelId,
            topNValue
        );
        String xContentString = xContentEntityToString(entity);
        assertThat(xContentString, equalToIgnoringWhitespaceInJsonString(Strings.format("""
            {
                "query": {"type":"image","format":"base64","value":"%s"},
                "model": "%s",
                "top_n": %d,
                "documents": [
                    {"type":"text","format":"text","value":"%s"},
                    {"type":"image","format":"base64","value":"%s"},
                    {"type":"text","format":"text","value":"%s"}
                ]
            }
            """, imageQueryValue, modelId, topNValue, textDocValue1, imageDocValue, textDocValue3)));
    }

    public void testNullQueryThrowsException() {
        NullPointerException e = expectThrows(
            NullPointerException.class,
            () -> new ElasticInferenceServiceRerankRequestEntity(null, List.of(ofText(textDocValue1)), modelId, null)
        );
        assertNotNull(e);
    }

    public void testNullDocumentsThrowsException() {
        NullPointerException e = expectThrows(
            NullPointerException.class,
            () -> new ElasticInferenceServiceRerankRequestEntity(ofText(textQueryValue), null, modelId, null)
        );
        assertNotNull(e);
    }

    public void testNullModelIdThrowsException() {
        NullPointerException e = expectThrows(
            NullPointerException.class,
            () -> new ElasticInferenceServiceRerankRequestEntity(ofText(textQueryValue), List.of(ofText(textDocValue1)), null, null)
        );
        assertNotNull(e);
    }

    private String xContentEntityToString(ElasticInferenceServiceRerankRequestEntity entity) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        return Strings.toString(builder);
    }
}
