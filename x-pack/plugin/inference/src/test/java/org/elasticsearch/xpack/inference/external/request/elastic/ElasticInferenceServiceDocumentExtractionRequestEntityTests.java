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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.elastic.request.ElasticInferenceServiceDocumentExtractionRequestEntity;
import org.junit.Before;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;

public class ElasticInferenceServiceDocumentExtractionRequestEntityTests extends ESTestCase {

    private String modelId;
    private String pdfDocValue1;
    private String pdfDocValue2;
    private String imageDocValue;

    @Before
    public void init() {
        modelId = randomAlphanumericOfLength(8);
        pdfDocValue1 = "data:application/pdf;base64," + randomAlphanumericOfLength(16);
        pdfDocValue2 = "data:application/pdf;base64," + randomAlphanumericOfLength(16);
        imageDocValue = "data:image/png;base64," + randomAlphanumericOfLength(16);
    }

    public void testToXContent_SingleDocument() throws IOException {
        var entity = new ElasticInferenceServiceDocumentExtractionRequestEntity(
            List.of(new InferenceString(DataType.PDF, DataFormat.BASE64, pdfDocValue1)),
            modelId
        );
        String xContentString = xContentEntityToString(entity);
        assertThat(xContentString, equalToIgnoringWhitespaceInJsonString(Strings.format("""
            {
                "model": "%s",
                "input": [{"content": {"type":"pdf","format":"base64","value":"%s"}}]
            }""", modelId, pdfDocValue1)));
    }

    public void testToXContent_MultipleDocuments() throws IOException {
        var entity = new ElasticInferenceServiceDocumentExtractionRequestEntity(
            List.of(
                new InferenceString(DataType.PDF, DataFormat.BASE64, pdfDocValue1),
                new InferenceString(DataType.IMAGE, DataFormat.BASE64, imageDocValue),
                new InferenceString(DataType.PDF, DataFormat.BASE64, pdfDocValue2)
            ),
            modelId
        );
        String xContentString = xContentEntityToString(entity);
        assertThat(xContentString, equalToIgnoringWhitespaceInJsonString(Strings.format("""
            {
                "model": "%s",
                "input": [
                    {"content": {"type":"pdf","format":"base64","value":"%s"}},
                    {"content": {"type":"image","format":"base64","value":"%s"}},
                    {"content": {"type":"pdf","format":"base64","value":"%s"}}
                ]
            }
            """, modelId, pdfDocValue1, imageDocValue, pdfDocValue2)));
    }

    private String xContentEntityToString(ElasticInferenceServiceDocumentExtractionRequestEntity entity) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        return Strings.toString(builder);
    }
}
