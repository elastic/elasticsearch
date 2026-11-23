/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.elastic;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.DataFormat;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.elastic.documentextraction.ElasticInferenceServiceDocumentExtractionTaskSettings;
import org.elasticsearch.xpack.inference.services.elastic.request.ElasticInferenceServiceDocumentExtractionRequestEntity;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.elasticsearch.xpack.inference.MatchersUtils.equalToIgnoringWhitespaceInJsonString;
import static org.hamcrest.Matchers.is;

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
            modelId,
            ElasticInferenceServiceDocumentExtractionTaskSettings.EMPTY_SETTINGS
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
            modelId,
            ElasticInferenceServiceDocumentExtractionTaskSettings.EMPTY_SETTINGS
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

    public void testToXContent_HtmlUrlDocument() throws IOException {
        var htmlUrl = "https://example.com/" + randomAlphanumericOfLength(8) + ".html";
        var entity = new ElasticInferenceServiceDocumentExtractionRequestEntity(
            List.of(new InferenceString(DataType.HTML, DataFormat.URL, htmlUrl)),
            modelId,
            ElasticInferenceServiceDocumentExtractionTaskSettings.EMPTY_SETTINGS
        );
        String xContentString = xContentEntityToString(entity);
        assertThat(xContentString, equalToIgnoringWhitespaceInJsonString(Strings.format("""
            {
                "model": "%s",
                "input": [{"content": {"type":"html","format":"url","value":"%s"}}]
            }""", modelId, htmlUrl)));
    }

    public void testToXContent_WithTaskSettings() throws IOException {
        var entity = new ElasticInferenceServiceDocumentExtractionRequestEntity(
            List.of(new InferenceString(DataType.PDF, DataFormat.BASE64, pdfDocValue1)),
            modelId,
            // Sorted, so the parameters are written in a known order
            new ElasticInferenceServiceDocumentExtractionTaskSettings("markdown", new TreeMap<>(Map.of("preset", "ocr", "page", 2)))
        );
        String xContentString = xContentEntityToString(entity);
        assertThat(xContentString, equalToIgnoringWhitespaceInJsonString(Strings.format("""
            {
                "model": "%s",
                "input": [{"content": {"type":"pdf","format":"base64","value":"%s"}}],
                "output_format": "markdown",
                "parameters": {"page": 2, "preset": "ocr"}
            }""", modelId, pdfDocValue1)));
    }

    public void testToXContent_WithEmbeddedParameterObjects() throws IOException {
        // Embedded objects and arrays are inlined into the request body as they are.
        // The parameters are made-up values only demonstrating the pass-through; they are not valid parameters.
        var parameters = Map.<String, Object>of(
            "preset",
            "ocr",
            "ocr",
            Map.of("preferEndToEnd", true, "layout", Map.of("columns", 2, "regions", List.of("header", "body"))),
            "targetSelector",
            List.of("main", Map.of("css", "#post", "weight", 1.5))
        );
        var entity = new ElasticInferenceServiceDocumentExtractionRequestEntity(
            List.of(new InferenceString(DataType.PDF, DataFormat.BASE64, pdfDocValue1)),
            modelId,
            new ElasticInferenceServiceDocumentExtractionTaskSettings(null, parameters)
        );

        // Compared as a map, as the order of the keys of the embedded objects is not defined
        var body = XContentHelper.convertToMap(XContentType.JSON.xContent(), xContentEntityToString(entity), false);

        assertThat(body.get("parameters"), is(parameters));
        assertThat(body.keySet(), is(Set.of("model", "input", "parameters")));
    }

    private String xContentEntityToString(ElasticInferenceServiceDocumentExtractionRequestEntity entity) throws IOException {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        return Strings.toString(builder);
    }
}
