/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.transforms.common;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.transform.transforms.common.DocumentConversionUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class DocumentConversionUtilsTests extends ESTestCase {

    private static String INDEX = "some-index";
    private static String PIPELINE = "some-pipeline";
    private static String ID = "some-id";
    private static Map<String, Object> DOCUMENT =
        new HashMap<>() {{
            put("_id", ID);
            put("field-1", "field-1-value");
            put("field-2", "field-2-value");
            put("field-3", "field-3-value");
            put("_internal-field-1", "internal-field-1-value");
            put("_internal-field-2", "internal-field-2-value");
        }};
    private static Map<String, Object> DOCUMENT_WITHOUT_INTERNAL_FIELDS =
        new HashMap<>() {{
            put("field-1", "field-1-value");
            put("field-2", "field-2-value");
            put("field-3", "field-3-value");
        }};

    public void testConvertDocumentToIndexRequest_MissingId() {
        Exception e =
            expectThrows(
                Exception.class,
                () -> DocumentConversionUtils.convertDocumentToIndexRequest(Collections.emptyMap(), INDEX, PIPELINE));
        assertThat(e.getMessage(), is(equalTo("Expected a document id but got null.")));
    }

    public void testConvertDocumentToIndexRequest() {
        IndexRequest indexRequest = DocumentConversionUtils.convertDocumentToIndexRequest(DOCUMENT, INDEX, PIPELINE);
        assertThat(indexRequest.index(), is(equalTo(INDEX)));
        assertThat(indexRequest.id(), is(equalTo(ID)));
        assertThat(indexRequest.getPipeline(), is(equalTo(PIPELINE)));
        assertThat(indexRequest.sourceAsMap(), is(equalTo(DOCUMENT_WITHOUT_INTERNAL_FIELDS)));
    }

    public void testConvertDocumentToIndexRequest_WithNullIndex() {
        IndexRequest indexRequest = DocumentConversionUtils.convertDocumentToIndexRequest(DOCUMENT, null, PIPELINE);
        assertThat(indexRequest.index(), is(nullValue()));
        assertThat(indexRequest.id(), is(equalTo(ID)));
        assertThat(indexRequest.getPipeline(), is(equalTo(PIPELINE)));
        assertThat(indexRequest.sourceAsMap(), is(equalTo(DOCUMENT_WITHOUT_INTERNAL_FIELDS)));
    }

    public void testConvertDocumentToIndexRequest_WithNullPipeline() {
        IndexRequest indexRequest = DocumentConversionUtils.convertDocumentToIndexRequest(DOCUMENT, INDEX, null);
        assertThat(indexRequest.index(), is(equalTo(INDEX)));
        assertThat(indexRequest.id(), is(equalTo(ID)));
        assertThat(indexRequest.getPipeline(), is(nullValue()));
        assertThat(indexRequest.sourceAsMap(), is(equalTo(DOCUMENT_WITHOUT_INTERNAL_FIELDS)));
    }
}
