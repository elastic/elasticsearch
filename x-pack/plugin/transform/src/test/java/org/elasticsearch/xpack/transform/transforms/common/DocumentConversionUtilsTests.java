/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms.common;

import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Map.entry;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.oneOf;

public class DocumentConversionUtilsTests extends ESTestCase {

    private static final String INDEX = "some-index";
    private static final String PIPELINE = "some-pipeline";
    private static final String ID = "some-id";
    private static final Map<String, Object> DOCUMENT = Map.ofEntries(
        entry("field-1", "field-1-value"),
        entry("field-2", "field-2-value"),
        entry("field-3", "field-3-value"),
        entry("_internal-field-1", "internal-field-1-value"),
        entry("_internal-field-2", "internal-field-2-value")
    );
    private static final Map<String, Object> DOCUMENT_WITHOUT_ID = Map.ofEntries(
        entry("field-1", "field-1-value"),
        entry("field-2", "field-2-value"),
        entry("field-3", "field-3-value"),
        entry("_internal-field-1", "internal-field-1-value"),
        entry("_internal-field-2", "internal-field-2-value")
    );
    private static final Map<String, Object> DOCUMENT_WITHOUT_INTERNAL_FIELDS = Map.ofEntries(
        entry("field-1", "field-1-value"),
        entry("field-2", "field-2-value"),
        entry("field-3", "field-3-value")
    );

    public void testConvertDocumentToIndexRequest_MissingId() {
        Exception e = expectThrows(
            Exception.class,
            () -> DocumentConversionUtils.convertDocumentToIndexRequest(null, Collections.emptyMap(), INDEX, PIPELINE)
        );
        assertThat(e.getMessage(), is(equalTo("Expected a document id but got null.")));
    }

    public void testConvertDocumentToIndexRequest() {
        IndexRequest indexRequest = DocumentConversionUtils.convertDocumentToIndexRequest(ID, DOCUMENT, INDEX, PIPELINE);
        assertThat(indexRequest.index(), is(equalTo(INDEX)));
        assertThat(indexRequest.id(), is(equalTo(ID)));
        assertThat(indexRequest.getPipeline(), is(equalTo(PIPELINE)));
        assertThat(indexRequest.sourceAsMap(), is(equalTo(DOCUMENT_WITHOUT_ID)));
    }

    public void testConvertDocumentToIndexRequest_WithNullIndex() {
        IndexRequest indexRequest = DocumentConversionUtils.convertDocumentToIndexRequest(ID, DOCUMENT, null, PIPELINE);
        assertThat(indexRequest.index(), is(nullValue()));
        assertThat(indexRequest.id(), is(equalTo(ID)));
        assertThat(indexRequest.getPipeline(), is(equalTo(PIPELINE)));
        assertThat(indexRequest.sourceAsMap(), is(equalTo(DOCUMENT_WITHOUT_ID)));
    }

    public void testConvertDocumentToIndexRequest_WithNullPipeline() {
        IndexRequest indexRequest = DocumentConversionUtils.convertDocumentToIndexRequest(ID, DOCUMENT, INDEX, null);
        assertThat(indexRequest.index(), is(equalTo(INDEX)));
        assertThat(indexRequest.id(), is(equalTo(ID)));
        assertThat(indexRequest.getPipeline(), is(nullValue()));
        assertThat(indexRequest.sourceAsMap(), is(equalTo(DOCUMENT_WITHOUT_ID)));
    }

    public void testRemoveInternalFields() {
        assertThat(DocumentConversionUtils.removeInternalFields(DOCUMENT), is(equalTo(DOCUMENT_WITHOUT_INTERNAL_FIELDS)));
    }

    public void testExtractFieldMappings() {
        FieldCapabilitiesResponse response = new FieldCapabilitiesResponse(new String[] { "some-index" }, new HashMap<>() {
            {
                put("field-1", new HashMap<>() {
                    {
                        put("keyword", createFieldCapabilities("field-1", "keyword"));
                    }
                });
                put("field-2", new HashMap<>() {
                    {
                        put("long", createFieldCapabilities("field-2", "long"));
                        put("keyword", createFieldCapabilities("field-2", "keyword"));
                    }
                });
            }
        });

        assertThat(
            DocumentConversionUtils.extractFieldMappings(response),
            allOf(hasEntry("field-1", "keyword"), hasEntry(is(equalTo("field-2")), is(oneOf("long", "keyword"))))
        );
    }

    private static FieldCapabilities createFieldCapabilities(String name, String type) {
        return new FieldCapabilities(
            name,
            type,
            false,
            true,
            true,
            Strings.EMPTY_ARRAY,
            Strings.EMPTY_ARRAY,
            Strings.EMPTY_ARRAY,
            Collections.emptyMap()
        );
    }
}
