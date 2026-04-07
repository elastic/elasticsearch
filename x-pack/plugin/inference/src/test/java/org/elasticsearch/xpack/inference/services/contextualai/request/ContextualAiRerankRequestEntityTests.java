/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.contextualai.request;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentHelper.stripWhitespace;
import static org.hamcrest.Matchers.is;

public class ContextualAiRerankRequestEntityTests extends ESTestCase {

    private static final String FIRST_DOCUMENT_VALUE = "some document";
    private static final String SECOND_DOCUMENT_VALUE = "some other document";
    private static final List<String> DOCUMENTS_VALUE = List.of(FIRST_DOCUMENT_VALUE, SECOND_DOCUMENT_VALUE);
    private static final String QUERY_VALUE = "some query";
    private static final String MODEL_VALUE = "some_model";
    private static final int TOP_N_VALUE = 7;
    private static final String INSTRUCTION_VALUE = "Rerank by relevance.";

    public void testXContent_WritesAllFields() throws IOException {
        var entity = new ContextualAiRerankRequestEntity(MODEL_VALUE, QUERY_VALUE, DOCUMENTS_VALUE, TOP_N_VALUE, INSTRUCTION_VALUE);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String result = Strings.toString(builder);
        String expected = Strings.format("""
            {
                "query": "%s",
                "documents": [
                    "%s",
                    "%s"
                ],
                "model": "%s",
                "top_n": %d,
                "instruction": "%s"
            }
            """, QUERY_VALUE, FIRST_DOCUMENT_VALUE, SECOND_DOCUMENT_VALUE, MODEL_VALUE, TOP_N_VALUE, INSTRUCTION_VALUE);
        assertThat(result, is(stripWhitespace(expected)));
    }

    public void testXContent_WritesRequiredFieldsOnly() throws IOException {
        var entity = new ContextualAiRerankRequestEntity(MODEL_VALUE, QUERY_VALUE, DOCUMENTS_VALUE, null, null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String result = Strings.toString(builder);
        String expected = Strings.format("""
            {
                "query": "%s",
                "documents": [
                    "%s",
                    "%s"
                ],
                "model": "%s"
            }
            """, QUERY_VALUE, FIRST_DOCUMENT_VALUE, SECOND_DOCUMENT_VALUE, MODEL_VALUE);
        assertThat(result, is(stripWhitespace(expected)));
    }

    public void testCreateRequestEntity_NoModelId_ThrowsException() {
        expectThrows(NullPointerException.class, () -> new ContextualAiRerankRequestEntity(null, QUERY_VALUE, DOCUMENTS_VALUE, null, null));
    }

    public void testCreateRequestEntity_NoQuery_ThrowsException() {
        expectThrows(NullPointerException.class, () -> new ContextualAiRerankRequestEntity(MODEL_VALUE, null, DOCUMENTS_VALUE, null, null));
    }

    public void testCreateRequestEntity_NoDocuments_ThrowsException() {
        expectThrows(NullPointerException.class, () -> new ContextualAiRerankRequestEntity(MODEL_VALUE, QUERY_VALUE, null, null, null));
    }
}
