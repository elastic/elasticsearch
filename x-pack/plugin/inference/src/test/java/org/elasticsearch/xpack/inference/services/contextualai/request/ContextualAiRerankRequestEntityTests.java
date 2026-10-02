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

import static org.elasticsearch.common.xcontent.XContentHelper.stripWhitespace;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_DOCUMENTS;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_FIRST_DOCUMENT;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_INSTRUCTION;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_MODEL_ID;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_QUERY;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_SECOND_DOCUMENT;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_TOP_N;
import static org.hamcrest.Matchers.is;

public class ContextualAiRerankRequestEntityTests extends ESTestCase {

    public void testXContent_WritesAllFields() throws IOException {
        var entity = new ContextualAiRerankRequestEntity(TEST_MODEL_ID, TEST_QUERY, TEST_DOCUMENTS, TEST_TOP_N, TEST_INSTRUCTION);

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
            """, TEST_QUERY, TEST_FIRST_DOCUMENT, TEST_SECOND_DOCUMENT, TEST_MODEL_ID, TEST_TOP_N, TEST_INSTRUCTION);
        assertThat(result, is(stripWhitespace(expected)));
    }

    public void testXContent_WritesRequiredFieldsOnly() throws IOException {
        var entity = new ContextualAiRerankRequestEntity(TEST_MODEL_ID, TEST_QUERY, TEST_DOCUMENTS, null, null);

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
            """, TEST_QUERY, TEST_FIRST_DOCUMENT, TEST_SECOND_DOCUMENT, TEST_MODEL_ID);
        assertThat(result, is(stripWhitespace(expected)));
    }

    public void testCreateRequestEntity_NoModelId_ThrowsException() {
        expectThrows(NullPointerException.class, () -> new ContextualAiRerankRequestEntity(null, TEST_QUERY, TEST_DOCUMENTS, null, null));
    }

    public void testCreateRequestEntity_NoQuery_ThrowsException() {
        expectThrows(
            NullPointerException.class,
            () -> new ContextualAiRerankRequestEntity(TEST_MODEL_ID, null, TEST_DOCUMENTS, null, null)
        );
    }

    public void testCreateRequestEntity_NoDocuments_ThrowsException() {
        expectThrows(NullPointerException.class, () -> new ContextualAiRerankRequestEntity(TEST_MODEL_ID, TEST_QUERY, null, null, null));
    }
}
