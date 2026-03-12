/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openshiftai.request.rarank;

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

public class OpenShiftAIRerankRequestEntityTests extends ESTestCase {
    private static final List<String> DOCUMENT_VALUE = List.of("some document");
    private static final String QUERY_VALUE = "some query";
    private static final String MODEL_VALUE = "some_model";
    private static final Integer TOP_N_VALUE = 8;
    private static final Boolean RETURN_DOCUMENTS_VALUE = true;

    public void testXContent_WritesAllFields_WhenTheyAreDefined() throws IOException {
        var entity = new OpenShiftAIRerankRequestEntity(MODEL_VALUE, QUERY_VALUE, DOCUMENT_VALUE, RETURN_DOCUMENTS_VALUE, TOP_N_VALUE);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String result = Strings.toString(builder);
        String expected = """
            {
                "model": "some_model",
                "query": "some query",
                "documents": ["some document"],
                "top_n": 8,
                "return_documents": true
            }
            """;
        assertThat(stripWhitespace(expected), is(result));
    }

    public void testXContent_WritesMinimalFields() throws IOException {
        var entity = new OpenShiftAIRerankRequestEntity(null, QUERY_VALUE, DOCUMENT_VALUE, null, null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String result = Strings.toString(builder);
        String expected = """
            {
                "query": "some query",
                "documents": ["some document"]
            }
            """;
        assertThat(stripWhitespace(expected), is(result));
    }

}
