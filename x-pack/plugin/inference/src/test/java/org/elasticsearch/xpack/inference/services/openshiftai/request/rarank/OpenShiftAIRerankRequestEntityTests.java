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
    private static final String DOCUMENT = "some document";
    private static final String QUERY = "some query";
    private static final String MODEL = "some model";
    private static final Integer TOP_N = 8;
    private static final Boolean RETURN_DOCUMENTS = true;

    public void testXContent_WritesAllFields_WhenTheyAreDefined() throws IOException {
        var entity = new OpenShiftAIRerankRequestEntity(MODEL, QUERY, List.of(DOCUMENT), RETURN_DOCUMENTS, TOP_N);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String result = Strings.toString(builder);
        String expected = """
            {
                "model": "some model",
                "query": "some query",
                "documents": ["some document"],
                "top_n": 8,
                "return_documents": true
            }
            """;
        assertThat(stripWhitespace(expected), is(result));
    }

    public void testXContent_WritesMinimalFields() throws IOException {
        var entity = new OpenShiftAIRerankRequestEntity(null, QUERY, List.of(DOCUMENT), null, null);

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
