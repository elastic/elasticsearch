/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openshiftai.request.rarank;

import junit.framework.TestCase;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentHelper.stripWhitespace;

public class OpenShiftAIRerankRequestEntityTests extends TestCase {
    private static final String INPUT = "documents";
    private static final String QUERY = "query";
    private static final String MODEL = "model";
    private static final Integer TOP_N = 8;
    private static final Boolean RETURN_DOCUMENTS = true;

    public void testXContent_WritesAllFields_WhenTheyAreDefined() throws IOException {
        var entity = new OpenShiftAIRerankRequestEntity(MODEL, QUERY, List.of(INPUT), RETURN_DOCUMENTS, TOP_N);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String result = Strings.toString(builder);
        String expected = """
            {
                "model": "model",
                "query": "query",
                "documents": ["documents"],
                "top_n": 8,
                "return_documents": true
            }
            """;
        assertEquals(stripWhitespace(expected), result);
    }

    public void testXContent_WritesMinimalFields() throws IOException {
        var entity = new OpenShiftAIRerankRequestEntity(null, QUERY, List.of(INPUT), null, null);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String result = Strings.toString(builder);
        String expected = """
            {
                "query": "query",
                "documents": ["documents"]
            }
            """;
        assertEquals(stripWhitespace(expected), result);
    }

}
