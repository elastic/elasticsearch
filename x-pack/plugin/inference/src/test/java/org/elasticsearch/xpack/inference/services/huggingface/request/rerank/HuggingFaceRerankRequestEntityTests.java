/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.request.rerank;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.huggingface.rerank.HuggingFaceRerankTaskSettings;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentHelper.stripWhitespace;

public class HuggingFaceRerankRequestEntityTests extends ESTestCase {
    private static final String INPUT = "texts";
    private static final String QUERY = "query";
    private static final Integer TOP_N = 8;
    private static final Boolean RETURN_DOCUMENTS = false;

    public void testXContent_WritesAllFields_WhenTheyAreDefined() throws IOException {
        var entity = new HuggingFaceRerankRequestEntity(
            QUERY,
            List.of(INPUT),
            Boolean.TRUE,
            TOP_N,
            new HuggingFaceRerankTaskSettings(TOP_N, RETURN_DOCUMENTS)
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String xContentResult = Strings.toString(builder);
        String expected = """
            {"texts":["texts"],
            "query":"query",
            "return_text":true,
            "top_n":8}""";
        assertEquals(stripWhitespace(expected), xContentResult);
    }

    public void testXContent_WritesMinimalFields() throws IOException {
        var entity = new HuggingFaceRerankRequestEntity(QUERY, List.of(INPUT), null, null, new HuggingFaceRerankTaskSettings(null, null));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String xContentResult = Strings.toString(builder);
        String expected = """
            {"texts":["texts"],"query":"query"}""";
        assertEquals(stripWhitespace(expected), xContentResult);
    }
}
