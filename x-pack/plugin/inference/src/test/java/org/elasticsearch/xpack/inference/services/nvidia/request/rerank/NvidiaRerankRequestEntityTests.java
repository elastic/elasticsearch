/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia.request.rerank;

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

public class NvidiaRerankRequestEntityTests extends ESTestCase {
    private static final String INPUT = "documents";
    private static final String QUERY = "query";
    private static final String MODEL = "model";

    public void testXContent_WritesAllFields_WhenTheyAreDefined() throws IOException {
        var entity = new NvidiaRerankRequestEntity(MODEL, QUERY, List.of(INPUT));

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String result = Strings.toString(builder);
        String expected = """
            {
                "model": "model",
                "query": {"text":"query"},
                "passages": [{"text":"documents"}]
            }
            """;
        assertThat(result, is(stripWhitespace(expected)));
    }
}
