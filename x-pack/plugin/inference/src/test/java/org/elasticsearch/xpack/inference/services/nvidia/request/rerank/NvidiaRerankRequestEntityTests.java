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
    private static final String FIRST_PASSAGE_VALUE = "some document";
    private static final String SECOND_PASSAGE_VALUE = "some other document";
    private static final List<String> PASSAGES_VALUE = List.of(FIRST_PASSAGE_VALUE, SECOND_PASSAGE_VALUE);
    private static final String QUERY_VALUE = "some query";
    private static final String MODEL_VALUE = "some_model";

    public void testXContent_WritesAllFields() throws IOException {
        var entity = new NvidiaRerankRequestEntity(MODEL_VALUE, QUERY_VALUE, PASSAGES_VALUE);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String result = Strings.toString(builder);
        String expected = Strings.format("""
            {
                "model": "%s",
                "query": {
                    "text": "%s"
                },
                "passages": [
                    {
                        "text": "%s"
                    },
                    {
                        "text": "%s"
                    }
                ]
            }
            """, MODEL_VALUE, QUERY_VALUE, FIRST_PASSAGE_VALUE, SECOND_PASSAGE_VALUE);
        assertThat(result, is(stripWhitespace(expected)));
    }

    public void testCreateRequestEntity_NoModelId_ThrowsException() {
        expectThrows(NullPointerException.class, () -> new NvidiaRerankRequestEntity(null, QUERY_VALUE, PASSAGES_VALUE));
    }

    public void testCreateRequestEntity_NoQuery_ThrowsException() {
        expectThrows(NullPointerException.class, () -> new NvidiaRerankRequestEntity(MODEL_VALUE, null, PASSAGES_VALUE));
    }

    public void testCreateRequestEntity_NoPassages_ThrowsException() {
        expectThrows(NullPointerException.class, () -> new NvidiaRerankRequestEntity(MODEL_VALUE, QUERY_VALUE, null));
    }
}
