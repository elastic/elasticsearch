/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia.request.embeddings;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.cohere.CohereTruncation;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.is;

public class NvidiaEmbeddingsRequestEntityTests extends ESTestCase {
    public void testXContent_AllFields() throws IOException {
        var entity = new NvidiaEmbeddingsRequestEntity(List.of("abc"), "model", InputType.SEARCH, CohereTruncation.START);
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(XContentHelper.stripWhitespace("""
            {
                "input": ["abc"],
                "model": "model",
                "input_type": "query",
                "truncate": "start"
            }
            """)));
    }

    public void testXContent_OnlyMandatoryFields() throws IOException {
        var entity = new NvidiaEmbeddingsRequestEntity(List.of("abc"), "model", null, null);
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(XContentHelper.stripWhitespace("""
            {
                "input": ["abc"],
                "model": "model"
            }
            """)));
    }

    public void testXContent_ModelIdNull_ThrowsException() {
        expectThrows(NullPointerException.class, () -> new NvidiaEmbeddingsRequestEntity(List.of("abc"), null, null, null));
    }

}
