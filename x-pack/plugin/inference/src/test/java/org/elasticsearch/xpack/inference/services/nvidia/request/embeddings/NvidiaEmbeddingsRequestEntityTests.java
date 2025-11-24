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
import org.elasticsearch.xpack.inference.common.model.Truncation;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.is;

public class NvidiaEmbeddingsRequestEntityTests extends ESTestCase {
    // Test values
    private static final String MODEL_VALUE = "some_model";
    private static final List<String> INPUT_VALUE = List.of("some input");
    private static final InputType INPUT_TYPE_VALUE = InputType.INGEST;
    private static final Truncation TRUNCATE_VALUE = Truncation.START;

    public void testXContent_AllFields() throws IOException {
        var entity = new NvidiaEmbeddingsRequestEntity(INPUT_VALUE, MODEL_VALUE, INPUT_TYPE_VALUE, TRUNCATE_VALUE);
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(XContentHelper.stripWhitespace("""
            {
                "input": ["some input"],
                "model": "some_model",
                "input_type": "passage",
                "truncate": "start"
            }
            """)));
    }

    public void testXContent_OnlyMandatoryFields() throws IOException {
        var entity = new NvidiaEmbeddingsRequestEntity(INPUT_VALUE, MODEL_VALUE, INPUT_TYPE_VALUE, null);
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is(XContentHelper.stripWhitespace("""
            {
                "input": ["some input"],
                "model": "some_model",
                "input_type": "passage"
            }
            """)));
    }

    public void testCreateRequestEntity_ModelIdNull_ThrowsException() {
        expectThrows(
            NullPointerException.class,
            () -> new NvidiaEmbeddingsRequestEntity(INPUT_VALUE, null, INPUT_TYPE_VALUE, TRUNCATE_VALUE)
        );
    }

}
