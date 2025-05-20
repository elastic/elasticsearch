/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.request;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.cohere.request.completion.CohereCompletionRequestEntity;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;

public class CohereCompletionRequestEntityTests extends ESTestCase {

    public void testXContent_WritesAllFields() throws IOException {
        var entity = new CohereCompletionRequestEntity(List.of("some input"), "model", false);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"message":"some input","model":"model"}"""));
    }

    public void testXContent_DoesNotWriteModelIfNotSpecified() throws IOException {
        var entity = new CohereCompletionRequestEntity(List.of("some input"), null, false);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"message":"some input"}"""));
    }

    public void testXContent_ThrowsIfInputIsNull() {
        expectThrows(NullPointerException.class, () -> new CohereCompletionRequestEntity(null, null, false));
    }

    public void testXContent_ThrowsIfMessageInInputIsNull() {
        expectThrows(NullPointerException.class, () -> new CohereCompletionRequestEntity(List.of((String) null), null, false));
    }
}
