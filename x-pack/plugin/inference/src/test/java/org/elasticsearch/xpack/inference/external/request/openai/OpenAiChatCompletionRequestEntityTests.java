/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.openai;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;

public class OpenAiChatCompletionRequestEntityTests extends ESTestCase {

    public void testXContent_WritesUserWhenDefined() throws IOException {
        var entity = new OpenAiChatCompletionRequestEntity(List.of("abc"), "model", "user", false);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"messages":[{"role":"user","content":"abc"}],"model":"model","n":1,"user":"user"}"""));

    }

    public void testXContent_DoesNotWriteUserWhenItIsNull() throws IOException {
        var entity = new OpenAiChatCompletionRequestEntity(List.of("abc"), "model", null, false);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"messages":[{"role":"user","content":"abc"}],"model":"model","n":1}"""));
    }

    public void testXContent_ThrowsIfModelIsNull() {
        assertThrows(NullPointerException.class, () -> new OpenAiChatCompletionRequestEntity(List.of("abc"), null, "user", false));
    }

    public void testXContent_ThrowsIfMessagesAreNull() {
        assertThrows(NullPointerException.class, () -> new OpenAiChatCompletionRequestEntity(null, "model", "user", false));
    }
}
