/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.anthropic;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.anthropic.completion.AnthropicChatCompletionServiceSettings;
import org.elasticsearch.xpack.inference.services.anthropic.completion.AnthropicChatCompletionTaskSettings;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;

public class AnthropicChatCompletionRequestEntityTests extends ESTestCase {

    public void testXContent() throws IOException {
        var entity = new AnthropicChatCompletionRequestEntity(
            List.of("abc"),
            new AnthropicChatCompletionServiceSettings("model", null),
            new AnthropicChatCompletionTaskSettings(1, -1.0, 1.2, 3),
            false
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"messages":[{"role":"user","content":"abc"}],"model":"model","max_tokens":1,"temperature":-1.0,"top_p":1.2,"top_k":3}"""));

    }

    public void testXContent_WithoutTemperature() throws IOException {
        var entity = new AnthropicChatCompletionRequestEntity(
            List.of("abc"),
            new AnthropicChatCompletionServiceSettings("model", null),
            new AnthropicChatCompletionTaskSettings(1, null, 1.2, 3),
            false
        );

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        entity.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, is("""
            {"messages":[{"role":"user","content":"abc"}],"model":"model","max_tokens":1,"top_p":1.2,"top_k":3}"""));

    }
}
