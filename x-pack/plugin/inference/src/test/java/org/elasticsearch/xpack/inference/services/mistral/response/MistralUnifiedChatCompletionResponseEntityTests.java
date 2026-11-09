/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral.response;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class MistralUnifiedChatCompletionResponseEntityTests extends ESTestCase {

    public void testStringContentIsParsedAsBefore() throws IOException {
        var result = MistralUnifiedChatCompletionResponseEntity.fromResponse("""
            {"id":"1","object":"chat.completion","model":"mistral-reasoning-test",
             "choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"hello"}}]}
            """.getBytes(StandardCharsets.UTF_8));

        assertThat(result.choices(), hasSize(1));
        var choice = result.choices().get(0);
        assertThat(choice.finishReason(), is("stop"));
        assertThat(choice.message().content(), is("hello"));
        assertNull(choice.message().reasoning());
    }

    public void testListContentIsSplitIntoContentAndReasoning() throws IOException {
        var result = MistralUnifiedChatCompletionResponseEntity.fromResponse("""
            {"id":"1","object":"chat.completion","model":"mistral-reasoning-test",
             "choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":[
               {"type":"thinking","thinking":[{"type":"text","text":"let me think"}]},
               {"type":"text","text":"the answer"}
             ]}}]}
            """.getBytes(StandardCharsets.UTF_8));

        var message = result.choices().get(0).message();
        assertThat(message.content(), is("the answer"));
        assertThat(message.reasoning(), is("let me think"));
        assertThat(message.role(), is("assistant"));
    }
}
