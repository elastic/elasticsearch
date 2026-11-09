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

public class MistralCompletionResponseEntityTests extends ESTestCase {

    public void testStringContentIsParsedAsBefore() throws IOException {
        var result = MistralCompletionResponseEntity.fromResponse("""
            {"id":"1","object":"chat.completion","model":"mistral-reasoning-test",
             "choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":"hello"}}]}
            """.getBytes(StandardCharsets.UTF_8));

        assertThat(result.getResults(), hasSize(1));
        assertThat(result.getResults().get(0).content(), is("hello"));
    }

    public void testListContentIsReducedToTheAnswerText() throws IOException {
        var result = MistralCompletionResponseEntity.fromResponse("""
            {"id":"1","object":"chat.completion","model":"mistral-reasoning-test",
             "choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":[
               {"type":"thinking","thinking":[{"type":"text","text":"let me think"}]},
               {"type":"text","text":"the answer"}
             ]}}]}
            """.getBytes(StandardCharsets.UTF_8));

        assertThat(result.getResults(), hasSize(1));
        assertThat(result.getResults().get(0).content(), is("the answer"));
    }
}
