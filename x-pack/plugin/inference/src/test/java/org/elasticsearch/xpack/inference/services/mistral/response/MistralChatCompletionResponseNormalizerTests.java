/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral.response;

import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class MistralChatCompletionResponseNormalizerTests extends ESTestCase {

    public void testStringContentIsReturnedUnchanged() throws IOException {
        var data = """
            {"id":"1","choices":[{"index":0,"delta":{"role":"assistant","content":"hello"}}]}""";

        assertThat(MistralChatCompletionResponseNormalizer.normalizeStreamingEvent(data), sameInstance(data));
    }

    public void testStringContentNonStreamingIsReturnedUnchanged() throws IOException {
        var body = """
            {"id":"1","choices":[{"index":0,"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}]}""".getBytes(
            StandardCharsets.UTF_8
        );

        assertThat(MistralChatCompletionResponseNormalizer.normalizeNonStreamingResponse(body), sameInstance(body));
    }

    public void testNullContentIsReturnedUnchanged() throws IOException {
        var data = """
            {"id":"1","choices":[{"index":0,"delta":{"content":null,"tool_calls":[{"index":0}]}}]}""";

        assertThat(MistralChatCompletionResponseNormalizer.normalizeStreamingEvent(data), sameInstance(data));
    }

    public void testDoneSentinelIsReturnedUnchanged() throws IOException {
        assertThat(MistralChatCompletionResponseNormalizer.normalizeStreamingEvent("[DONE]"), is("[DONE]"));
    }

    public void testListContentWithOnlyTextIsJoinedIntoString() throws IOException {
        var data = """
            {"id":"1","choices":[{"index":0,"delta":{"content":[{"type":"text","text":"foo"},{"type":"text","text":"bar"}]}}]}""";

        var delta = firstChoiceNode(MistralChatCompletionResponseNormalizer.normalizeStreamingEvent(data), "delta");

        assertThat(delta.get("content"), is("foobar"));
        assertFalse(delta.containsKey("reasoning"));
    }

    public void testListContentWithOnlyThinkingProducesReasoningAndEmptyContent() throws IOException {
        var data = """
            {"id":"1","choices":[{"index":0,"delta":{"content":[
              {"type":"thinking","thinking":[{"type":"text","text":"let me "},{"type":"text","text":"think"}]}
            ]}}]}""";

        var delta = firstChoiceNode(MistralChatCompletionResponseNormalizer.normalizeStreamingEvent(data), "delta");

        assertThat(delta.get("content"), is(""));
        assertThat(delta.get("reasoning"), is("let me think"));
    }

    public void testMixedChunksAreSplitIntoContentAndReasoning() throws IOException {
        var data = """
            {"id":"1","choices":[{"index":0,"delta":{"content":[
              {"type":"thinking","thinking":[{"type":"text","text":"hmm"}]},
              {"type":"text","text":"the answer"}
            ]}}]}""";

        var delta = firstChoiceNode(MistralChatCompletionResponseNormalizer.normalizeStreamingEvent(data), "delta");

        assertThat(delta.get("content"), is("the answer"));
        assertThat(delta.get("reasoning"), is("hmm"));
    }

    public void testNonStreamingListContentIsNormalizedAndOtherFieldsSurvive() throws IOException {
        var body = """
            {"id":"abc","model":"m","choices":[{"index":0,"finish_reason":"stop","message":{"role":"assistant","content":[
              {"type":"thinking","thinking":[{"type":"text","text":"hmm"}]},
              {"type":"text","text":"done"}
            ]}}],"usage":{"total_tokens":3}}""".getBytes(StandardCharsets.UTF_8);

        var normalized = MistralChatCompletionResponseNormalizer.normalizeNonStreamingResponse(body);

        assertThat(normalized, not(sameInstance(body)));
        var payload = XContentHelper.convertToMap(XContentType.JSON.xContent(), new String(normalized, StandardCharsets.UTF_8), false);
        assertThat(payload.get("id"), is("abc"));
        assertThat(payload.get("usage"), is(Map.of("total_tokens", 3)));
        var message = firstChoiceNode(new String(normalized, StandardCharsets.UTF_8), "message");
        assertThat(message.get("content"), is("done"));
        assertThat(message.get("reasoning"), is("hmm"));
        assertThat(message.get("role"), is("assistant"));
    }

    public void testUnrecognizedChunkTypesAreIgnored() throws IOException {
        var data = """
            {"id":"1","choices":[{"index":0,"delta":{"content":[{"type":"reference","reference_ids":[1]},{"type":"text","text":"x"}]}}]}""";

        var delta = firstChoiceNode(MistralChatCompletionResponseNormalizer.normalizeStreamingEvent(data), "delta");

        assertThat(delta.get("content"), is("x"));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> firstChoiceNode(String json, String node) {
        var payload = XContentHelper.convertToMap(XContentType.JSON.xContent(), json, false);
        var choices = (List<Map<String, Object>>) payload.get("choices");
        return (Map<String, Object>) choices.get(0).get(node);
    }
}
