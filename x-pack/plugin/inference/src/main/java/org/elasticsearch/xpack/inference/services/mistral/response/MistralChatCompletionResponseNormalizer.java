/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral.response;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Normalizes Mistral chat completion payloads into the shape the shared OpenAI parser expects.
 *
 * <p>Mistral reasoning models may return {@code content} as a list of typed chunks instead of a
 * string, e.g.
 * <pre>{@code
 * "content": [
 *   {"type": "thinking", "thinking": [{"type": "text", "text": "let me think"}]},
 *   {"type": "text", "text": "the answer"}
 * ]}</pre>
 * The shared OpenAI parser ({@code OpenAiUnifiedChatCompletionParser}) only accepts a string
 * {@code content}, so a list would fail the whole response. Rather than forking that parser, this
 * class rewrites the payload into the OpenAI shape before it reaches the parser: the text chunks are
 * joined into the string {@code content} and the thinking chunks are joined into a {@code reasoning}
 * field, which the shared parser already passes through to the unified response.
 *
 * <p>Payloads that need no rewriting are returned byte-for-byte, so string content behaves exactly as
 * before. Only Mistral payloads go through this class; the OpenAI service is untouched.
 */
public final class MistralChatCompletionResponseNormalizer {

    private static final String CHOICES_FIELD = "choices";
    private static final List<String> CHOICE_CONTENT_FIELDS = List.of("delta", "message");
    private static final String CONTENT_FIELD = "content";
    private static final String REASONING_FIELD = "reasoning";
    private static final String TYPE_FIELD = "type";
    private static final String TEXT_FIELD = "text";
    private static final String THINKING_FIELD = "thinking";
    private static final String TEXT_TYPE = "text";
    private static final String THINKING_TYPE = "thinking";

    /**
     * Normalizes one streaming event payload.
     *
     * @param data the raw {@code data} of an SSE event
     * @return the normalized JSON, or {@code data} unchanged when it is not a JSON object or nothing had to change
     */
    public static String normalizeStreamingEvent(String data) throws IOException {
        return normalize(data);
    }

    /**
     * Normalizes a non-streaming response body.
     *
     * @param body the raw response body
     * @return the normalized JSON, or the body unchanged when nothing had to change
     */
    public static byte[] normalizeNonStreamingResponse(byte[] body) throws IOException {
        var data = new String(body, StandardCharsets.UTF_8);
        var normalized = normalize(data);
        return normalized == data ? body : normalized.getBytes(StandardCharsets.UTF_8);
    }

    private static String normalize(String data) throws IOException {
        Map<String, Object> payload = parseObject(data);
        if (payload == null || normalizeChoices(payload) == false) {
            return data;
        }

        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.map(payload);
        return Strings.toString(builder);
    }

    /**
     * @return the payload as a mutable map, or null if {@code data} is not a JSON object (for example the
     *         {@code [DONE]} sentinel), in which case the caller leaves it for the downstream parser to judge
     */
    private static Map<String, Object> parseObject(String data) throws IOException {
        try (var parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, data)) {
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                return null;
            }
            return parser.mapOrdered();
        } catch (IOException e) {
            // Not valid JSON: leave it alone so the downstream parser reports it with its usual error.
            return null;
        }
    }

    /**
     * @return true if any choice was rewritten
     */
    private static boolean normalizeChoices(Map<String, Object> payload) {
        boolean changed = false;
        if (payload.get(CHOICES_FIELD) instanceof List<?> choices) {
            for (Object choice : choices) {
                if (choice instanceof Map<?, ?> choiceMap) {
                    for (String field : CHOICE_CONTENT_FIELDS) {
                        if (choiceMap.get(field) instanceof Map<?, ?> node) {
                            changed |= normalizeContent(asObjectMap(node));
                        }
                    }
                }
            }
        }
        return changed;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asObjectMap(Map<?, ?> map) {
        // The parser produced this map, so its keys are strings; the cast lets us write back into the node.
        return (Map<String, Object>) map;
    }

    /**
     * Rewrites a list-typed {@code content} into a string {@code content} plus a {@code reasoning}
     * string. String, null and absent content are left untouched.
     *
     * @return true if the node was rewritten
     */
    private static boolean normalizeContent(Map<String, Object> node) {
        if (node.get(CONTENT_FIELD) instanceof List<?> chunks) {
            var text = new StringBuilder();
            var thinking = new StringBuilder();
            for (Object chunk : chunks) {
                if (chunk instanceof String chunkText) {
                    text.append(chunkText);
                } else if (chunk instanceof Map<?, ?> chunkMap) {
                    var type = chunkMap.get(TYPE_FIELD);
                    if (TEXT_TYPE.equals(type)) {
                        text.append(stringOrEmpty(chunkMap.get(TEXT_FIELD)));
                    } else if (THINKING_TYPE.equals(type)) {
                        appendThinking(thinking, chunkMap.get(THINKING_FIELD));
                    }
                    // Unrecognized chunk types are ignored so a provider-side extension does not fail the response.
                }
            }

            node.put(CONTENT_FIELD, text.toString());
            if (thinking.length() > 0) {
                node.put(REASONING_FIELD, thinking.toString());
            }
            return true;
        }
        return false;
    }

    private static void appendThinking(StringBuilder thinking, Object parts) {
        if (parts instanceof List<?> partList) {
            for (Object part : partList) {
                if (part instanceof String partText) {
                    thinking.append(partText);
                } else if (part instanceof Map<?, ?> partMap && TEXT_TYPE.equals(partMap.get(TYPE_FIELD))) {
                    thinking.append(stringOrEmpty(partMap.get(TEXT_FIELD)));
                }
            }
        }
    }

    private static String stringOrEmpty(Object value) {
        return value instanceof String string ? string : "";
    }

    private MistralChatCompletionResponseNormalizer() {}
}
