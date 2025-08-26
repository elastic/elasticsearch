/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class UnifiedCompletionRequestTests extends AbstractBWCWireSerializationTestCase<UnifiedCompletionRequest> {

    public void testParseAllFields() throws IOException {
        String requestJson = """
            {
                "messages": [
                  {
                    "content": [
                        {
                          "text": "some text",
                          "type": "string"
                        }
                    ],
                    "role": "user",
                    "tool_call_id": "100",
                    "tool_calls": [
                        {
                            "id": "call_62136354",
                            "function": {
                                "arguments": "{'order_id': 'order_12345'}",
                                "name": "get_delivery_date"
                            },
                            "type": "function"
                        }
                    ]
                  }
                ],
                "stop": ["stop"],
                "temperature": 0.1,
                "tool_choice": {
                  "type": "function",
                  "function": {
                    "name": "some function"
                  }
                },
                "tools": [
                  {
                    "type": "function",
                    "function": {
                      "description": "Get the current weather in a given location",
                      "name": "get_current_weather",
                      "parameters": {
                        "type": "object"
                      }
                    }
                  }
                ],
                "top_p": 0.2,
                "max_completion_tokens": 100,
                "model": "gpt-4o"
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = UnifiedCompletionRequest.PARSER.apply(parser, null);
            var expected = new UnifiedCompletionRequest(
                List.of(
                    new UnifiedCompletionRequest.Message(
                        new UnifiedCompletionRequest.ContentObjects(
                            List.of(new UnifiedCompletionRequest.ContentObject("some text", "string"))
                        ),
                        "user",
                        "100",
                        List.of(
                            new UnifiedCompletionRequest.ToolCall(
                                "call_62136354",
                                new UnifiedCompletionRequest.ToolCall.FunctionField("{'order_id': 'order_12345'}", "get_delivery_date"),
                                "function"
                            )
                        )
                    )
                ),
                "gpt-4o",
                100L,
                List.of("stop"),
                0.1F,
                new UnifiedCompletionRequest.ToolChoiceObject(
                    "function",
                    new UnifiedCompletionRequest.ToolChoiceObject.FunctionField("some function")
                ),
                List.of(
                    new UnifiedCompletionRequest.Tool(
                        "function",
                        new UnifiedCompletionRequest.Tool.FunctionField(
                            "Get the current weather in a given location",
                            "get_current_weather",
                            Map.of("type", "object"),
                            null
                        )
                    )
                ),
                0.2F
            );

            assertThat(request, is(expected));
            assertThat(
                Strings.toString(request, UnifiedCompletionRequest.withMaxCompletionTokensTokens("gpt-4o", ToXContent.EMPTY_PARAMS)),
                is(XContentHelper.stripWhitespace(requestJson))
            );
        }
    }

    public void testParsing() throws IOException {
        String requestJson = """
            {
                "model": "gpt-4o",
                "messages": [
                  {
                    "role": "user",
                    "content": "What is the weather like in Boston today?"
                  }
                ],
                "stop": "none",
                "tools": [
                  {
                    "type": "function",
                    "function": {
                      "name": "get_current_weather",
                      "description": "Get the current weather in a given location",
                      "parameters": {
                        "type": "object"
                      }
                    }
                  }
                ],
                "tool_choice": "auto"
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, requestJson)) {
            var request = UnifiedCompletionRequest.PARSER.apply(parser, null);
            var expected = new UnifiedCompletionRequest(
                List.of(
                    new UnifiedCompletionRequest.Message(
                        new UnifiedCompletionRequest.ContentString("What is the weather like in Boston today?"),
                        "user",
                        null,
                        null
                    )
                ),
                "gpt-4o",
                null,
                List.of("none"),
                null,
                new UnifiedCompletionRequest.ToolChoiceString("auto"),
                List.of(
                    new UnifiedCompletionRequest.Tool(
                        "function",
                        new UnifiedCompletionRequest.Tool.FunctionField(
                            "Get the current weather in a given location",
                            "get_current_weather",
                            Map.of("type", "object"),
                            null
                        )
                    )
                ),
                null
            );

            assertThat(request, is(expected));
        }
    }

    public static UnifiedCompletionRequest randomUnifiedCompletionRequest() {
        return new UnifiedCompletionRequest(
            randomList(5, UnifiedCompletionRequestTests::randomMessage),
            randomAlphaOfLengthOrNull(10),
            randomNonNegativeLongOrNull(),
            randomStopOrNull(),
            randomFloatOrNull(),
            randomToolChoiceOrNull(),
            randomToolListOrNull(),
            randomFloatOrNull()
        );
    }

    public static UnifiedCompletionRequest.Message randomMessage() {
        return new UnifiedCompletionRequest.Message(
            randomContent(),
            randomAlphaOfLength(10),
            randomAlphaOfLengthOrNull(10),
            randomToolCallListOrNull()
        );
    }

    public static UnifiedCompletionRequest.Content randomContent() {
        return randomBoolean()
            ? new UnifiedCompletionRequest.ContentString(randomAlphaOfLength(10))
            : new UnifiedCompletionRequest.ContentObjects(randomList(10, UnifiedCompletionRequestTests::randomContentObject));
    }

    public static UnifiedCompletionRequest.ContentObject randomContentObject() {
        return new UnifiedCompletionRequest.ContentObject(randomAlphaOfLength(10), randomAlphaOfLength(10));
    }

    public static List<UnifiedCompletionRequest.ToolCall> randomToolCallListOrNull() {
        return randomBoolean() ? randomList(10, UnifiedCompletionRequestTests::randomToolCall) : null;
    }

    public static UnifiedCompletionRequest.ToolCall randomToolCall() {
        return new UnifiedCompletionRequest.ToolCall(randomAlphaOfLength(10), randomToolCallFunctionField(), randomAlphaOfLength(10));
    }

    public static UnifiedCompletionRequest.ToolCall.FunctionField randomToolCallFunctionField() {
        return new UnifiedCompletionRequest.ToolCall.FunctionField(randomAlphaOfLength(10), randomAlphaOfLength(10));
    }

    public static List<String> randomStopOrNull() {
        return randomBoolean() ? randomStop() : null;
    }

    public static List<String> randomStop() {
        return randomList(5, () -> randomAlphaOfLength(10));
    }

    public static UnifiedCompletionRequest.ToolChoice randomToolChoiceOrNull() {
        return randomBoolean() ? randomToolChoice() : null;
    }

    public static UnifiedCompletionRequest.ToolChoice randomToolChoice() {
        return randomBoolean()
            ? new UnifiedCompletionRequest.ToolChoiceString(randomAlphaOfLength(10))
            : new UnifiedCompletionRequest.ToolChoiceObject(randomAlphaOfLength(10), randomToolChoiceObjectFunctionField());
    }

    public static UnifiedCompletionRequest.ToolChoiceObject.FunctionField randomToolChoiceObjectFunctionField() {
        return new UnifiedCompletionRequest.ToolChoiceObject.FunctionField(randomAlphaOfLength(10));
    }

    public static List<UnifiedCompletionRequest.Tool> randomToolListOrNull() {
        return randomBoolean() ? randomList(10, UnifiedCompletionRequestTests::randomTool) : null;
    }

    public static UnifiedCompletionRequest.Tool randomTool() {
        return new UnifiedCompletionRequest.Tool(randomAlphaOfLength(10), randomToolFunctionField());
    }

    public static UnifiedCompletionRequest.Tool.FunctionField randomToolFunctionField() {
        return new UnifiedCompletionRequest.Tool.FunctionField(
            randomAlphaOfLengthOrNull(10),
            randomAlphaOfLength(10),
            null,
            randomOptionalBoolean()
        );
    }

    @Override
    protected UnifiedCompletionRequest mutateInstanceForVersion(UnifiedCompletionRequest instance, TransportVersion version) {
        return instance;
    }

    @Override
    protected Writeable.Reader<UnifiedCompletionRequest> instanceReader() {
        return UnifiedCompletionRequest::new;
    }

    @Override
    protected UnifiedCompletionRequest createTestInstance() {
        return randomUnifiedCompletionRequest();
    }

    @Override
    protected UnifiedCompletionRequest mutateInstance(UnifiedCompletionRequest instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(UnifiedCompletionRequest.getNamedWriteables());
    }
}
