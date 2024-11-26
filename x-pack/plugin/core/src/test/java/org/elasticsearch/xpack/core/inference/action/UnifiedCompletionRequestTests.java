/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.UnifiedCompletionRequest;
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
                "model": "gpt-4o",
                "messages": [
                  {
                    "role": "user",
                    "content": [
                        {
                          "text": "some text",
                          "type": "string"
                        }
                    ],
                    "name": "a name",
                    "tool_call_id": "100",
                    "tool_calls": [
                        {
                            "id": "call_62136354",
                            "type": "function",
                            "function": {
                                "arguments": "{'order_id': 'order_12345'}",
                                "name": "get_delivery_date"
                            }
                        }
                    ]
                  }
                ],
                "max_completion_tokens": 100,
                "n": 1,
                "stop": ["stop"],
                "temperature": 0.1,
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
                "tool_choice": {
                  "type": "function",
                  "function": {
                    "name": "some function"
                  }
                },
                "top_p": 0.2,
                "user": "user"
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
                        "a name",
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
                1,
                new UnifiedCompletionRequest.StopValues(List.of("stop")),
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
                0.2F,
                "user"
            );

            assertThat(request, is(expected));
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
                        null,
                        null
                    )
                ),
                "gpt-4o",
                null,
                null,
                new UnifiedCompletionRequest.StopString("none"),
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
                null,
                null
            );

            assertThat(request, is(expected));
        }
    }

    public static UnifiedCompletionRequest randomUnifiedCompletionRequest() {
        return new UnifiedCompletionRequest(
            randomList(5, UnifiedCompletionRequestTests::randomMessage),
            randomNullOrAlphaOfLength(10),
            randomNullOrPositiveLong(),
            randomNullOrPositiveInt(),
            randomNullOrStop(),
            randomNullOrFloat(),
            randomNullOrToolChoice(),
            randomList(5, UnifiedCompletionRequestTests::randomTool),
            randomNullOrFloat(),
            randomNullOrAlphaOfLength(10)
        );
    }

    public static UnifiedCompletionRequest.Message randomMessage() {
        return new UnifiedCompletionRequest.Message(
            randomContent(),
            randomAlphaOfLength(10),
            randomNullOrAlphaOfLength(10),
            randomNullOrAlphaOfLength(10),
            randomList(10, UnifiedCompletionRequestTests::randomToolCall)
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

    public static UnifiedCompletionRequest.ToolCall randomToolCall() {
        return new UnifiedCompletionRequest.ToolCall(randomAlphaOfLength(10), randomToolCallFunctionField(), randomAlphaOfLength(10));
    }

    public static UnifiedCompletionRequest.ToolCall.FunctionField randomToolCallFunctionField() {
        return new UnifiedCompletionRequest.ToolCall.FunctionField(randomAlphaOfLength(10), randomAlphaOfLength(10));
    }

    public static UnifiedCompletionRequest.Stop randomNullOrStop() {
        return randomBoolean() ? randomStop() : null;
    }

    public static UnifiedCompletionRequest.Stop randomStop() {
        return randomBoolean()
            ? new UnifiedCompletionRequest.StopString(randomAlphaOfLength(10))
            : new UnifiedCompletionRequest.StopValues(randomList(5, () -> randomAlphaOfLength(10)));
    }

    public static UnifiedCompletionRequest.ToolChoice randomNullOrToolChoice() {
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

    public static UnifiedCompletionRequest.Tool randomTool() {
        return new UnifiedCompletionRequest.Tool(randomAlphaOfLength(10), randomToolFunctionField());
    }

    public static UnifiedCompletionRequest.Tool.FunctionField randomToolFunctionField() {
        return new UnifiedCompletionRequest.Tool.FunctionField(
            randomNullOrAlphaOfLength(10),
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
        // List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        // entries.addAll(new MlInferenceNamedXContentProvider().getNamedWriteables());
        // entries.addAll(InferenceNamedWriteablesProvider.getNamedWriteables());
        return new NamedWriteableRegistry(UnifiedCompletionRequest.getNamedWriteables());
    }
}
