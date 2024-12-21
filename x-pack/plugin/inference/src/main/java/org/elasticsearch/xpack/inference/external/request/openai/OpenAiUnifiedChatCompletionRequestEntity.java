/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.openai;

import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.openai.completion.OpenAiChatCompletionModel;

import java.io.IOException;
import java.util.Objects;

public class OpenAiUnifiedChatCompletionRequestEntity implements ToXContentObject {

    public static final String NAME_FIELD = "name";
    public static final String TOOL_CALL_ID_FIELD = "tool_call_id";
    public static final String TOOL_CALLS_FIELD = "tool_calls";
    public static final String ID_FIELD = "id";
    public static final String FUNCTION_FIELD = "function";
    public static final String ARGUMENTS_FIELD = "arguments";
    public static final String DESCRIPTION_FIELD = "description";
    public static final String PARAMETERS_FIELD = "parameters";
    public static final String STRICT_FIELD = "strict";
    public static final String TOP_P_FIELD = "top_p";
    public static final String USER_FIELD = "user";
    public static final String STREAM_FIELD = "stream";
    private static final String NUMBER_OF_RETURNED_CHOICES_FIELD = "n";
    private static final String MODEL_FIELD = "model";
    public static final String MESSAGES_FIELD = "messages";
    private static final String ROLE_FIELD = "role";
    private static final String CONTENT_FIELD = "content";
    private static final String MAX_COMPLETION_TOKENS_FIELD = "max_completion_tokens";
    private static final String STOP_FIELD = "stop";
    private static final String TEMPERATURE_FIELD = "temperature";
    private static final String TOOL_CHOICE_FIELD = "tool_choice";
    private static final String TOOL_FIELD = "tools";
    private static final String TEXT_FIELD = "text";
    private static final String TYPE_FIELD = "type";
    private static final String STREAM_OPTIONS_FIELD = "stream_options";
    private static final String INCLUDE_USAGE_FIELD = "include_usage";

    private final UnifiedCompletionRequest unifiedRequest;
    private final boolean stream;
    private final OpenAiChatCompletionModel model;

    public OpenAiUnifiedChatCompletionRequestEntity(UnifiedChatInput unifiedChatInput, OpenAiChatCompletionModel model) {
        Objects.requireNonNull(unifiedChatInput);

        this.unifiedRequest = unifiedChatInput.getRequest();
        this.stream = unifiedChatInput.stream();
        this.model = Objects.requireNonNull(model);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray(MESSAGES_FIELD);
        {
            for (UnifiedCompletionRequest.Message message : unifiedRequest.messages()) {
                builder.startObject();
                {
                    switch (message.content()) {
                        case UnifiedCompletionRequest.ContentString contentString -> builder.field(CONTENT_FIELD, contentString.content());
                        case UnifiedCompletionRequest.ContentObjects contentObjects -> {
                            builder.startArray(CONTENT_FIELD);
                            for (UnifiedCompletionRequest.ContentObject contentObject : contentObjects.contentObjects()) {
                                builder.startObject();
                                builder.field(TEXT_FIELD, contentObject.text());
                                builder.field(TYPE_FIELD, contentObject.type());
                                builder.endObject();
                            }
                            builder.endArray();
                        }
                        case null -> {
                            // do nothing
                        }
                    }

                    builder.field(ROLE_FIELD, message.role());
                    if (message.name() != null) {
                        builder.field(NAME_FIELD, message.name());
                    }
                    if (message.toolCallId() != null) {
                        builder.field(TOOL_CALL_ID_FIELD, message.toolCallId());
                    }
                    if (message.toolCalls() != null) {
                        builder.startArray(TOOL_CALLS_FIELD);
                        for (UnifiedCompletionRequest.ToolCall toolCall : message.toolCalls()) {
                            builder.startObject();
                            {
                                builder.field(ID_FIELD, toolCall.id());
                                builder.startObject(FUNCTION_FIELD);
                                {
                                    builder.field(ARGUMENTS_FIELD, toolCall.function().arguments());
                                    builder.field(NAME_FIELD, toolCall.function().name());
                                }
                                builder.endObject();
                                builder.field(TYPE_FIELD, toolCall.type());
                            }
                            builder.endObject();
                        }
                        builder.endArray();
                    }
                }
                builder.endObject();
            }
        }
        builder.endArray();

        builder.field(MODEL_FIELD, model.getServiceSettings().modelId());
        if (unifiedRequest.maxCompletionTokens() != null) {
            builder.field(MAX_COMPLETION_TOKENS_FIELD, unifiedRequest.maxCompletionTokens());
        }

        builder.field(NUMBER_OF_RETURNED_CHOICES_FIELD, 1);

        if (unifiedRequest.stop() != null && unifiedRequest.stop().isEmpty() == false) {
            builder.field(STOP_FIELD, unifiedRequest.stop());
        }
        if (unifiedRequest.temperature() != null) {
            builder.field(TEMPERATURE_FIELD, unifiedRequest.temperature());
        }
        if (unifiedRequest.toolChoice() != null) {
            if (unifiedRequest.toolChoice() instanceof UnifiedCompletionRequest.ToolChoiceString) {
                builder.field(TOOL_CHOICE_FIELD, ((UnifiedCompletionRequest.ToolChoiceString) unifiedRequest.toolChoice()).value());
            } else if (unifiedRequest.toolChoice() instanceof UnifiedCompletionRequest.ToolChoiceObject) {
                builder.startObject(TOOL_CHOICE_FIELD);
                {
                    builder.field(TYPE_FIELD, ((UnifiedCompletionRequest.ToolChoiceObject) unifiedRequest.toolChoice()).type());
                    builder.startObject(FUNCTION_FIELD);
                    {
                        builder.field(
                            NAME_FIELD,
                            ((UnifiedCompletionRequest.ToolChoiceObject) unifiedRequest.toolChoice()).function().name()
                        );
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
        }
        if (unifiedRequest.tools() != null && unifiedRequest.tools().isEmpty() == false) {
            builder.startArray(TOOL_FIELD);
            for (UnifiedCompletionRequest.Tool t : unifiedRequest.tools()) {
                builder.startObject();
                {
                    builder.field(TYPE_FIELD, t.type());
                    builder.startObject(FUNCTION_FIELD);
                    {
                        builder.field(DESCRIPTION_FIELD, t.function().description());
                        builder.field(NAME_FIELD, t.function().name());
                        builder.field(PARAMETERS_FIELD, t.function().parameters());
                        if (t.function().strict() != null) {
                            builder.field(STRICT_FIELD, t.function().strict());
                        }
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endArray();
        }
        if (unifiedRequest.topP() != null) {
            builder.field(TOP_P_FIELD, unifiedRequest.topP());
        }

        if (Strings.isNullOrEmpty(model.getTaskSettings().user()) == false) {
            builder.field(USER_FIELD, model.getTaskSettings().user());
        }

        builder.field(STREAM_FIELD, stream);
        if (stream) {
            builder.startObject(STREAM_OPTIONS_FIELD);
            builder.field(INCLUDE_USAGE_FIELD, true);
            builder.endObject();
        }
        builder.endObject();

        return builder;
    }
}
