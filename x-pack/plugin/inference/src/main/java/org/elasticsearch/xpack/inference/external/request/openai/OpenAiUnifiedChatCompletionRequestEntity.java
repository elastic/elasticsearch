/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.openai;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.inference.action.UnifiedCompletionRequest;
import org.elasticsearch.xpack.inference.external.http.sender.DocumentsOnlyInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class OpenAiUnifiedChatCompletionRequestEntity implements ToXContentObject {

    private static final String MESSAGES_FIELD = "messages";
    private static final String MODEL_FIELD = "model";

    private static final String NUMBER_OF_RETURNED_CHOICES_FIELD = "n";

    private static final String ROLE_FIELD = "role";
    private static final String USER_FIELD = "user";
    private static final String CONTENT_FIELD = "content";
    private static final String STREAM_FIELD = "stream";
    private static final String MAX_COMPLETION_TOKENS_FIELD = "max_completion_tokens";
    private static final String STOP_FIELD = "stop";
    private static final String TEMPERATURE_FIELD = "temperature";
    private static final String TOOL_CHOICE_FIELD = "tool_choice";
    private static final String TOOL_FIELD = "tool";
    private static final String TOP_P_FIELD = "top_p";

    private final String user;

    public boolean isStream() {
        return stream;
    }

    private final boolean stream;
    private final Long maxCompletionTokens;
    private final Integer n;
    private final UnifiedCompletionRequest.Stop stop;
    private final Float temperature;
    private final UnifiedCompletionRequest.ToolChoice toolChoice;
    private final List<UnifiedCompletionRequest.Tool> tool;
    private final Float topP;
    private final List<UnifiedCompletionRequest.Message> messages;
    private final String model;

    public OpenAiUnifiedChatCompletionRequestEntity(DocumentsOnlyInput input) {
        this(convertDocumentsOnlyInputToMessages(input), null, null, null, null, null, null, null, null, null);
    }

    private static List<UnifiedCompletionRequest.Message> convertDocumentsOnlyInputToMessages(DocumentsOnlyInput input) {
        return input.getInputs()
            .stream()
            .map(doc -> new UnifiedCompletionRequest.Message(new UnifiedCompletionRequest.ContentString(doc), "user", null, null, null))
            .toList();
    }

    public OpenAiUnifiedChatCompletionRequestEntity(
        List<UnifiedCompletionRequest.Message> messages,
        @Nullable String model,
        @Nullable Long maxCompletionTokens,
        @Nullable Integer n,
        @Nullable UnifiedCompletionRequest.Stop stop,
        @Nullable Float temperature,
        @Nullable UnifiedCompletionRequest.ToolChoice toolChoice,
        @Nullable List<UnifiedCompletionRequest.Tool> tool,
        @Nullable Float topP,
        @Nullable String user
    ) {
        Objects.requireNonNull(messages);
        Objects.requireNonNull(model);

        this.user = user;
        this.stream = true; // always stream in unified API
        this.maxCompletionTokens = maxCompletionTokens;
        this.n = n;
        this.stop = stop;
        this.temperature = temperature;
        this.toolChoice = toolChoice;
        this.tool = tool;
        this.topP = topP;
        this.messages = messages;
        this.model = model;

    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray(MESSAGES_FIELD);
        {
            for (UnifiedCompletionRequest.Message message : messages) {
                builder.startObject();
                {
                    switch (message.content()) {
                        case UnifiedCompletionRequest.ContentString contentString -> builder.field(CONTENT_FIELD, contentString.content());
                        case UnifiedCompletionRequest.ContentObjects contentObjects -> {
                            for (UnifiedCompletionRequest.ContentObject contentObject : contentObjects.contentObjects()) {
                                builder.startObject(CONTENT_FIELD);
                                builder.field("text", contentObject.text());
                                builder.field("type", contentObject.type());
                                builder.endObject();
                            }
                        }
                    }

                    builder.field(ROLE_FIELD, message.role());
                    if (message.name() != null) {
                        builder.field("name", message.name());
                    }
                    if (message.toolCallId() != null) {
                        builder.field("tool_call_id", message.toolCallId());
                    }
                    if (message.toolCalls() != null) {
                        builder.startArray("tool_calls");
                        for (UnifiedCompletionRequest.ToolCall toolCall : message.toolCalls()) {
                            builder.startObject();
                            {
                                builder.field("id", toolCall.id());
                                builder.startObject("function");
                                {
                                    builder.field("arguments", toolCall.function().arguments());
                                    builder.field("name", toolCall.function().name());
                                }
                                builder.endObject();
                                builder.field("type", toolCall.type());
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

        if (model != null) {
            builder.field(MODEL_FIELD, model);
        }
        if (maxCompletionTokens != null) {
            builder.field(MAX_COMPLETION_TOKENS_FIELD, maxCompletionTokens);
        }
        if (n != null) {
            builder.field(NUMBER_OF_RETURNED_CHOICES_FIELD, n);
        }
        if (stop != null) {
            switch (stop) {
                case UnifiedCompletionRequest.StopString stopString -> builder.field(STOP_FIELD, stopString.value());
                case UnifiedCompletionRequest.StopValues stopValues -> builder.field(STOP_FIELD, stopValues.values());
            }
        }
        if (temperature != null) {
            builder.field(TEMPERATURE_FIELD, temperature);
        }
        if (toolChoice != null) {
            if (toolChoice instanceof UnifiedCompletionRequest.ToolChoiceString) {
                builder.field(TOOL_CHOICE_FIELD, ((UnifiedCompletionRequest.ToolChoiceString) toolChoice).value());
            } else if (toolChoice instanceof UnifiedCompletionRequest.ToolChoiceObject) {
                builder.startObject(TOOL_CHOICE_FIELD);
                {
                    builder.field("type", ((UnifiedCompletionRequest.ToolChoiceObject) toolChoice).type());
                    builder.startObject("function");
                    {
                        builder.field("name", ((UnifiedCompletionRequest.ToolChoiceObject) toolChoice).function().name());
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
        }
        if (tool != null) {
            builder.startArray(TOOL_FIELD);
            for (UnifiedCompletionRequest.Tool t : tool) {
                builder.startObject();
                {
                    builder.field("type", t.type());
                    builder.startObject("function");
                    {
                        builder.field("description", t.function().description());
                        builder.field("name", t.function().name());
                        builder.field("parameters", t.function().parameters());
                        builder.field("strict", t.function().strict());
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endArray();
        }
        if (topP != null) {
            builder.field(TOP_P_FIELD, topP);
        }
        if (Strings.isNullOrEmpty(user) == false) {
            builder.field(USER_FIELD, user);
        }
        builder.endObject();
        return builder;
    }
}
