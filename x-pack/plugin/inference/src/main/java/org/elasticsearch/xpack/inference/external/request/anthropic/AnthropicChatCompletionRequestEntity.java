/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.anthropic;

import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.anthropic.completion.AnthropicChatCompletionServiceSettings;
import org.elasticsearch.xpack.inference.services.anthropic.completion.AnthropicChatCompletionTaskSettings;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class AnthropicChatCompletionRequestEntity implements ToXContentObject {

    private static final String MESSAGES_FIELD = "messages";
    private static final String MODEL_FIELD = "model";

    private static final String ROLE_FIELD = "role";
    private static final String USER_VALUE = "user";
    private static final String CONTENT_FIELD = "content";
    private static final String MAX_TOKENS_FIELD = "max_tokens";
    private static final String TEMPERATURE_FIELD = "temperature";
    private static final String TOP_P_FIELD = "top_p";
    private static final String TOP_K_FIELD = "top_k";
    private static final String STREAM = "stream";

    private final List<String> messages;
    private final AnthropicChatCompletionServiceSettings serviceSettings;
    private final AnthropicChatCompletionTaskSettings taskSettings;
    private final boolean stream;

    public AnthropicChatCompletionRequestEntity(
        List<String> messages,
        AnthropicChatCompletionServiceSettings serviceSettings,
        AnthropicChatCompletionTaskSettings taskSettings,
        boolean stream
    ) {
        this.messages = Objects.requireNonNull(messages);
        this.serviceSettings = Objects.requireNonNull(serviceSettings);
        this.taskSettings = Objects.requireNonNull(taskSettings);
        this.stream = stream;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.startArray(MESSAGES_FIELD);
        {
            for (String message : messages) {
                builder.startObject();

                {
                    builder.field(ROLE_FIELD, USER_VALUE);
                    builder.field(CONTENT_FIELD, message);
                }

                builder.endObject();
            }
        }
        builder.endArray();

        builder.field(MODEL_FIELD, serviceSettings.modelId());
        builder.field(MAX_TOKENS_FIELD, taskSettings.maxTokens());

        if (taskSettings.temperature() != null) {
            builder.field(TEMPERATURE_FIELD, taskSettings.temperature());
        }

        if (taskSettings.topP() != null) {
            builder.field(TOP_P_FIELD, taskSettings.topP());
        }

        if (taskSettings.topK() != null) {
            builder.field(TOP_K_FIELD, taskSettings.topK());
        }

        if (stream) {
            builder.field(STREAM, true);
        }

        builder.endObject();

        return builder;
    }
}
