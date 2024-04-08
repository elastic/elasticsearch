/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.openai;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class OpenAiChatCompletionRequestEntity implements ToXContentObject {

    private static final String MESSAGES_FIELD = "messages";
    private static final String MODEL_FIELD = "model";

    private static final String NUMBER_OF_RETURNED_CHOICES_FIELD = "n";

    private static final String ROLE_FIELD = "role";
    private static final String USER_FIELD = "user";
    private static final String CONTENT_FIELD = "content";

    private final List<String> messages;
    private final String model;

    private final String user;

    public OpenAiChatCompletionRequestEntity(List<String> messages, String model, String user) {
        Objects.requireNonNull(messages);
        Objects.requireNonNull(model);

        this.messages = messages;
        this.model = model;
        this.user = user;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray(MESSAGES_FIELD);
        {
            for (String message : messages) {
                builder.startObject();

                {
                    builder.field(ROLE_FIELD, USER_FIELD);
                    builder.field(CONTENT_FIELD, message);
                }

                builder.endObject();
            }
        }
        builder.endArray();

        builder.field(MODEL_FIELD, model);
        builder.field(NUMBER_OF_RETURNED_CHOICES_FIELD, 1);

        if (Strings.isNullOrEmpty(user) == false) {
            builder.field(USER_FIELD, user);
        }

        builder.endObject();

        return builder;
    }
}
