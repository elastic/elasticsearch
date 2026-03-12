/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.request.completion;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.completion.AlibabaCloudSearchCompletionTaskSettings;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public record AlibabaCloudSearchCompletionRequestEntity(
    List<String> messages,
    AlibabaCloudSearchCompletionTaskSettings taskSettings,
    @Nullable String model
) implements ToXContentObject {

    private static final String MESSAGE = "messages";
    private static final String PARAMETERS = "parameters";
    private static final String ROLE_FIELD = "role";
    private static final String ROLE_USER = "user";
    private static final String ROLE_ASSISTANT = "assistant";
    private static final String CONTENT_FIELD = "content";

    public AlibabaCloudSearchCompletionRequestEntity {
        Objects.requireNonNull(messages);
        Objects.requireNonNull(taskSettings);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.startArray(MESSAGE);
            {
                for (int i = 0; i < messages.size(); i++) {
                    builder.startObject();
                    {
                        String roleValue = i % 2 == 0 ? ROLE_USER : ROLE_ASSISTANT;
                        builder.field(ROLE_FIELD, roleValue);
                        builder.field(CONTENT_FIELD, messages.get(i));
                    }
                    builder.endObject();
                }
            }
            builder.endArray();
            if (taskSettings.getParameters() != null) {
                builder.field(PARAMETERS, taskSettings.getParameters());
            }
        }
        builder.endObject();
        return builder;
    }
}
