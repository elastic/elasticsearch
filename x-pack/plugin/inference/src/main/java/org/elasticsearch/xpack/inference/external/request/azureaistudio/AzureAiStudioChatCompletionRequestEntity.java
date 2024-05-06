/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.azureaistudio;

import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioEndpointType;
import org.elasticsearch.xpack.inference.services.azureaistudio.completion.AzureAiStudioCompletionModel;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.azureaistudio.AzureAiStudioRequestFields.INPUT_DATA_OBJECT;
import static org.elasticsearch.xpack.inference.external.request.azureaistudio.AzureAiStudioRequestFields.INPUT_STRING_ARRAY;
import static org.elasticsearch.xpack.inference.external.request.azureaistudio.AzureAiStudioRequestFields.MESSAGES_ARRAY;
import static org.elasticsearch.xpack.inference.external.request.azureaistudio.AzureAiStudioRequestFields.MESSAGE_CONTENT;
import static org.elasticsearch.xpack.inference.external.request.azureaistudio.AzureAiStudioRequestFields.PARAMETERS_OBJECT;
import static org.elasticsearch.xpack.inference.external.request.azureaistudio.AzureAiStudioRequestFields.ROLE;
import static org.elasticsearch.xpack.inference.external.request.azureaistudio.AzureAiStudioRequestFields.USER_ROLE;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.DO_SAMPLE_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.MAX_NEW_TOKENS_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.TEMPERATURE_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.TOP_P_FIELD;

public class AzureAiStudioChatCompletionRequestEntity implements ToXContentObject {

    private final List<String> messages;
    private final AzureAiStudioCompletionModel model;

    public AzureAiStudioChatCompletionRequestEntity(AzureAiStudioCompletionModel model, List<String> messages) {
        this.model = Objects.requireNonNull(model);
        this.messages = Objects.requireNonNull(messages);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (model.getServiceSettings().endpointType() == AzureAiStudioEndpointType.TOKEN) {
            createPayAsYouGoRequest(builder, params);
        } else {
            createRealtimeRequest(builder, params);
        }

        builder.endObject();
        return builder;
    }

    private void createRealtimeRequest(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(INPUT_DATA_OBJECT);
        builder.startArray(INPUT_STRING_ARRAY);

        for (String message : messages) {
            addMessageContentObject(builder, message);
        }

        builder.endArray();

        addRequestParameters(builder);

        builder.endObject();
    }

    private void createPayAsYouGoRequest(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(MESSAGES_ARRAY);

        for (String message : messages) {
            addMessageContentObject(builder, message);
        }

        addRequestParameters(builder);

        builder.endArray();
    }

    private void addMessageContentObject(XContentBuilder builder, String message) throws IOException {
        builder.startObject();

        builder.field(MESSAGE_CONTENT, message);
        builder.field(ROLE, USER_ROLE);

        builder.endObject();
    }

    private void addRequestParameters(XContentBuilder builder) throws IOException {
        var taskSettings = model.getTaskSettings();
        if (taskSettings.areAnyParametersAvailable() == false) {
            return;
        }

        builder.startObject(PARAMETERS_OBJECT);

        if (taskSettings.temperature() != null) {
            builder.field(TEMPERATURE_FIELD, taskSettings.temperature());
        }

        if (taskSettings.topP() != null) {
            builder.field(TOP_P_FIELD, taskSettings.topP());
        }

        if (taskSettings.doSample() != null) {
            builder.field(DO_SAMPLE_FIELD, taskSettings.doSample());
        }

        if (taskSettings.maxTokens() != null) {
            builder.field(MAX_NEW_TOKENS_FIELD, taskSettings.maxTokens());
        }

        builder.endObject();
    }
}
