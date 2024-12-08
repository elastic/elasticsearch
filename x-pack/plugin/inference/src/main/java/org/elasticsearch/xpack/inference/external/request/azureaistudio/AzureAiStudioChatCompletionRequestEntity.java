/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.azureaistudio;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioEndpointType;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.azureaistudio.AzureAiStudioRequestFields.INPUT_DATA_OBJECT;
import static org.elasticsearch.xpack.inference.external.request.azureaistudio.AzureAiStudioRequestFields.INPUT_STRING_ARRAY;
import static org.elasticsearch.xpack.inference.external.request.azureaistudio.AzureAiStudioRequestFields.MESSAGES_ARRAY;
import static org.elasticsearch.xpack.inference.external.request.azureaistudio.AzureAiStudioRequestFields.MESSAGE_CONTENT;
import static org.elasticsearch.xpack.inference.external.request.azureaistudio.AzureAiStudioRequestFields.PARAMETERS_OBJECT;
import static org.elasticsearch.xpack.inference.external.request.azureaistudio.AzureAiStudioRequestFields.ROLE;
import static org.elasticsearch.xpack.inference.external.request.azureaistudio.AzureAiStudioRequestFields.STREAM;
import static org.elasticsearch.xpack.inference.external.request.azureaistudio.AzureAiStudioRequestFields.USER_ROLE;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.DO_SAMPLE_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.MAX_NEW_TOKENS_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.TEMPERATURE_FIELD;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.TOP_P_FIELD;

public record AzureAiStudioChatCompletionRequestEntity(
    List<String> messages,
    AzureAiStudioEndpointType endpointType,
    @Nullable Double temperature,
    @Nullable Double topP,
    @Nullable Boolean doSample,
    @Nullable Integer maxNewTokens,
    boolean stream
) implements ToXContentObject {

    public AzureAiStudioChatCompletionRequestEntity {
        Objects.requireNonNull(messages);
        Objects.requireNonNull(endpointType);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (endpointType == AzureAiStudioEndpointType.TOKEN) {
            createPayAsYouGoRequest(builder, params);
        } else {
            createRealtimeRequest(builder, params);
        }

        if (stream) {
            builder.field(STREAM, true);
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

        builder.endArray();

        addRequestParameters(builder);
    }

    private void addMessageContentObject(XContentBuilder builder, String message) throws IOException {
        builder.startObject();

        builder.field(MESSAGE_CONTENT, message);
        builder.field(ROLE, USER_ROLE);

        builder.endObject();
    }

    private void addRequestParameters(XContentBuilder builder) throws IOException {
        if (temperature == null && topP == null && doSample == null && maxNewTokens == null) {
            return;
        }

        builder.startObject(PARAMETERS_OBJECT);

        if (temperature != null) {
            builder.field(TEMPERATURE_FIELD, temperature);
        }

        if (topP != null) {
            builder.field(TOP_P_FIELD, topP);
        }

        if (doSample != null) {
            builder.field(DO_SAMPLE_FIELD, doSample);
        }

        if (maxNewTokens != null) {
            builder.field(MAX_NEW_TOKENS_FIELD, maxNewTokens);
        }

        builder.endObject();
    }
}
