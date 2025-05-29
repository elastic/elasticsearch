/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.request.completion;

import software.amazon.awssdk.services.bedrockruntime.model.ConverseRequest;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseStreamRequest;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.core.common.socket.SocketAccess;
import org.elasticsearch.xpack.inference.services.amazonbedrock.client.AmazonBedrockBaseClient;
import org.elasticsearch.xpack.inference.services.amazonbedrock.completion.AmazonBedrockChatCompletionModel;
import org.elasticsearch.xpack.inference.services.amazonbedrock.request.AmazonBedrockRequest;
import org.elasticsearch.xpack.inference.services.amazonbedrock.response.completion.AmazonBedrockChatCompletionResponseListener;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.Flow;

import static org.elasticsearch.xpack.inference.services.amazonbedrock.request.completion.AmazonBedrockConverseUtils.getConverseMessageList;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.request.completion.AmazonBedrockConverseUtils.inferenceConfig;

public class AmazonBedrockChatCompletionRequest extends AmazonBedrockRequest {
    public static final String USER_ROLE = "user";
    private final AmazonBedrockConverseRequestEntity requestEntity;
    private AmazonBedrockChatCompletionResponseListener listener;
    private final boolean stream;

    public AmazonBedrockChatCompletionRequest(
        AmazonBedrockChatCompletionModel model,
        AmazonBedrockConverseRequestEntity requestEntity,
        @Nullable TimeValue timeout,
        boolean stream
    ) {
        super(model, timeout);
        this.requestEntity = Objects.requireNonNull(requestEntity);
        this.stream = stream;
    }

    @Override
    protected void executeRequest(AmazonBedrockBaseClient client) {
        var converseRequest = getConverseRequest();

        try {
            SocketAccess.doPrivileged(() -> client.converse(converseRequest, listener));
        } catch (IOException e) {
            listener.onFailure(new RuntimeException(e));
        }
    }

    @Override
    public TaskType taskType() {
        return TaskType.COMPLETION;
    }

    private ConverseRequest getConverseRequest() {
        var converseRequest = ConverseRequest.builder()
            .modelId(amazonBedrockModel.model())
            .messages(getConverseMessageList(requestEntity.messages()))
            .additionalModelResponseFieldPaths(requestEntity.additionalModelFields());

        inferenceConfig(requestEntity).ifPresent(converseRequest::inferenceConfig);

        if (requestEntity.additionalModelFields() != null) {
            converseRequest.additionalModelResponseFieldPaths(requestEntity.additionalModelFields());
        }
        return converseRequest.build();
    }

    public void executeChatCompletionRequest(
        AmazonBedrockBaseClient awsBedrockClient,
        AmazonBedrockChatCompletionResponseListener chatCompletionResponseListener
    ) {
        this.listener = chatCompletionResponseListener;
        this.executeRequest(awsBedrockClient);
    }

    public Flow.Publisher<? extends InferenceServiceResults.Result> executeStreamChatCompletionRequest(
        AmazonBedrockBaseClient awsBedrockClient
    ) {
        var converseStreamRequest = ConverseStreamRequest.builder()
            .modelId(amazonBedrockModel.model())
            .messages(getConverseMessageList(requestEntity.messages()));

        inferenceConfig(requestEntity).ifPresent(converseStreamRequest::inferenceConfig);

        if (requestEntity.additionalModelFields() != null) {
            converseStreamRequest.additionalModelResponseFieldPaths(requestEntity.additionalModelFields());
        }
        return awsBedrockClient.converseStream(converseStreamRequest.build());
    }

    @Override
    public boolean isStreaming() {
        return stream;
    }
}
