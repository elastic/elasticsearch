/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.request.completion;

import org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResults;

import software.amazon.awssdk.core.document.Document;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseStreamRequest;
import software.amazon.awssdk.services.bedrockruntime.model.SpecificToolChoice;
import software.amazon.awssdk.services.bedrockruntime.model.Tool;
import software.amazon.awssdk.services.bedrockruntime.model.ToolChoice;
import software.amazon.awssdk.services.bedrockruntime.model.ToolConfiguration;
import software.amazon.awssdk.services.bedrockruntime.model.ToolInputSchema;
import software.amazon.awssdk.services.bedrockruntime.model.ToolSpecification;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.inference.services.amazonbedrock.client.AmazonBedrockBaseClient;
import org.elasticsearch.xpack.inference.services.amazonbedrock.completion.AmazonBedrockChatCompletionModel;
import org.elasticsearch.xpack.inference.services.amazonbedrock.request.AmazonBedrockRequest;
import org.elasticsearch.xpack.inference.services.amazonbedrock.response.completion.AmazonBedrockChatCompletionResponseListener;

import java.util.Objects;
import java.util.concurrent.Flow;

import static org.elasticsearch.xpack.inference.services.amazonbedrock.request.completion.AmazonBedrockConverseUtils.getInputSchema;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.request.completion.AmazonBedrockConverseUtils.getUnifiedConverseMessageList;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.request.completion.AmazonBedrockConverseUtils.inferenceConfig;

public class AmazonBedrockUnifiedChatCompletionRequest extends AmazonBedrockRequest {
    public static final String USER_ROLE = "user";
    private final AmazonBedrockUnifiedConverseRequestEntity requestEntity;
    private AmazonBedrockChatCompletionResponseListener listener;
    private final boolean stream;

    public AmazonBedrockUnifiedChatCompletionRequest(
        AmazonBedrockChatCompletionModel model,
        AmazonBedrockUnifiedConverseRequestEntity requestEntity,
        @Nullable TimeValue timeout,
        boolean stream
    ) {
        super(model, timeout);
        this.requestEntity = Objects.requireNonNull(requestEntity);
        this.stream = stream;
    }

    public Flow.Publisher<StreamingUnifiedChatCompletionResults.Results> executeStreamChatCompletionRequest(
        AmazonBedrockBaseClient awsBedrockClient
    ) {
        var converseStreamRequest = ConverseStreamRequest.builder()
            .messages(getUnifiedConverseMessageList(requestEntity.messages()))
            .modelId(amazonBedrockModel.model());

        if (requestEntity.tools() != null) {
            requestEntity.tools().forEach(tool -> {
                converseStreamRequest.toolConfig(
                    ToolConfiguration.builder()
                        .tools(
                            Tool.builder()
                                .toolSpec(
                                    ToolSpecification.builder()
                                        .name(tool.function().name())
                                        .description(tool.function().description())
                                        .inputSchema(getInputSchema())
                                        .build()
                                )
                                .build()
                        )
                        .toolChoice(ToolChoice.builder().tool(SpecificToolChoice.builder().name(tool.function().name()).build()).build())
                        .build()
                );
            });
        }

        inferenceConfig(requestEntity).ifPresent(converseStreamRequest::inferenceConfig);
        return awsBedrockClient.converseUnifiedStream(converseStreamRequest.build());
    }

    @Override
    protected void executeRequest(AmazonBedrockBaseClient client) {}

    @Override
    public TaskType taskType() {
        return TaskType.CHAT_COMPLETION;
    }

    @Override
    public boolean isStreaming() {
        return stream;
    }
}
