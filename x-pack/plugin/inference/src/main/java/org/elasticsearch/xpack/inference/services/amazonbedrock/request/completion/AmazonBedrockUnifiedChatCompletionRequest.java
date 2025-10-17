/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.request.completion;

import org.elasticsearch.inference.UnifiedCompletionRequest;

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
import org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResults;
import org.elasticsearch.xpack.inference.services.amazonbedrock.client.AmazonBedrockBaseClient;
import org.elasticsearch.xpack.inference.services.amazonbedrock.completion.AmazonBedrockChatCompletionModel;
import org.elasticsearch.xpack.inference.services.amazonbedrock.request.AmazonBedrockRequest;
import org.elasticsearch.xpack.inference.services.amazonbedrock.response.completion.AmazonBedrockChatCompletionResponseListener;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Flow;

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
                try {
                    converseStreamRequest.toolConfig(
                        ToolConfiguration.builder()
                            .tools(
                                Tool.builder()
                                    .toolSpec(
                                        ToolSpecification.builder()
                                            .name(tool.function().name())
                                            .description(tool.function().description())
                                            .inputSchema(ToolInputSchema
                                                .fromJson(Document.fromMap(paramToDocumentMap(tool))))
                                            .build()
                                    )
                                    .build()
                            )
                            .toolChoice(ToolChoice.builder().tool(SpecificToolChoice.builder()
                                .name(tool.function().name()).build()).build())
                            .build()
                    );
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        inferenceConfig(requestEntity).ifPresent(converseStreamRequest::inferenceConfig);
        return awsBedrockClient.converseUnifiedStream(converseStreamRequest.build());
    }

    private Document toDocument(Object value) {
        return switch (value) {
            case null -> Document.fromNull();
            case String stringValue -> Document.fromString(stringValue);
            case Integer numberValue -> Document.fromNumber(numberValue);
            case Map<?,?> mapValue -> {
                final Map<String, Document> converted = new HashMap<>();
                for (Map.Entry<?,?> entry : mapValue.entrySet()) {
                    converted.put(
                        String.valueOf(entry.getKey()),
                        toDocument(entry.getValue()));
                }
                yield Document.fromMap(converted);
            }
            default -> Document.mapBuilder().build();
        };
    }

    private Map<String, Document> paramToDocumentMap(UnifiedCompletionRequest.Tool tool) throws IOException {
        Map<String, Document> paramDocuments = new HashMap<>();
        for (Map.Entry<String, Object> entry : tool.function().parameters().entrySet()) {
            paramDocuments.put(entry.getKey(), toDocument(entry.getValue()));
        }
        return paramDocuments;
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
