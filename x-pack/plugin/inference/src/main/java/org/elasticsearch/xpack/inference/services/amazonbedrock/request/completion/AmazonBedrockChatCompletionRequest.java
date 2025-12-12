/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.request.completion;

import software.amazon.awssdk.core.document.Document;
import software.amazon.awssdk.services.bedrockruntime.model.AnyToolChoice;
import software.amazon.awssdk.services.bedrockruntime.model.AutoToolChoice;
import software.amazon.awssdk.services.bedrockruntime.model.ConverseStreamRequest;
import software.amazon.awssdk.services.bedrockruntime.model.SpecificToolChoice;
import software.amazon.awssdk.services.bedrockruntime.model.Tool;
import software.amazon.awssdk.services.bedrockruntime.model.ToolChoice;
import software.amazon.awssdk.services.bedrockruntime.model.ToolConfiguration;
import software.amazon.awssdk.services.bedrockruntime.model.ToolInputSchema;
import software.amazon.awssdk.services.bedrockruntime.model.ToolSpecification;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.xpack.core.inference.results.StreamingUnifiedChatCompletionResults;
import org.elasticsearch.xpack.inference.services.amazonbedrock.client.AmazonBedrockBaseClient;
import org.elasticsearch.xpack.inference.services.amazonbedrock.completion.AmazonBedrockChatCompletionModel;
import org.elasticsearch.xpack.inference.services.amazonbedrock.request.AmazonBedrockRequest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Flow;

import static org.elasticsearch.xpack.inference.services.amazonbedrock.request.completion.AmazonBedrockConverseUtils.convertChatCompletionMessagesToConverse;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.request.completion.AmazonBedrockConverseUtils.inferenceConfig;
import static org.elasticsearch.xpack.inference.services.amazonbedrock.request.completion.AmazonBedrockConverseUtils.toDocument;

public class AmazonBedrockChatCompletionRequest extends AmazonBedrockRequest {
    static final String AUTO_TOOL_CHOICE = "auto";
    static final String REQUIRED_TOOL_CHOICE = "required";
    static final String NONE_TOOL_CHOICE = "none";
    static final String FUNCTION_TYPE = "function";

    private static final Set<String> VALID_TOOL_CHOICES = Set.of(AUTO_TOOL_CHOICE, REQUIRED_TOOL_CHOICE, NONE_TOOL_CHOICE);

    private final AmazonBedrockChatCompletionRequestEntity requestEntity;
    private final boolean stream;

    public AmazonBedrockChatCompletionRequest(
        AmazonBedrockChatCompletionModel model,
        AmazonBedrockChatCompletionRequestEntity requestEntity,
        @Nullable TimeValue timeout,
        boolean stream
    ) {
        super(model, timeout);
        this.requestEntity = Objects.requireNonNull(requestEntity);
        this.stream = stream;
    }

    @Override
    protected void executeRequest(AmazonBedrockBaseClient client) {
        throw new UnsupportedOperationException("Unsupported operation, use streaming execution instead");
    }

    @Override
    public TaskType taskType() {
        return TaskType.CHAT_COMPLETION;
    }

    public Flow.Publisher<StreamingUnifiedChatCompletionResults.Results> executeStreamChatCompletionRequest(
        AmazonBedrockBaseClient awsBedrockClient
    ) {
        var toolChoice = convertToolChoice(requestEntity.tools(), requestEntity.toolChoice());

        var toolsEnabled = toolChoice != null;

        var translatedMessages = convertChatCompletionMessagesToConverse(requestEntity.messages(), toolsEnabled);
        var converseStreamRequest = ConverseStreamRequest.builder()
            .messages(translatedMessages.messages())
            .system(translatedMessages.systemContent())
            .modelId(amazonBedrockModel.model());

        if (toolsEnabled) {
            converseStreamRequest.toolConfig(
                ToolConfiguration.builder().tools(convertTools(requestEntity.tools())).toolChoice(toolChoice.build()).build()
            );
        }

        inferenceConfig(requestEntity).ifPresent(converseStreamRequest::inferenceConfig);
        return awsBedrockClient.converseUnifiedStream(converseStreamRequest.build());
    }

    private static ToolChoice.Builder convertToolChoice(
        @Nullable List<UnifiedCompletionRequest.Tool> tools,
        @Nullable UnifiedCompletionRequest.ToolChoice toolChoice
    ) {
        if (tools == null || tools.isEmpty()) {
            return null;
        }

        return determineToolChoice(toolChoice);
    }

    // default for testing
    static ToolChoice.Builder determineToolChoice(@Nullable UnifiedCompletionRequest.ToolChoice toolChoice) {
        // If a specific tool choice isn't provided, the chat completion schema (openai) defaults to "auto"
        if (toolChoice == null) {
            return ToolChoice.builder().auto(AutoToolChoice.builder().build());
        }

        return switch (toolChoice) {
            case UnifiedCompletionRequest.ToolChoiceString toolChoiceString -> switch (toolChoiceString.value()) {
                case AUTO_TOOL_CHOICE -> ToolChoice.builder().auto(AutoToolChoice.builder().build());
                case REQUIRED_TOOL_CHOICE -> ToolChoice.builder().any(AnyToolChoice.builder().build());
                case NONE_TOOL_CHOICE -> null;
                default -> throw new IllegalArgumentException(
                    Strings.format("Invalid tool choice [%s], must be one of %s", toolChoiceString.value(), VALID_TOOL_CHOICES)
                );
            };
            case UnifiedCompletionRequest.ToolChoiceObject toolChoiceObject -> ToolChoice.builder()
                .tool(SpecificToolChoice.builder().name(toolChoiceObject.function().name()).build());
        };
    }

    // default for testing
    static List<Tool> convertTools(@Nullable List<UnifiedCompletionRequest.Tool> tools) {
        if (tools == null || tools.isEmpty()) {
            return List.of();
        }

        var builtTools = new ArrayList<Tool>();

        for (var requestTool : tools) {
            if (requestTool.type().equals(FUNCTION_TYPE) == false) {
                throw new IllegalArgumentException(
                    Strings.format("Unsupported tool type [%s], only [%s] is supported", requestTool.type(), FUNCTION_TYPE)
                );
            }

            builtTools.add(
                Tool.builder()
                    .toolSpec(
                        // Bedrock does not use the strict field
                        ToolSpecification.builder()
                            .name(requestTool.function().name())
                            .description(requestTool.function().description())
                            .inputSchema(ToolInputSchema.fromJson(Document.fromMap(paramToDocumentMap(requestTool))))
                            .build()
                    )
                    .build()
            );
        }

        return builtTools;
    }

    // default for testing
    static Map<String, Document> paramToDocumentMap(UnifiedCompletionRequest.Tool tool) {
        if (tool.function().parameters() == null) {
            return Map.of();
        }

        var paramDocuments = new HashMap<String, Document>();
        for (Map.Entry<String, Object> entry : tool.function().parameters().entrySet()) {
            paramDocuments.put(entry.getKey(), toDocument(entry.getValue()));
        }

        return paramDocuments;
    }

    @Override
    public boolean isStreaming() {
        return stream;
    }
}
