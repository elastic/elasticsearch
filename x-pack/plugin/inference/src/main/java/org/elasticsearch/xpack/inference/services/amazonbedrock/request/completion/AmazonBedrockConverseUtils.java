/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.request.completion;

import software.amazon.awssdk.core.document.Document;
import software.amazon.awssdk.services.bedrockruntime.model.ContentBlock;
import software.amazon.awssdk.services.bedrockruntime.model.ConversationRole;
import software.amazon.awssdk.services.bedrockruntime.model.InferenceConfiguration;
import software.amazon.awssdk.services.bedrockruntime.model.Message;
import software.amazon.awssdk.services.bedrockruntime.model.SystemContentBlock;
import software.amazon.awssdk.services.bedrockruntime.model.ToolResultBlock;
import software.amazon.awssdk.services.bedrockruntime.model.ToolResultContentBlock;
import software.amazon.awssdk.services.bedrockruntime.model.ToolUseBlock;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.amazonbedrock.translation.ChatCompletionRole;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class AmazonBedrockConverseUtils {

    static final String TEXT_CONTENT_TYPE = "text";
    static final String HI_TEXT = "Hi";
    static final String PLEASE_CONTINUE_TEXT = "Please continue.";

    static final Message DEFAULT_USER_MESSAGE = Message.builder()
        .role(ConversationRole.USER)
        .content(ContentBlock.builder().text(HI_TEXT).build())
        .build();

    static final Message CONTINUE_ASSISTANT_MESSAGE = Message.builder()
        .role(ConversationRole.ASSISTANT)
        .content(ContentBlock.builder().text(PLEASE_CONTINUE_TEXT).build())
        .build();

    public static List<Message> getConverseMessageList(List<String> texts) {
        return texts.stream()
            .map(text -> ContentBlock.builder().text(text).build())
            .map(content -> Message.builder().role(ChatCompletionRole.USER.toString()).content(content).build())
            .toList();
    }

    public record TranslatedMessages(List<Message> messages, List<SystemContentBlock> systemContent) {}

    public static TranslatedMessages convertChatCompletionMessagesToConverse(
        List<UnifiedCompletionRequest.Message> messages,
        boolean toolsEnabled
    ) {
        var systemContent = new ArrayList<SystemContentBlock>();
        List<Message> convertedMessages = new ArrayList<>();

        for (var message : messages) {
            Message convertedMessage = null;
            var role = ChatCompletionRole.fromString(message.role());

            switch (role) {
                case ChatCompletionRole.SYSTEM -> systemContent.addAll(getSystemContentBlock(message.content()));
                case ChatCompletionRole.TOOL -> {
                    if (toolsEnabled) {
                        convertedMessage = convertToolResultMessage(message);
                    }
                }
                case ChatCompletionRole.ASSISTANT -> convertedMessage = convertAssistantMessage(message, toolsEnabled);
                case ChatCompletionRole.USER -> convertedMessage = convertUserMessage(message);
                default -> throw new IllegalArgumentException("Unsupported message role: " + role);
            }

            if (convertedMessage != null) {
                convertedMessages = mergeOrAddMessage(convertedMessage, convertedMessages);
            }
        }

        return new TranslatedMessages(convertedMessages, systemContent);
    }

    /**
     * Handles the logic for adding a new message.
     * 1. For consecutive messages of the same role:
     *   - If both are text messages: merges their content
     *   - If both are tool results: merges their content
     *   - If mixing tool and text: adds an assistant transition message
     *
     * We do this to avoid:
     * a. An error occurred (ValidationException) when calling the Converse operation:
     *   A conversation must alternate between user and assistant roles.
     *
     * b. An error occurred (ValidationException) when calling the Converse operation:
     *   Conversation blocks and tool result blocks cannot be provided in the same turn.
     *
     * 2. For different roles or first message: adds as new message
     */
    private static List<Message> mergeOrAddMessage(Message message, List<Message> messages) {
        if (message == null || message.content().isEmpty()) {
            return messages;
        }

        // If this is the first message and it's an assistant, prepend a default user message
        // System messages are not of concern here and tool message are also just user messages
        if (messages.isEmpty() && message.role().equals(ConversationRole.ASSISTANT)) {
            messages.add(DEFAULT_USER_MESSAGE);
        }

        // Check if we should consider merging (not first message and same role)
        if (messages.isEmpty() == false && messages.getLast().role().equals(message.role())) {
            var previousMessage = messages.getLast();

            // Detect message types to determine merging strategy
            var previousToolResult = findToolResult(previousMessage.content());
            var currentToolResult = findToolResult(message.content());

            var previousToolResultExists = previousToolResult != null;
            var currentToolResultExists = currentToolResult != null;

            // If exactly one message is a tool result (XOR), insert assistant transition
            if (previousToolResultExists != currentToolResultExists) {
                // Transitioning between tool and text messages
                // Add assistant acknowledgment to maintain conversation flow
                messages.add(CONTINUE_ASSISTANT_MESSAGE);
                messages.add(message);
            } else {
                // Both messages are the same type (both tool results, both text or mix of (assistant) *tool use* and text)
                // Safe to merge their content directly
                var previousMessageBuilder = previousMessage.toBuilder();
                var previousContent = new ArrayList<>(previousMessage.content());
                previousContent.addAll(message.content());
                var combinedMessage = previousMessageBuilder.content(previousContent).build();
                // replace last message with combined one
                messages.set(messages.size() - 1, combinedMessage);
            }
        } else {
            // Either first message or a different role so add it
            messages.add(message);
        }

        return messages;
    }

    private static ToolResultBlock findToolResult(List<ContentBlock> blocks) {
        if (blocks == null) {
            return null;
        }

        for (ContentBlock block : blocks) {
            if (block != null && block.toolResult() != null) {
                return block.toolResult();
            }
        }

        return null;
    }

    private static List<SystemContentBlock> getSystemContentBlock(UnifiedCompletionRequest.Content content) {
        return switch (content) {
            case UnifiedCompletionRequest.ContentString stringContent -> convertContentString(
                stringContent.content(),
                (text) -> SystemContentBlock.builder().text(text).build()
            );
            case UnifiedCompletionRequest.ContentObjects objectsContent -> objectsContent.contentObjects()
                .stream()
                .filter(obj -> obj.type().equals(TEXT_CONTENT_TYPE) && obj.text().isEmpty() == false)
                .map(obj -> SystemContentBlock.builder().text(obj.text()).build())
                .toList();
        };
    }

    private static Message convertToolResultMessage(UnifiedCompletionRequest.Message requestMessage) {
        // Bedrock allows empty tool result string content
        var convertedToolResultContentBlock = switch (requestMessage.content()) {
            case UnifiedCompletionRequest.ContentString stringContent -> List.of(
                ToolResultContentBlock.builder().text(stringContent.content()).build()
            );
            case UnifiedCompletionRequest.ContentObjects objectsContent -> objectsContent.contentObjects()
                .stream()
                .map(obj -> ToolResultContentBlock.builder().text(obj.text()).build())
                .toList();
        };

        if (convertedToolResultContentBlock.isEmpty()) {
            throw new IllegalArgumentException("Tool message must have non-empty contents");
        }

        return Message.builder()
            .role(ConversationRole.USER)
            .content(
                ContentBlock.builder()
                    .toolResult(
                        ToolResultBlock.builder().content(convertedToolResultContentBlock).toolUseId(requestMessage.toolCallId()).build()
                    )
                    .build()
            )
            .build();
    }

    private static Message convertAssistantMessage(UnifiedCompletionRequest.Message requestMessage, boolean toolsEnabled) {
        var blocks = convertContentToBlocks(requestMessage.content());

        // Only assistant messages can contain potential ToolCalls
        if (toolsEnabled && requestMessage.toolCalls() != null && requestMessage.toolCalls().isEmpty() == false) {
            blocks.addAll(
                requestMessage.toolCalls()
                    .stream()
                    .map(
                        toolCall -> ContentBlock.builder()
                            .toolUse(
                                ToolUseBlock.builder()
                                    .name(toolCall.function().name())
                                    .toolUseId(toolCall.id())
                                    .input(toDocument(parseFunctionArguments(toolCall.function().arguments())))
                                    .build()
                            )
                            .build()
                    )
                    .toList()
            );
        }

        return Message.builder().role(ConversationRole.ASSISTANT).content(blocks).build();
    }

    private static List<ContentBlock> convertContentToBlocks(UnifiedCompletionRequest.Content content) {
        var blocks = switch (content) {
            case UnifiedCompletionRequest.ContentString stringContent -> convertContentString(
                stringContent.content(),
                (text) -> ContentBlock.builder().text(text).build()
            );
            case UnifiedCompletionRequest.ContentObjects objectsContent -> objectsContent.contentObjects()
                .stream()
                .filter(obj -> obj.text().isEmpty() == false)
                .map(obj -> ContentBlock.builder().text(obj.text()).build())
                .toList();
        };

        return new ArrayList<>(blocks);
    }

    private static <T> List<T> convertContentString(String content, Function<String, T> converter) {
        if (content == null || content.isEmpty()) {
            return List.of();
        }

        return List.of(converter.apply(content));
    }

    private static Map<String, Object> parseFunctionArguments(String arguments) {
        if (arguments == null || arguments.isEmpty()) {
            return Map.of();
        }

        try (var p = XContentFactory.xContent(XContentType.JSON).createParser(XContentParserConfiguration.EMPTY, arguments)) {
            return p.map();
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to parse function arguments", e);
        }
    }

    public static Document toDocument(Object value) {
        return switch (value) {
            case null -> Document.fromNull();
            case String stringValue -> Document.fromString(stringValue);
            case Integer numberValue -> Document.fromNumber(numberValue);
            case List<?> values -> Document.fromList(values.stream().map(v -> {
                if (v instanceof String) {
                    return Document.fromString((String) v);
                }
                return Document.fromNull();
            }).collect(Collectors.toList()));
            case Map<?, ?> mapValue -> {
                final Map<String, Document> converted = new HashMap<>();
                for (Map.Entry<?, ?> entry : mapValue.entrySet()) {
                    converted.put(String.valueOf(entry.getKey()), toDocument(entry.getValue()));
                }
                yield Document.fromMap(converted);
            }
            default -> Document.mapBuilder().build();
        };
    }

    private static Message convertUserMessage(UnifiedCompletionRequest.Message requestMessage) {
        return Message.builder().role(ConversationRole.USER).content(convertContentToBlocks(requestMessage.content())).build();
    }

    public static Optional<InferenceConfiguration> inferenceConfig(AmazonBedrockCompletionRequestEntity request) {
        if (request.temperature() != null || request.topP() != null || request.maxTokenCount() != null) {
            var builder = InferenceConfiguration.builder();
            if (request.temperature() != null) {
                builder.temperature(request.temperature().floatValue());
            }

            if (request.topP() != null) {
                builder.topP(request.topP().floatValue());
            }

            if (request.maxTokenCount() != null) {
                builder.maxTokens(request.maxTokenCount());
            }
            return Optional.of(builder.build());
        }
        return Optional.empty();
    }

    public static Optional<InferenceConfiguration> inferenceConfig(AmazonBedrockChatCompletionRequestEntity request) {
        if (request.temperature() != null || request.topP() != null || request.maxCompletionTokens() != null) {
            var builder = InferenceConfiguration.builder();
            if (request.temperature() != null) {
                builder.temperature(request.temperature().floatValue());
            }

            if (request.topP() != null) {
                builder.topP(request.topP().floatValue());
            }

            if (request.maxCompletionTokens() != null) {
                builder.maxTokens(Math.toIntExact(request.maxCompletionTokens()));
            }

            if (request.stop() != null) {
                builder.stopSequences(request.stop());
            }
            return Optional.of(builder.build());
        }
        return Optional.empty();
    }

    @Nullable
    public static List<String> additionalTopK(@Nullable Double topK) {
        if (topK == null) {
            return null;
        }

        return List.of(Strings.format("{\"top_k\":%f}", topK.floatValue()));
    }

    private AmazonBedrockConverseUtils() {}
}
