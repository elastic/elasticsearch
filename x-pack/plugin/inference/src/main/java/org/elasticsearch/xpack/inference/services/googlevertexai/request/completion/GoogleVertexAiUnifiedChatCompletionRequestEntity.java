/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.request.completion;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.completion.ContentObject;
import org.elasticsearch.inference.completion.ContentObject.ContentObjectText;
import org.elasticsearch.inference.completion.ContentObjects;
import org.elasticsearch.inference.completion.ContentString;
import org.elasticsearch.inference.completion.Message;
import org.elasticsearch.inference.completion.Reasoning;
import org.elasticsearch.inference.completion.ReasoningDetail;
import org.elasticsearch.inference.completion.ToolChoice.ToolChoiceObject;
import org.elasticsearch.inference.completion.ToolChoice.ToolChoiceString;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.googlevertexai.completion.ThinkingConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.core.Strings.format;

public class GoogleVertexAiUnifiedChatCompletionRequestEntity implements ToXContentObject {
    private static final String CONTENTS = "contents";
    private static final String ROLE = "role";
    private static final String PARTS = "parts";
    private static final String TEXT = "text";
    private static final String GENERATION_CONFIG = "generationConfig";
    private static final String TEMPERATURE = "temperature";
    private static final String MAX_OUTPUT_TOKENS = "maxOutputTokens";
    private static final String TOP_P = "topP";
    private static final String THINKING_CONFIG = "thinkingConfig";
    private static final String THINKING_BUDGET = "thinkingBudget";
    private static final String THINKING_LEVEL = "thinkingLevel";
    private static final String INCLUDE_THOUGHTS = "includeThoughts";

    /**
     * Marks a part as a thought summary produced by the model rather than user-visible content.
     */
    private static final String THOUGHT = "thought";
    /**
     * The opaque, encrypted representation of the model's reasoning for a part. Gemini 3 rejects a request whose
     * function call parts are missing the signature it previously issued for them.
     */
    private static final String THOUGHT_SIGNATURE = "thoughtSignature";

    private static final String TOOLS = "tools";
    private static final String FUNCTION_DECLARATIONS = "functionDeclarations";
    private static final String FUNCTION_NAME = "name";
    private static final String FUNCTION_DESCRIPTION = "description";
    private static final String FUNCTION_PARAMETERS = "parameters";
    private static final String FUNCTION_TYPE = "function";
    private static final String TOOL_CONFIG = "toolConfig";
    private static final String FUNCTION_CALLING_CONFIG = "functionCallingConfig";
    private static final String TOOL_MODE = "mode";
    private static final String TOOL_MODE_ANY = "ANY";
    private static final String TOOL_MODE_AUTO = "auto";
    private static final String ALLOWED_FUNCTION_NAMES = "allowedFunctionNames";

    private static final String FUNCTION_CALL = "functionCall";
    private static final String FUNCTION_CALL_NAME = "name";
    private static final String FUNCTION_CALL_ARGS = "args";
    private static final String FUNCTION_CALL_ID = "id";

    private static final String FUNCTION_RESPONSE = "functionResponse";
    private static final String FUNCTION_RESPONSE_RESPONSE = "response";
    /**
     * Google treats a {@code functionResponse.response} object without an {@code output} or {@code error} key as the
     * function output itself, so a non-object tool result is wrapped under this key.
     */
    private static final String FUNCTION_RESPONSE_OUTPUT = "output";

    private static final String SUPPORTED_REASONING_EFFORTS = "minimal, low, medium, high";

    private final UnifiedChatInput unifiedChatInput;
    private final ThinkingConfig thinkingConfig;
    @Nullable
    private final Integer taskMaxTokens;

    private static final String USER_ROLE = "user";
    private static final String MODEL_ROLE = "model";
    private static final String ASSISTANT_ROLE = "assistant";
    private static final String SYSTEM_ROLE = "system";
    private static final String TOOL_ROLE = "tool";
    private static final String STOP_SEQUENCES = "stopSequences";

    private static final String SYSTEM_INSTRUCTION = "systemInstruction";

    public GoogleVertexAiUnifiedChatCompletionRequestEntity(UnifiedChatInput unifiedChatInput, ThinkingConfig thinkingConfig) {
        this(unifiedChatInput, thinkingConfig, null);
    }

    public GoogleVertexAiUnifiedChatCompletionRequestEntity(
        UnifiedChatInput unifiedChatInput,
        ThinkingConfig thinkingConfig,
        @Nullable Integer taskMaxTokens
    ) {
        this.unifiedChatInput = Objects.requireNonNull(unifiedChatInput);
        this.thinkingConfig = Objects.requireNonNull(thinkingConfig);
        this.taskMaxTokens = taskMaxTokens;
    }

    private String messageRoleToGoogleVertexAiSupportedRole(String messageRole) {
        var messageRoleLowered = messageRole.toLowerCase(Locale.ROOT);

        if (messageRoleLowered.equals(USER_ROLE)) {
            return USER_ROLE;
        } else if (messageRole.equals(ASSISTANT_ROLE)) {
            // Gemini VertexAI API does not use "assistant". Instead, it uses "model"
            return MODEL_ROLE;
        } else if (messageRole.equals(TOOL_ROLE)) {
            // Gemini VertexAI has no tool role; Content.role is only ever "user" or "model". A tool result is
            // produced by the client, so it is a user turn carrying a functionResponse part.
            return USER_ROLE;
        }

        var errorMessage = format(
            "Role [%s] not supported by Google VertexAI ChatCompletion. Supported roles: [%s, %s]",
            messageRole,
            USER_ROLE,
            ASSISTANT_ROLE
        );
        throw new ElasticsearchStatusException(errorMessage, RestStatus.BAD_REQUEST);
    }

    /**
     * Collects the text of a message into one entry per part, rejecting any non-text content. Empty strings are
     * dropped because the VertexAI API does not accept empty text parts.
     */
    private List<String> extractTextParts(Message message) {
        var texts = new ArrayList<String>();

        if (message.content() instanceof ContentString(String content)) {
            if (content.isEmpty() == false) {
                texts.add(content);
            }
        } else if (message.content() instanceof ContentObjects(List<ContentObject> objects)) {
            for (var contentObject : objects) {
                if (contentObject instanceof ContentObjectText contentObjectText) {
                    // We are only supporting Text messages for now
                    if (contentObjectText.text().isEmpty() == false) {
                        texts.add(contentObjectText.text());
                    }
                } else {
                    var errorMessage = format(
                        "Type [%s] not supported by Google VertexAI ChatCompletion. Supported types: [text]",
                        contentObject.type()
                    );
                    throw new ElasticsearchStatusException(errorMessage, RestStatus.BAD_REQUEST);
                }
            }
        }

        return texts;
    }

    private static Map<String, Object> jsonStringToMap(String jsonString) throws IOException {
        if (jsonString == null || jsonString.isEmpty()) {
            return null;
        }
        XContentParserConfiguration parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(
            LoggingDeprecationHandler.INSTANCE
        );

        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, jsonString)) {
            XContentParser.Token token = parser.nextToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
            return parser.map();
        }
    }

    private void buildSystemInstruction(XContentBuilder builder) throws IOException {
        var messages = unifiedChatInput.getRequest().messages();
        var systemMessages = messages.stream().filter(message -> message.role().equalsIgnoreCase(SYSTEM_ROLE)).toList();

        if (systemMessages.isEmpty()) {
            return;
        }

        builder.startObject(SYSTEM_INSTRUCTION);
        {
            builder.startArray(PARTS);
            for (var systemMessage : systemMessages) {
                if (systemMessage.content() instanceof ContentString contentString) {
                    if (contentString.content().isEmpty()) {
                        var errorMessage = "System message cannot be empty for Google Vertex AI";
                        throw new ElasticsearchStatusException(errorMessage, RestStatus.BAD_REQUEST);
                    }
                    builder.startObject();
                    builder.field(TEXT, contentString.content());
                    builder.endObject();
                } else if (systemMessage.content() instanceof ContentObjects contentObjects) {
                    for (var contentObject : contentObjects.contentObjects()) {
                        if (contentObject instanceof ContentObjectText contentObjectText) {
                            builder.startObject();
                            builder.field(TEXT, contentObjectText.text());
                            builder.endObject();
                        } else {
                            var errorMessage = format(
                                "Type [%s] not supported by Google VertexAI ChatCompletion. Supported types: [text]",
                                contentObject.type()
                            );
                            throw new ElasticsearchStatusException(errorMessage, RestStatus.BAD_REQUEST);
                        }
                    }
                } else {
                    var errorMessage = "Only text system instructions are supported for Vertex AI";
                    throw new ElasticsearchStatusException(errorMessage, RestStatus.BAD_REQUEST);
                }
            }
            builder.endArray();
        }
        builder.endObject();

    }

    private void buildContents(XContentBuilder builder) throws IOException {
        var messages = unifiedChatInput.getRequest().messages();

        builder.startArray(CONTENTS);
        for (Message message : messages) {
            if (message.role().equalsIgnoreCase(SYSTEM_ROLE)) {
                // System messages are built in another method
                continue;
            }

            builder.startObject();
            builder.field(ROLE, messageRoleToGoogleVertexAiSupportedRole(message.role()));
            builder.startArray(PARTS);
            {
                if (message.role().equalsIgnoreCase(TOOL_ROLE)) {
                    buildFunctionResponsePart(builder, message, messages);
                } else {
                    buildMessageParts(builder, message);
                }
            }
            builder.endArray();
            builder.endObject();
        }
        builder.endArray();
    }

    /**
     * Emits the parts of a user or model message: thought summaries first, then text, then function calls.
     * <p>
     * Thought signatures carried on the message's reasoning details are re-attached to the part they belong to. A
     * detail whose {@code id} matches a tool call binds its signature to that function call, which is what Gemini 3
     * validates. A signature with no {@code id} and no text belongs to the text of the message, so it is attached to
     * the trailing text part, falling back to the first function call. Signatures that have nowhere to land are
     * dropped: Google only validates them strictly on function calls.
     */
    private void buildMessageParts(XContentBuilder builder, Message message) throws IOException {
        var texts = extractTextParts(message);
        var signaturesByToolCallId = signaturesByToolCallId(message);
        var unboundSignature = unboundSignature(message);
        var toolCalls = message.toolCalls();
        var hasToolCalls = toolCalls != null && toolCalls.isEmpty() == false;

        for (var reasoningDetail : textReasoningDetails(message)) {
            if (reasoningDetail.text() == null) {
                continue;
            }
            builder.startObject();
            builder.field(TEXT, reasoningDetail.text());
            builder.field(THOUGHT, true);
            if (reasoningDetail.signature() != null) {
                builder.field(THOUGHT_SIGNATURE, reasoningDetail.signature());
            }
            builder.endObject();
        }

        for (int i = 0; i < texts.size(); i++) {
            builder.startObject();
            builder.field(TEXT, texts.get(i));
            if (unboundSignature != null && i == texts.size() - 1) {
                builder.field(THOUGHT_SIGNATURE, unboundSignature);
                unboundSignature = null;
            }
            builder.endObject();
        }

        if (hasToolCalls) {
            for (var toolCall : toolCalls) {
                var signature = signaturesByToolCallId.get(toolCall.id());
                if (signature == null && unboundSignature != null) {
                    // Google attaches the signature to the first function call of a step, so an unbound signature
                    // belongs to the first call that does not already carry one.
                    signature = unboundSignature;
                    unboundSignature = null;
                }

                builder.startObject();
                {
                    builder.startObject(FUNCTION_CALL);
                    builder.field(FUNCTION_CALL_NAME, toolCall.function().name());
                    builder.field(FUNCTION_CALL_ARGS, jsonStringToMap(toolCall.function().arguments()));
                    builder.endObject();
                    if (signature != null) {
                        builder.field(THOUGHT_SIGNATURE, signature);
                    }
                }
                builder.endObject();
            }
        }
    }

    /**
     * Emits the {@code functionResponse} part for a tool message. Google requires the function name, which the
     * unified tool message does not carry, so it is resolved from the tool call the message responds to.
     */
    private void buildFunctionResponsePart(XContentBuilder builder, Message message, List<Message> messages) throws IOException {
        var toolCallId = message.toolCallId();
        if (toolCallId == null) {
            throw new ElasticsearchStatusException(
                "Tool messages require a [tool_call_id] for Google VertexAI ChatCompletion",
                RestStatus.BAD_REQUEST
            );
        }

        var functionName = resolveFunctionName(toolCallId, messages);

        builder.startObject();
        {
            builder.startObject(FUNCTION_RESPONSE);
            builder.field(FUNCTION_NAME, functionName);
            // Only echo an id the model actually issued. When the id equals the resolved function name it was
            // synthesized from that name because the response carried none, so there is no real id to send back.
            if (toolCallId.equals(functionName) == false) {
                builder.field(FUNCTION_CALL_ID, toolCallId);
            }
            builder.field(FUNCTION_RESPONSE_RESPONSE, toolResponse(message));
            builder.endObject();
        }
        builder.endObject();
    }

    /**
     * Finds the name of the function the given tool call invoked. The chat completion response path falls back to the
     * function name when Google returns no function call id, so an unmatched id is itself the name.
     */
    private static String resolveFunctionName(String toolCallId, List<Message> messages) {
        for (var message : messages) {
            if (message.toolCalls() == null) {
                continue;
            }
            for (var toolCall : message.toolCalls()) {
                if (toolCallId.equals(toolCall.id())) {
                    return toolCall.function().name();
                }
            }
        }
        return toolCallId;
    }

    private Map<String, Object> toolResponse(Message message) {
        var texts = extractTextParts(message);
        if (texts.isEmpty()) {
            return Map.of();
        }

        var text = String.join("", texts);
        try {
            var parsed = jsonStringToMap(text);
            if (parsed != null) {
                return parsed;
            }
        } catch (Exception e) {
            // Not a JSON object, so it is the raw function output and is wrapped below.
        }
        return Map.of(FUNCTION_RESPONSE_OUTPUT, text);
    }

    private static List<ReasoningDetail.TextReasoningDetail> textReasoningDetails(Message message) {
        if (message.reasoningDetails() == null) {
            return List.of();
        }
        return message.reasoningDetails()
            .stream()
            .filter(ReasoningDetail.TextReasoningDetail.class::isInstance)
            .map(ReasoningDetail.TextReasoningDetail.class::cast)
            .toList();
    }

    private static Map<String, String> signaturesByToolCallId(Message message) {
        var signatures = new HashMap<String, String>();
        for (var reasoningDetail : textReasoningDetails(message)) {
            if (reasoningDetail.id() != null && reasoningDetail.signature() != null) {
                signatures.put(reasoningDetail.id(), reasoningDetail.signature());
            }
        }
        return signatures;
    }

    /**
     * The signature of a reasoning detail that names neither a tool call nor any thought text, and so has to be
     * matched to a part positionally.
     */
    @Nullable
    private static String unboundSignature(Message message) {
        for (var reasoningDetail : textReasoningDetails(message)) {
            if (reasoningDetail.id() == null && reasoningDetail.text() == null && reasoningDetail.signature() != null) {
                return reasoningDetail.signature();
            }
        }
        return null;
    }

    private void buildTools(XContentBuilder builder) throws IOException {
        var request = unifiedChatInput.getRequest();

        var tools = request.tools();
        if (tools == null || tools.isEmpty()) {
            return;
        }

        builder.startArray(TOOLS);
        {
            builder.startObject();
            builder.startArray(FUNCTION_DECLARATIONS);
            for (var tool : tools) {
                if (FUNCTION_TYPE.equals(tool.type()) == false) {
                    var errorMessage = format(
                        "Tool type [%s] not supported by Google VertexAI ChatCompletion. Supported types: [%s]",
                        tool.type(),
                        FUNCTION_TYPE
                    );
                    throw new ElasticsearchStatusException(errorMessage, RestStatus.BAD_REQUEST);
                }
                var function = tool.function();
                if (function == null) {
                    var errorMessage = format("Tool of type [%s] must have a function definition", tool.type());
                    throw new ElasticsearchStatusException(errorMessage, RestStatus.BAD_REQUEST);
                }

                builder.startObject();
                builder.field(FUNCTION_NAME, function.name());
                if (Strings.hasText(function.description())) {
                    builder.field(FUNCTION_DESCRIPTION, function.description());
                }

                if (function.parameters() != null && function.parameters().isEmpty() == false) {
                    builder.field(FUNCTION_PARAMETERS, function.parameters());
                }
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
        }
        builder.endArray();
    }

    private void buildToolConfig(XContentBuilder builder) throws IOException {
        var request = unifiedChatInput.getRequest();

        ToolChoiceObject toolChoice;
        if (request.toolChoice() instanceof ToolChoiceObject) {
            ToolChoiceObject toolChoiceObject = (ToolChoiceObject) request.toolChoice();
            toolChoice = toolChoiceObject;
        } else if (request.toolChoice() instanceof ToolChoiceString) {
            ToolChoiceString toolChoiceString = (ToolChoiceString) request.toolChoice();
            if (toolChoiceString.value().equals(TOOL_MODE_AUTO)) {
                return;
            }
            throw new ElasticsearchStatusException(
                format(
                    "Tool choice value [%s] not supported by Google VertexAI ChatCompletion. Supported values: [%s]",
                    toolChoiceString.value(),
                    TOOL_MODE_AUTO
                ),
                RestStatus.BAD_REQUEST
            );
        } else {
            return;
        }
        if (FUNCTION_TYPE.equals(toolChoice.type()) == false) {
            var errorMessage = format(
                "Tool choice type [%s] not supported by Google VertexAI ChatCompletion. Supported types: [%s]",
                toolChoice.type(),
                FUNCTION_TYPE
            );
            throw new ElasticsearchStatusException(errorMessage, RestStatus.BAD_REQUEST);
        }

        builder.startObject(TOOL_CONFIG);
        builder.startObject(FUNCTION_CALLING_CONFIG);

        var chosenFunction = toolChoice.function();
        if (chosenFunction != null) {
            // If we are using toolChoice we set the API to use the 'ANY', meaning that the model will call this tool
            // We do that since it's the only supported way right now to make compatible the OpenAi spec with VertexAI spec
            builder.field(TOOL_MODE, TOOL_MODE_ANY);
            if (Strings.hasText(chosenFunction.name())) {
                builder.startArray(ALLOWED_FUNCTION_NAMES);
                builder.value(chosenFunction.name());
                builder.endArray();
            }

            builder.endObject();
            builder.endObject();
        }
    }

    private void buildGenerationConfig(XContentBuilder builder) throws IOException {
        var request = unifiedChatInput.getRequest();

        // Fall back to the configured task-settings maxTokens when the per-request value is null,
        // so endpoint-level configuration isn't silently ignored. Use an explicit if/else (not a
        // ternary) to avoid Java unboxing both Long and Integer to a primitive, which would NPE on
        // the unused branch when one side is null.
        final Number maxOutputTokens;
        if (request.maxCompletionTokens() != null) {
            maxOutputTokens = request.maxCompletionTokens();
        } else {
            maxOutputTokens = taskMaxTokens;
        }

        boolean hasAnyConfig = request.stop() != null
            || request.temperature() != null
            || maxOutputTokens != null
            || request.topP() != null
            || request.reasoning() != null
            || thinkingConfig.isEmpty() == false;

        if (hasAnyConfig == false) {
            return;
        }

        builder.startObject(GENERATION_CONFIG);

        if (request.stop() != null) {
            builder.stringListField(STOP_SEQUENCES, request.stop());
        }
        if (request.temperature() != null) {
            builder.field(TEMPERATURE, request.temperature());
        }
        if (maxOutputTokens != null) {
            builder.field(MAX_OUTPUT_TOKENS, maxOutputTokens);
        }
        if (request.topP() != null) {
            builder.field(TOP_P, request.topP());
        }
        buildThinkingConfig(builder, request.reasoning());

        builder.endObject();
    }

    /**
     * Writes {@code generationConfig.thinkingConfig}. Request-level reasoning maps to {@code thinkingLevel}, which
     * Google rejects if combined with {@code thinkingBudget}, so the endpoint-level thinking budget only applies when
     * the request carries no reasoning of its own.
     */
    private void buildThinkingConfig(XContentBuilder builder, @Nullable Reasoning reasoning) throws IOException {
        if (reasoning != null) {
            builder.startObject(THINKING_CONFIG);
            if (reasoning.effort() != null) {
                builder.field(THINKING_LEVEL, toThinkingLevel(reasoning.effort()));
            }
            builder.field(INCLUDE_THOUGHTS, Boolean.TRUE.equals(reasoning.exclude()) == false);
            builder.endObject();
            return;
        }

        if (thinkingConfig.isEmpty() == false) {
            builder.startObject(THINKING_CONFIG);
            builder.field(THINKING_BUDGET, thinkingConfig.getThinkingBudget());
            builder.endObject();
        }
    }

    /**
     * Maps a unified reasoning effort onto Google's {@code ThinkingLevel} enum. Google has no equivalent of
     * {@code xhigh}, and Gemini 3 cannot disable thinking, so neither is silently substituted for a level the caller
     * did not ask for.
     */
    private static String toThinkingLevel(Reasoning.ReasoningEffort effort) {
        return switch (effort) {
            case MINIMAL -> "MINIMAL";
            case LOW -> "LOW";
            case MEDIUM -> "MEDIUM";
            case HIGH -> "HIGH";
            case XHIGH, NONE -> throw new ElasticsearchStatusException(
                format(
                    "Reasoning effort [%s] not supported by Google VertexAI ChatCompletion. Supported efforts: [%s]",
                    effort,
                    SUPPORTED_REASONING_EFFORTS
                ),
                RestStatus.BAD_REQUEST
            );
        };
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        buildContents(builder);
        buildGenerationConfig(builder);
        buildTools(builder);
        buildToolConfig(builder);
        buildSystemInstruction(builder);

        builder.endObject();
        return builder;
    }
}
