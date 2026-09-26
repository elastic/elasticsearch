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
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.googlevertexai.GoogleVertexAiUnifiedStreamingProcessor;
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
    private static final Logger logger = LogManager.getLogger(GoogleVertexAiUnifiedChatCompletionRequestEntity.class);

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
    /**
     * Google's documented placeholder that tells Gemini to skip thought signature validation. Used when a client
     * replays a function call without the signature Gemini issued for it (e.g. because the client does not yet support
     * {@code reasoning_details}), which Gemini 3 would otherwise reject with a 400.
     * <p>
     * The equivalent sentinel {@code context_engineering_is_the_way_to_go} is also accepted. Both are documented by
     * Google for this use case; this one is used by gemini-cli, LiteLLM, and pydantic-ai.
     * See <a href="https://ai.google.dev/gemini-api/docs/generate-content/thought-signatures">thought signatures</a>.
     */
    private static final String SKIP_THOUGHT_SIGNATURE_VALIDATOR = "skip_thought_signature_validator";

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

    private static String messageRoleToGoogleVertexAiSupportedRole(String messageRole) {
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
        var systemMessages = messages.stream().filter(GoogleVertexAiUnifiedChatCompletionRequestEntity::isSystemMessage).toList();

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

    private static boolean isSystemMessage(Message message) {
        return message.role().equalsIgnoreCase(SYSTEM_ROLE);
    }

    private static boolean isToolMessage(Message message) {
        return message.role().equalsIgnoreCase(TOOL_ROLE);
    }

    /**
     * Groups non-system messages into Gemini content turns, one turn per run of consecutive messages that map to the
     * same Gemini role. Gemini expects contents to alternate between {@code user} and {@code model}, and the unified
     * API does not guarantee that once {@code tool} messages become {@code user} contents: a tool result followed by
     * a user message would otherwise be two adjacent {@code user} contents, for which Gemini 2.5 returns an empty
     * answer.
     * <p>
     * This also keeps the {@code functionResponse} parts that answer a parallel function call step in a single
     * {@code user} content, as Gemini requires, whereas the unified API carries one {@code tool} message per tool
     * call. The number of {@code functionResponse} parts then matches the number of {@code functionCall} parts in the
     * preceding model turn.
     */
    private static List<List<Message>> toContentTurns(List<Message> messages) {
        var turns = new ArrayList<List<Message>>();
        String turnRole = null;
        for (var message : messages) {
            if (isSystemMessage(message)) {
                // System messages are written via systemInstruction; they do not produce a content turn.
                continue;
            }
            var role = messageRoleToGoogleVertexAiSupportedRole(message.role());
            if (role.equals(turnRole)) {
                // Append to the current turn so that the contents keep alternating between user and model.
                turns.getLast().add(message);
            } else {
                var turn = new ArrayList<Message>();
                turn.add(message);
                turns.add(turn);
                turnRole = role;
            }
        }
        return turns;
    }

    private void buildContents(XContentBuilder builder) throws IOException {
        var messages = unifiedChatInput.getRequest().messages();

        // Build a tool-call-id → function-name map once to avoid O(n·m) scanning when resolving
        // the function name for each tool message.
        var functionNameById = new HashMap<String, String>();
        for (var message : messages) {
            if (message.toolCalls() != null) {
                for (var toolCall : message.toolCalls()) {
                    if (toolCall.id() != null) {
                        functionNameById.put(toolCall.id(), toolCall.function().name());
                    }
                }
            }
        }

        builder.startArray(CONTENTS);
        for (var turn : toContentTurns(messages)) {
            builder.startObject();
            builder.field(ROLE, messageRoleToGoogleVertexAiSupportedRole(turn.getFirst().role()));
            builder.startArray(PARTS);
            {
                // A merged turn keeps its messages' parts in message order, e.g. a tool result's functionResponse
                // followed by the text of the user message after it.
                for (var message : turn) {
                    if (isToolMessage(message)) {
                        buildFunctionResponsePart(builder, message, functionNameById);
                    } else {
                        buildMessageParts(builder, message);
                    }
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
     * the trailing text part, falling back to the first function call.
     * <p>
     * When the first function call of a step has no signature at all,
     * {@link #SKIP_THOUGHT_SIGNATURE_VALIDATOR} is used so that Gemini 3 does not reject
     * the request with a 400. Real signatures always take precedence; the sentinel is only a fallback.
     */
    private void buildMessageParts(XContentBuilder builder, Message message) throws IOException {
        var texts = extractTextParts(message);
        var googleReasoningDetails = textReasoningDetails(message);
        var signaturesByToolCallId = signaturesByToolCallId(googleReasoningDetails);
        var unboundSignature = unboundSignature(googleReasoningDetails);
        var toolCalls = message.toolCalls();
        var hasToolCalls = toolCalls != null && toolCalls.isEmpty() == false;

        for (var reasoningDetail : googleReasoningDetails) {
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
            var firstCall = true;
            for (var toolCall : toolCalls) {
                var signature = signaturesByToolCallId.get(toolCall.id());
                if (signature == null && unboundSignature != null) {
                    // Google attaches the signature to the first function call of a step, so an unbound signature
                    // belongs to the first call that does not already carry one.
                    signature = unboundSignature;
                    unboundSignature = null;
                }
                if (signature == null && firstCall) {
                    // Gemini 3 requires a thought signature on the first functionCall of a step. When the client has
                    // not sent reasoning_details (e.g. because the client predates that field), use Google's sentinel
                    // so the request is not rejected with a 400.
                    logger.debug(
                        "No thought signature for first function call [{}]; using skip-validator sentinel",
                        toolCall.function().name()
                    );
                    signature = SKIP_THOUGHT_SIGNATURE_VALIDATOR;
                }
                firstCall = false;

                builder.startObject();
                {
                    builder.startObject(FUNCTION_CALL);
                    builder.field(FUNCTION_CALL_NAME, toolCall.function().name());
                    builder.field(FUNCTION_CALL_ARGS, jsonStringToMap(toolCall.function().arguments()));
                    // Only echo an id the model actually issued. When the id equals the function name it was
                    // synthesized from that name because the response carried none.
                    if (isModelIssuedId(toolCall.id(), toolCall.function().name())) {
                        builder.field(FUNCTION_CALL_ID, toolCall.id());
                    }
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
     * Emits one {@code functionResponse} part for a tool message. When multiple tool messages answer a parallel
     * function call turn they are all written inside the same {@code parts} array, with one call to this method per
     * message. Google requires the function name, which the unified tool message does not carry, so it is looked up
     * in the pre-built {@code functionNameById} map. When no entry is found the id is itself the function name
     * (the response path synthesises an id from the name when the model returns none).
     */
    private void buildFunctionResponsePart(XContentBuilder builder, Message message, Map<String, String> functionNameById)
        throws IOException {
        var toolCallId = message.toolCallId();
        if (toolCallId == null) {
            throw new ElasticsearchStatusException(
                "Tool messages require a [tool_call_id] for Google VertexAI ChatCompletion",
                RestStatus.BAD_REQUEST
            );
        }

        var functionName = functionNameById.getOrDefault(toolCallId, toolCallId);

        builder.startObject();
        {
            builder.startObject(FUNCTION_RESPONSE);
            builder.field(FUNCTION_NAME, functionName);
            // Only echo an id the model actually issued. When the id equals the resolved function name it was
            // synthesized from that name because the response carried none, so there is no real id to send back.
            if (isModelIssuedId(toolCallId, functionName)) {
                builder.field(FUNCTION_CALL_ID, toolCallId);
            }
            builder.field(FUNCTION_RESPONSE_RESPONSE, toolResponse(message));
            builder.endObject();
        }
        builder.endObject();
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

    /**
     * Returns the {@link ReasoningDetail.TextReasoningDetail} entries from the message that were produced by
     * this provider ({@code format == google-vertex-ai-v1}). Details from other providers (e.g. Anthropic) are
     * filtered out to avoid sending foreign signatures to Gemini, which would result in a 400.
     */
    private static List<ReasoningDetail.TextReasoningDetail> textReasoningDetails(Message message) {
        if (message.reasoningDetails() == null) {
            return List.of();
        }
        return message.reasoningDetails()
            .stream()
            .filter(ReasoningDetail.TextReasoningDetail.class::isInstance)
            .map(ReasoningDetail.TextReasoningDetail.class::cast)
            .filter(d -> GoogleVertexAiUnifiedStreamingProcessor.GOOGLE_VERTEX_AI_FORMAT.equals(d.format()))
            .toList();
    }

    private static Map<String, String> signaturesByToolCallId(List<ReasoningDetail.TextReasoningDetail> details) {
        var signatures = new HashMap<String, String>();
        for (var reasoningDetail : details) {
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
    private static String unboundSignature(List<ReasoningDetail.TextReasoningDetail> details) {
        for (var reasoningDetail : details) {
            if (reasoningDetail.id() == null && reasoningDetail.text() == null && reasoningDetail.signature() != null) {
                return reasoningDetail.signature();
            }
        }
        return null;
    }

    /**
     * Returns {@code true} when {@code id} is a real model-issued identifier rather than one synthesized from the
     * function name. The response parser falls back to the function name as the id when the model returns no id, so
     * an id that equals the name has no independent value and should not be echoed back.
     */
    private static boolean isModelIssuedId(@Nullable String id, String functionName) {
        return id != null && id.equals(functionName) == false;
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
     * Writes {@code generationConfig.thinkingConfig}.
     * <p>
     * Request-level reasoning with an explicit effort maps to {@code thinkingLevel}. Google rejects a request that
     * carries both {@code thinkingLevel} and {@code thinkingBudget}, so the endpoint-level budget is dropped when an
     * effort is set. When the request carries reasoning but no effort (e.g. only {@code exclude} or {@code summary}),
     * there is no {@code thinkingLevel} to conflict with, so the endpoint budget is still written.
     */
    private void buildThinkingConfig(XContentBuilder builder, @Nullable Reasoning reasoning) throws IOException {
        if (reasoning != null) {
            builder.startObject(THINKING_CONFIG);
            if (reasoning.effort() != null) {
                builder.field(THINKING_LEVEL, toThinkingLevel(reasoning.effort()));
            } else if (thinkingConfig.isEmpty() == false) {
                // No effort was specified so thinkingLevel is not written; the endpoint-level budget is still valid.
                builder.field(THINKING_BUDGET, thinkingConfig.getThinkingBudget());
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
     * Maps a unified reasoning effort onto Google's {@code ThinkingLevel} enum.
     * <p>
     * <strong>Gemini model compatibility:</strong> {@code thinkingLevel} is only supported by Gemini 3 models.
     * Gemini 2.5 and earlier accept only {@code thinkingBudget}; sending {@code thinkingLevel} to those models
     * results in a 400 from the API. When using this parameter, ensure the endpoint's model supports it.
     * <p>
     * Google has no equivalent of {@code xhigh}, and Gemini 3 cannot disable thinking, so neither is silently
     * substituted for a level the caller did not ask for.
     * <p>
     * See <a href="https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/thinking">thinking</a>.
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
