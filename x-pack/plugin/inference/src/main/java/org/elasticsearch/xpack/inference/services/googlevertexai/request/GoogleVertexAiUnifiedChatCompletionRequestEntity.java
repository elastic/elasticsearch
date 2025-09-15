/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.request;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.inference.UnifiedCompletionRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;

import java.io.IOException;
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

    private final UnifiedChatInput unifiedChatInput;

    private static final String USER_ROLE = "user";
    private static final String MODEL_ROLE = "model";
    private static final String ASSISTANT_ROLE = "assistant";
    private static final String SYSTEM_ROLE = "system";
    private static final String TOOL_ROLE = "tool";
    private static final String STOP_SEQUENCES = "stopSequences";

    private static final String SYSTEM_INSTRUCTION = "systemInstruction";

    public GoogleVertexAiUnifiedChatCompletionRequestEntity(UnifiedChatInput unifiedChatInput) {
        this.unifiedChatInput = Objects.requireNonNull(unifiedChatInput);
    }

    private String messageRoleToGoogleVertexAiSupportedRole(String messageRole) {
        var messageRoleLowered = messageRole.toLowerCase(Locale.ROOT);

        if (messageRoleLowered.equals(USER_ROLE)) {
            return USER_ROLE;
        } else if (messageRole.equals(ASSISTANT_ROLE)) {
            // Gemini VertexAI API does not use "assistant". Instead, it uses "model"
            return MODEL_ROLE;
        } else if (messageRole.equals(TOOL_ROLE)) {
            // Gemini VertexAI does not have the tool role, so we map it to "model"
            return MODEL_ROLE;
        }

        var errorMessage = format(
            "Role [%s] not supported by Google VertexAI ChatCompletion. Supported roles: [%s, %s]",
            messageRole,
            USER_ROLE,
            ASSISTANT_ROLE
        );
        throw new ElasticsearchStatusException(errorMessage, RestStatus.BAD_REQUEST);
    }

    private void validateAndAddContentObjectsToBuilder(XContentBuilder builder, UnifiedCompletionRequest.ContentObjects contentObjects)
        throws IOException {

        for (var contentObject : contentObjects.contentObjects()) {
            if (contentObject.type().equals(TEXT) == false) {
                var errorMessage = format(
                    "Type [%s] not supported by Google VertexAI ChatCompletion. Supported types: [text]",
                    contentObject.type()
                );
                throw new ElasticsearchStatusException(errorMessage, RestStatus.BAD_REQUEST);
            }

            if (contentObject.text().isEmpty()) {
                return; // VertexAI API does not support empty text parts
            }

            // We are only supporting Text messages for now
            builder.startObject();
            builder.field(TEXT, contentObject.text());
            builder.endObject();
        }

    }

    private static Map<String, String> jsonStringToMap(String jsonString) throws IOException {
        if (jsonString == null || jsonString.isEmpty()) {
            return null;
        }
        XContentParserConfiguration parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(
            LoggingDeprecationHandler.INSTANCE
        );

        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, jsonString)) {
            XContentParser.Token token = parser.nextToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
            return parser.mapStrings();
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
                if (systemMessage.content() instanceof UnifiedCompletionRequest.ContentString contentString) {
                    if (contentString.content().isEmpty()) {
                        var errorMessage = "System message cannot be empty for Google Vertex AI";
                        throw new ElasticsearchStatusException(errorMessage, RestStatus.BAD_REQUEST);
                    }
                    builder.startObject();
                    builder.field(TEXT, contentString.content());
                    builder.endObject();
                } else if (systemMessage.content() instanceof UnifiedCompletionRequest.ContentObjects contentObjects) {
                    for (var contentObject : contentObjects.contentObjects()) {
                        builder.startObject();
                        builder.field(TEXT, contentObject.text());
                        builder.endObject();
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
        for (UnifiedCompletionRequest.Message message : messages) {
            if (message.role().equalsIgnoreCase(SYSTEM_ROLE)) {
                // System messages are built in another method
                continue;
            }

            builder.startObject();
            builder.field(ROLE, messageRoleToGoogleVertexAiSupportedRole(message.role()));
            builder.startArray(PARTS);
            {
                if (message.content() instanceof UnifiedCompletionRequest.ContentString) {
                    UnifiedCompletionRequest.ContentString contentString = (UnifiedCompletionRequest.ContentString) message.content();
                    // VertexAI does not support empty text parts
                    if (contentString.content().isEmpty() == false) {
                        builder.startObject();
                        builder.field(TEXT, contentString.content());
                        builder.endObject();
                    }
                } else if (message.content() instanceof UnifiedCompletionRequest.ContentObjects) {
                    UnifiedCompletionRequest.ContentObjects contentObjects = (UnifiedCompletionRequest.ContentObjects) message.content();
                    validateAndAddContentObjectsToBuilder(builder, contentObjects);
                }

                if (message.toolCalls() != null && message.toolCalls().isEmpty() == false) {
                    var toolCalls = message.toolCalls();
                    for (var toolCall : toolCalls) {
                        builder.startObject();
                        {
                            builder.startObject(FUNCTION_CALL);
                            builder.field(FUNCTION_CALL_NAME, toolCall.function().name());
                            builder.field(FUNCTION_CALL_ARGS, jsonStringToMap(toolCall.function().arguments()));
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                }
            }
            builder.endArray();
            builder.endObject();
        }
        builder.endArray();
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

        UnifiedCompletionRequest.ToolChoiceObject toolChoice;
        if (request.toolChoice() instanceof UnifiedCompletionRequest.ToolChoiceObject) {
            UnifiedCompletionRequest.ToolChoiceObject toolChoiceObject = (UnifiedCompletionRequest.ToolChoiceObject) request.toolChoice();
            toolChoice = toolChoiceObject;
        } else if (request.toolChoice() instanceof UnifiedCompletionRequest.ToolChoiceString) {
            UnifiedCompletionRequest.ToolChoiceString toolChoiceString = (UnifiedCompletionRequest.ToolChoiceString) request.toolChoice();
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

        boolean hasAnyConfig = request.stop() != null
            || request.temperature() != null
            || request.maxCompletionTokens() != null
            || request.topP() != null;

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
        if (request.maxCompletionTokens() != null) {
            builder.field(MAX_OUTPUT_TOKENS, request.maxCompletionTokens());
        }
        if (request.topP() != null) {
            builder.field(TOP_P, request.topP());
        }

        builder.endObject();
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
