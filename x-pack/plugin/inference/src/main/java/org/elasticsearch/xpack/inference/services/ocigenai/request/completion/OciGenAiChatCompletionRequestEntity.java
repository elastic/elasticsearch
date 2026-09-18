/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.request.completion;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.UnifiedCompletionRequestBody;
import org.elasticsearch.inference.completion.Content;
import org.elasticsearch.inference.completion.ContentObject;
import org.elasticsearch.inference.completion.ContentObjects;
import org.elasticsearch.inference.completion.ContentString;
import org.elasticsearch.inference.completion.Message;
import org.elasticsearch.inference.completion.ToolChoice;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiChatApiFormat;
import org.elasticsearch.xpack.inference.services.ocigenai.completion.OciGenAiChatCompletionModel;
import org.elasticsearch.xpack.inference.services.ocigenai.request.OciGenAiRequestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;

/**
 * Body of an OCI Generative AI {@code chat} request, translated from the unified (OpenAI compatible) chat completion request.
 * <p>
 * Cohere Command models use the {@code COHERE} API format, which expects the latest user message in {@code message} and the
 * preceding conversation in {@code chatHistory}; tool calling is not translated for this format. Every other model uses the
 * {@code GENERIC} API format, which mirrors the OpenAI message structure and supports text and image content, function tools and
 * tool choice.
 *
 * @see <a href="https://docs.oracle.com/en-us/iaas/api/#/en/generative-ai-inference/20231130/datatypes/GenericChatRequest">GenericChatRequest</a>
 * @see <a href="https://docs.oracle.com/en-us/iaas/api/#/en/generative-ai-inference/20231130/datatypes/CohereChatRequest">CohereChatRequest</a>
 */
public class OciGenAiChatCompletionRequestEntity implements ToXContentObject {

    static final String CHAT_REQUEST_FIELD = "chatRequest";
    static final String API_FORMAT_FIELD = "apiFormat";
    static final String IS_STREAM_FIELD = "isStream";
    static final String MAX_TOKENS_FIELD = "maxTokens";
    static final String TEMPERATURE_FIELD = "temperature";
    static final String TOP_P_FIELD = "topP";

    // GENERIC format
    static final String MESSAGES_FIELD = "messages";
    static final String ROLE_FIELD = "role";
    static final String CONTENT_FIELD = "content";
    static final String TYPE_FIELD = "type";
    static final String TEXT_FIELD = "text";
    static final String IMAGE_URL_FIELD = "imageUrl";
    static final String URL_FIELD = "url";
    static final String DETAIL_FIELD = "detail";
    static final String TOOL_CALL_ID_FIELD = "toolCallId";
    static final String TOOL_CALLS_FIELD = "toolCalls";
    static final String ID_FIELD = "id";
    static final String NAME_FIELD = "name";
    static final String ARGUMENTS_FIELD = "arguments";
    static final String DESCRIPTION_FIELD = "description";
    static final String PARAMETERS_FIELD = "parameters";
    static final String STOP_FIELD = "stop";
    static final String TOOLS_FIELD = "tools";
    static final String TOOL_CHOICE_FIELD = "toolChoice";
    static final String TEXT_CONTENT_TYPE = "TEXT";
    static final String IMAGE_CONTENT_TYPE = "IMAGE";
    static final String FUNCTION_TYPE = "FUNCTION";

    // COHERE format
    static final String MESSAGE_FIELD = "message";
    static final String CHAT_HISTORY_FIELD = "chatHistory";
    static final String STOP_SEQUENCES_FIELD = "stopSequences";

    static final String USER_ROLE = "USER";
    static final String ASSISTANT_ROLE = "ASSISTANT";
    static final String SYSTEM_ROLE = "SYSTEM";
    static final String TOOL_ROLE = "TOOL";
    static final String CHATBOT_ROLE = "CHATBOT";

    private static final String OPENAI_USER_ROLE = "user";
    private static final String OPENAI_ASSISTANT_ROLE = "assistant";
    private static final String OPENAI_SYSTEM_ROLE = "system";
    private static final String OPENAI_DEVELOPER_ROLE = "developer";
    private static final String OPENAI_TOOL_ROLE = "tool";
    private static final String OPENAI_FUNCTION_TYPE = "function";
    private static final String OPENAI_TOOL_CHOICE_AUTO = "auto";
    private static final String OPENAI_TOOL_CHOICE_NONE = "none";
    private static final String OPENAI_TOOL_CHOICE_REQUIRED = "required";

    private final UnifiedCompletionRequestBody request;
    private final boolean stream;
    private final OciGenAiChatCompletionModel model;
    private final OciGenAiChatApiFormat apiFormat;

    public OciGenAiChatCompletionRequestEntity(UnifiedChatInput chatInput, OciGenAiChatCompletionModel model) {
        Objects.requireNonNull(chatInput);
        this.request = chatInput.getRequest();
        this.stream = chatInput.stream();
        this.model = Objects.requireNonNull(model);
        this.apiFormat = model.getServiceSettings().apiFormat();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        OciGenAiRequestUtils.writeCompartmentAndServingMode(builder, model.getServiceSettings());

        builder.startObject(CHAT_REQUEST_FIELD);
        builder.field(API_FORMAT_FIELD, apiFormat.name());
        builder.field(IS_STREAM_FIELD, stream);
        switch (apiFormat) {
            case GENERIC -> writeGenericRequest(builder);
            case COHERE -> writeCohereRequest(builder);
        }
        builder.endObject();

        builder.endObject();
        return builder;
    }

    // ---------------------------------------------------------------------------------------------------------------------------
    // GENERIC API format
    // ---------------------------------------------------------------------------------------------------------------------------

    private void writeGenericRequest(XContentBuilder builder) throws IOException {
        builder.startArray(MESSAGES_FIELD);
        for (var message : request.messages()) {
            writeGenericMessage(builder, message);
        }
        builder.endArray();

        writeSamplingParameters(builder, STOP_FIELD);
        writeGenericTools(builder);
        writeGenericToolChoice(builder);
    }

    private void writeGenericMessage(XContentBuilder builder, Message message) throws IOException {
        var role = toGenericRole(message.role());

        builder.startObject();
        builder.field(ROLE_FIELD, role);

        if (TOOL_ROLE.equals(role) && message.toolCallId() != null) {
            builder.field(TOOL_CALL_ID_FIELD, message.toolCallId());
        }

        if (message.content() != null) {
            var contentItems = toGenericContentItems(message.content());
            if (contentItems.isEmpty() == false) {
                builder.startArray(CONTENT_FIELD);
                for (var item : contentItems) {
                    item.toXContent(builder, ToXContentObject.EMPTY_PARAMS);
                }
                builder.endArray();
            }
        }

        if (message.toolCalls() != null && message.toolCalls().isEmpty() == false) {
            builder.startArray(TOOL_CALLS_FIELD);
            for (var toolCall : message.toolCalls()) {
                builder.startObject();
                builder.field(TYPE_FIELD, FUNCTION_TYPE);
                builder.field(ID_FIELD, toolCall.id());
                builder.field(NAME_FIELD, toolCall.function().name());
                builder.field(ARGUMENTS_FIELD, toolCall.function().arguments());
                builder.endObject();
            }
            builder.endArray();
        }

        builder.endObject();
    }

    private static String toGenericRole(String role) {
        return switch (role.toLowerCase(Locale.ROOT)) {
            case OPENAI_USER_ROLE -> USER_ROLE;
            case OPENAI_ASSISTANT_ROLE -> ASSISTANT_ROLE;
            case OPENAI_SYSTEM_ROLE, OPENAI_DEVELOPER_ROLE -> SYSTEM_ROLE;
            case OPENAI_TOOL_ROLE -> TOOL_ROLE;
            default -> throw new ElasticsearchStatusException(
                format(
                    "Role [%s] is not supported by the OCI Generative AI chat completion. Supported roles: [%s, %s, %s, %s]",
                    role,
                    OPENAI_USER_ROLE,
                    OPENAI_ASSISTANT_ROLE,
                    OPENAI_SYSTEM_ROLE,
                    OPENAI_TOOL_ROLE
                ),
                RestStatus.BAD_REQUEST
            );
        };
    }

    private static List<ToXContentObject> toGenericContentItems(Content content) {
        var items = new ArrayList<ToXContentObject>();
        if (content instanceof ContentString contentString) {
            if (contentString.content().isEmpty() == false) {
                items.add(textContentItem(contentString.content()));
            }
        } else if (content instanceof ContentObjects contentObjects) {
            for (var contentObject : contentObjects.contentObjects()) {
                if (contentObject instanceof ContentObject.ContentObjectText text) {
                    if (text.text().isEmpty() == false) {
                        items.add(textContentItem(text.text()));
                    }
                } else if (contentObject instanceof ContentObject.ContentObjectImage image) {
                    items.add((builder, params) -> {
                        builder.startObject();
                        builder.field(TYPE_FIELD, IMAGE_CONTENT_TYPE);
                        builder.startObject(IMAGE_URL_FIELD);
                        builder.field(URL_FIELD, image.imageUrl().url());
                        if (image.imageUrl().detail() != null) {
                            builder.field(DETAIL_FIELD, image.imageUrl().detail().toString().toUpperCase(Locale.ROOT));
                        }
                        builder.endObject();
                        builder.endObject();
                        return builder;
                    });
                } else {
                    throw new ElasticsearchStatusException(
                        format(
                            "Content type [%s] is not supported by the OCI Generative AI chat completion. "
                                + "Supported types: [text, image_url]",
                            contentObject.type()
                        ),
                        RestStatus.BAD_REQUEST
                    );
                }
            }
        }
        return items;
    }

    private static ToXContentObject textContentItem(String text) {
        return (builder, params) -> {
            builder.startObject();
            builder.field(TYPE_FIELD, TEXT_CONTENT_TYPE);
            builder.field(TEXT_FIELD, text);
            builder.endObject();
            return builder;
        };
    }

    private void writeGenericTools(XContentBuilder builder) throws IOException {
        var tools = request.tools();
        if (tools == null || tools.isEmpty()) {
            return;
        }

        builder.startArray(TOOLS_FIELD);
        for (var tool : tools) {
            if (OPENAI_FUNCTION_TYPE.equals(tool.type()) == false || tool.function() == null) {
                throw new ElasticsearchStatusException(
                    format(
                        "Tool type [%s] is not supported by the OCI Generative AI chat completion. Supported types: [%s]",
                        tool.type(),
                        OPENAI_FUNCTION_TYPE
                    ),
                    RestStatus.BAD_REQUEST
                );
            }
            builder.startObject();
            builder.field(TYPE_FIELD, FUNCTION_TYPE);
            builder.field(NAME_FIELD, tool.function().name());
            if (tool.function().description() != null) {
                builder.field(DESCRIPTION_FIELD, tool.function().description());
            }
            if (tool.function().parameters() != null) {
                builder.field(PARAMETERS_FIELD, tool.function().parameters());
            }
            builder.endObject();
        }
        builder.endArray();
    }

    private void writeGenericToolChoice(XContentBuilder builder) throws IOException {
        var toolChoice = request.toolChoice();
        if (toolChoice == null) {
            return;
        }

        builder.startObject(TOOL_CHOICE_FIELD);
        if (toolChoice instanceof ToolChoice.ToolChoiceString toolChoiceString) {
            var type = switch (toolChoiceString.value().toLowerCase(Locale.ROOT)) {
                case OPENAI_TOOL_CHOICE_AUTO -> "AUTO";
                case OPENAI_TOOL_CHOICE_NONE -> "NONE";
                case OPENAI_TOOL_CHOICE_REQUIRED -> "REQUIRED";
                default -> throw new ElasticsearchStatusException(
                    format(
                        "Tool choice [%s] is not supported by the OCI Generative AI chat completion. Supported values: [%s, %s, %s]",
                        toolChoiceString.value(),
                        OPENAI_TOOL_CHOICE_AUTO,
                        OPENAI_TOOL_CHOICE_NONE,
                        OPENAI_TOOL_CHOICE_REQUIRED
                    ),
                    RestStatus.BAD_REQUEST
                );
            };
            builder.field(TYPE_FIELD, type);
        } else if (toolChoice instanceof ToolChoice.ToolChoiceObject toolChoiceObject) {
            if (OPENAI_FUNCTION_TYPE.equals(toolChoiceObject.type()) == false || toolChoiceObject.function() == null) {
                throw new ElasticsearchStatusException(
                    format(
                        "Tool choice type [%s] is not supported by the OCI Generative AI chat completion. Supported types: [%s]",
                        toolChoiceObject.type(),
                        OPENAI_FUNCTION_TYPE
                    ),
                    RestStatus.BAD_REQUEST
                );
            }
            builder.field(TYPE_FIELD, FUNCTION_TYPE);
            builder.field(NAME_FIELD, toolChoiceObject.function().name());
        }
        builder.endObject();
    }

    // ---------------------------------------------------------------------------------------------------------------------------
    // COHERE API format
    // ---------------------------------------------------------------------------------------------------------------------------

    private void writeCohereRequest(XContentBuilder builder) throws IOException {
        if ((request.tools() != null && request.tools().isEmpty() == false) || request.toolChoice() != null) {
            throw new ElasticsearchStatusException(
                "Tools are not supported by the OCI Generative AI chat completion for Cohere models",
                RestStatus.BAD_REQUEST
            );
        }

        var messages = request.messages();
        if (messages.isEmpty()) {
            throw new ElasticsearchStatusException("At least one message is required", RestStatus.BAD_REQUEST);
        }

        var lastMessage = messages.getLast();
        if (OPENAI_USER_ROLE.equalsIgnoreCase(lastMessage.role()) == false) {
            throw new ElasticsearchStatusException(
                format(
                    "The last message must have the [%s] role for the OCI Generative AI chat completion with Cohere models, found [%s]",
                    OPENAI_USER_ROLE,
                    lastMessage.role()
                ),
                RestStatus.BAD_REQUEST
            );
        }
        builder.field(MESSAGE_FIELD, toText(lastMessage.content()));

        if (messages.size() > 1) {
            builder.startArray(CHAT_HISTORY_FIELD);
            for (var message : messages.subList(0, messages.size() - 1)) {
                builder.startObject();
                builder.field(ROLE_FIELD, toCohereRole(message.role()));
                builder.field(MESSAGE_FIELD, toText(message.content()));
                builder.endObject();
            }
            builder.endArray();
        }

        writeSamplingParameters(builder, STOP_SEQUENCES_FIELD);
    }

    private static String toCohereRole(String role) {
        return switch (role.toLowerCase(Locale.ROOT)) {
            case OPENAI_USER_ROLE -> USER_ROLE;
            case OPENAI_ASSISTANT_ROLE -> CHATBOT_ROLE;
            case OPENAI_SYSTEM_ROLE, OPENAI_DEVELOPER_ROLE -> SYSTEM_ROLE;
            default -> throw new ElasticsearchStatusException(
                format(
                    "Role [%s] is not supported by the OCI Generative AI chat completion for Cohere models. Supported roles: [%s, %s, %s]",
                    role,
                    OPENAI_USER_ROLE,
                    OPENAI_ASSISTANT_ROLE,
                    OPENAI_SYSTEM_ROLE
                ),
                RestStatus.BAD_REQUEST
            );
        };
    }

    private static String toText(@Nullable Content content) {
        if (content instanceof ContentString contentString) {
            return contentString.content();
        } else if (content instanceof ContentObjects contentObjects) {
            return contentObjects.contentObjects().stream().map(contentObject -> {
                if (contentObject instanceof ContentObject.ContentObjectText text) {
                    return text.text();
                }
                throw new ElasticsearchStatusException(
                    format(
                        "Content type [%s] is not supported by the OCI Generative AI chat completion for Cohere models. "
                            + "Supported types: [text]",
                        contentObject.type()
                    ),
                    RestStatus.BAD_REQUEST
                );
            }).collect(Collectors.joining("\n"));
        }
        return "";
    }

    // ---------------------------------------------------------------------------------------------------------------------------
    // shared
    // ---------------------------------------------------------------------------------------------------------------------------

    private void writeSamplingParameters(XContentBuilder builder, String stopFieldName) throws IOException {
        if (request.maxCompletionTokens() != null) {
            builder.field(MAX_TOKENS_FIELD, request.maxCompletionTokens());
        }
        if (request.temperature() != null) {
            builder.field(TEMPERATURE_FIELD, request.temperature());
        }
        if (request.topP() != null) {
            builder.field(TOP_P_FIELD, request.topP());
        }
        if (request.stop() != null && request.stop().isEmpty() == false) {
            builder.stringListField(stopFieldName, request.stop());
        }
    }
}
