/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.anthropic.request;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.completion.Content;
import org.elasticsearch.inference.completion.ContentObject;
import org.elasticsearch.inference.completion.ContentObject.ContentObjectFile;
import org.elasticsearch.inference.completion.ContentObject.ContentObjectImage;
import org.elasticsearch.inference.completion.ContentObject.ContentObjectText;
import org.elasticsearch.inference.completion.ContentObjects;
import org.elasticsearch.inference.completion.ContentString;
import org.elasticsearch.inference.completion.Message;
import org.elasticsearch.inference.completion.Tool;
import org.elasticsearch.inference.completion.ToolChoice;
import org.elasticsearch.inference.completion.ToolChoice.ToolChoiceObject;
import org.elasticsearch.inference.completion.ToolChoice.ToolChoiceString;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.CONTENT_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.DESCRIPTION_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.ID_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.MESSAGES_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.NAME_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.ROLE_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TEXT_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TOOL_CHOICE_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TOOL_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TYPE_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.URL_FIELD;

/**
 * Serializes the tool-calling portion of a unified {@code chat_completion} request into the shape expected by the
 * <a href="https://platform.claude.com/docs/en/api/messages/create">Anthropic Messages API</a>.
 *
 * <p>The unified API accepts {@code tool_choice} and {@code tools} in OpenAI's vocabulary, which differs from Anthropic's.
 * These helpers perform that translation so it is implemented in exactly one place. Both the direct Anthropic service and the
 * Google Model Garden Anthropic provider (which proxies the same Messages API) share this logic.
 */
public final class AnthropicToolUtils {

    private static final String TOOL_CHOICE_TOOL_TYPE = "tool";
    private static final String INPUT_SCHEMA_FIELD = "input_schema";
    private static final String OBJECT_TYPE = "object";
    private static final String PROPERTIES_FIELD = "properties";
    private static final String REQUIRED_FIELD = "required";

    private static final String TOOL_ROLE = "tool";
    private static final String USER_ROLE = "user";
    private static final String ASSISTANT_ROLE = "assistant";
    private static final String TEXT_TYPE = "text";
    private static final String TOOL_USE_TYPE = "tool_use";
    private static final String TOOL_RESULT_TYPE = "tool_result";
    private static final String TOOL_USE_ID_FIELD = "tool_use_id";
    private static final String INPUT_FIELD = "input";

    private static final String IMAGE_TYPE = "image";
    private static final String DOCUMENT_TYPE = "document";
    private static final String SOURCE_FIELD = "source";
    private static final String BASE64_SOURCE_TYPE = "base64";
    private static final String URL_SOURCE_TYPE = "url";
    private static final String MEDIA_TYPE_FIELD = "media_type";
    private static final String DATA_FIELD = "data";
    private static final String TITLE_FIELD = "title";
    private static final String PDF_MEDIA_TYPE = "application/pdf";
    private static final String PLAIN_TEXT_MEDIA_TYPE = "text/plain";

    private AnthropicToolUtils() {}

    /**
     * Writes the {@code messages} array, translating the unified (OpenAI-shaped) tool-calling messages into the content-block shape
     * required by the <a href="https://platform.claude.com/docs/en/api/messages/create">Anthropic Messages API</a>. Anthropic does
     * not understand OpenAI's {@code role: "tool"} messages or the {@code tool_calls} field on an assistant message, so forwarding
     * them verbatim causes a {@code 400 messages: Unexpected role "tool"} on the second turn of any tool-calling conversation.
     *
     * <p>Two message shapes are translated:
     * <ul>
     *     <li>An assistant message carrying {@code tool_calls} becomes an {@code assistant} message whose {@code content} is an array
     *         of {@code tool_use} blocks ({@code {"type":"tool_use","id":...,"name":...,"input":{...}}}). Any accompanying text
     *         content is emitted as a leading {@code text} block. The OpenAI {@code arguments} string is parsed back into the JSON
     *         object Anthropic expects under {@code input}.</li>
     *     <li>A {@code role: "tool"} message becomes a {@code user} message whose {@code content} is a single {@code tool_result}
     *         block ({@code {"type":"tool_result","tool_use_id":...,"content":[...]}}). The unified API accepts the tool message's
     *         {@code content} as either a plain string (emitted as a single {@code text} block) or an array of content objects,
     *         each translated like a user message's content objects below — {@code text}, {@code image} and {@code document} are
     *         exactly the block types Anthropic allows inside a {@code tool_result}.</li>
     *     <li>Any other message whose {@code content} is an array of content objects has each item translated into the Anthropic
     *         block shape: {@code text} objects become {@code text} blocks, OpenAI {@code image_url} objects become {@code image}
     *         blocks and {@code file} objects become {@code document} blocks, since Anthropic rejects the OpenAI shapes.</li>
     * </ul>
     *
     * <p>Every message is emitted with array-shaped {@code content}: plain-string content becomes a single {@code text} block, and
     * a user or assistant message whose content is empty or absent (and that carries no tool calls) is normalized to a single
     * empty {@code text} block, since Anthropic requires {@code content} on every message.
     * Both the direct Anthropic service and the Google Model Garden Anthropic provider share this logic so they emit identical
     * Anthropic-shaped messages.
     */
    public static void writeMessages(XContentBuilder builder, List<Message> messages) throws IOException {
        builder.startArray(MESSAGES_FIELD);
        for (var message : messages) {
            if (USER_ROLE.equals(message.role())) {
                writePlainMessage(builder, USER_ROLE, message);
            } else if (ASSISTANT_ROLE.equals(message.role())) {
                writeAssistantMessage(builder, message);
            } else if (TOOL_ROLE.equals(message.role())) {
                writeToolResult(builder, message);
            } else {
                throw new ElasticsearchStatusException(
                    Strings.format("Unsupported role [%s] for the Anthropic chat completion API.", message.role()),
                    RestStatus.BAD_REQUEST
                );
            }
        }
        builder.endArray();
    }

    /**
     * Writes a message that carries no tool calls: a user message, or an assistant message without {@code tool_calls}. Non-empty
     * plain-string content is wrapped in a single text block; content objects are translated into Anthropic blocks; empty or
     * absent content is normalized to a single empty text block since Anthropic rejects messages without content.
     */
    private static void writePlainMessage(XContentBuilder builder, String role, Message message) throws IOException {
        if (message.content() instanceof ContentString(String content) && content.isEmpty() == false) {
            writeContentObjectsMessage(builder, role, List.of(new ContentObjectText(content)));
        } else if (message.content() instanceof ContentObjects(List<ContentObject> contentObjects) && contentObjects.isEmpty() == false) {
            writeContentObjectsMessage(builder, role, contentObjects);
        } else {
            writeContentObjectsMessage(builder, role, List.of(new ContentObjectText("")));
        }
    }

    private static void writeAssistantMessage(XContentBuilder builder, Message message) throws IOException {
        var toolCalls = message.toolCalls();
        if (toolCalls == null || toolCalls.isEmpty()) {
            writePlainMessage(builder, ASSISTANT_ROLE, message);
            return;
        }

        builder.startObject();
        builder.field(ROLE_FIELD, ASSISTANT_ROLE);
        builder.startArray(CONTENT_FIELD);

        // Anthropic allows a leading text block alongside tool_use blocks; the unified assistant message often carries empty content.
        var text = extractText(message.content());
        if (text.isEmpty() == false) {
            writeTextBlock(builder, text);
        }
        for (var toolCall : toolCalls) {
            builder.startObject();
            builder.field(TYPE_FIELD, TOOL_USE_TYPE);
            builder.field(ID_FIELD, toolCall.id());
            builder.field(NAME_FIELD, toolCall.function().name());
            writeToolCallInput(builder, toolCall.function().arguments());
            builder.endObject();
        }

        builder.endArray();
        builder.endObject();
    }

    private static void writeToolResult(XContentBuilder builder, Message message) throws IOException {
        // Anthropic requires tool_use_id on every tool_result block, so a tool message without a tool_call_id cannot be translated.
        if (message.toolCallId() == null) {
            throw new ElasticsearchStatusException(
                "Field [tool_call_id] is required in a tool message for the Anthropic chat completion API.",
                RestStatus.BAD_REQUEST
            );
        }
        builder.startObject();
        builder.field(ROLE_FIELD, USER_ROLE);
        builder.startArray(CONTENT_FIELD);
        builder.startObject();
        builder.field(TYPE_FIELD, TOOL_RESULT_TYPE);
        builder.field(TOOL_USE_ID_FIELD, message.toolCallId());
        writeToolResultContent(builder, message.content());
        builder.endObject();
        builder.endArray();
        builder.endObject();
    }

    /**
     * Writes a {@code tool_result} block's {@code content}. Anthropic accepts either a plain string or an array of content blocks
     * here; the array form is always emitted, so the unified API's two tool-message content shapes serialize uniformly: a
     * {@link ContentString} becomes a single {@code text} block and each item of a {@link ContentObjects} is translated via
     * {@link #writeContentBlock} - {@code text}, {@code image} and {@code document} are exactly the block types Anthropic allows
     * inside a {@code tool_result}. A {@code null} content writes no {@code content} field, which Anthropic permits on a
     * {@code tool_result}.
     */
    private static void writeToolResultContent(XContentBuilder builder, Content content) throws IOException {
        if (content instanceof ContentString(String text)) {
            builder.startArray(CONTENT_FIELD);
            writeTextBlock(builder, text);
            builder.endArray();
        } else if (content instanceof ContentObjects(List<ContentObject> contentObjects)) {
            builder.startArray(CONTENT_FIELD);
            for (var contentObject : contentObjects) {
                writeContentBlock(builder, contentObject);
            }
            builder.endArray();
        }
    }

    private static void writeContentObjectsMessage(XContentBuilder builder, String role, List<ContentObject> contentObjects)
        throws IOException {
        builder.startObject();
        builder.field(ROLE_FIELD, role);
        builder.startArray(CONTENT_FIELD);
        for (var contentObject : contentObjects) {
            writeContentBlock(builder, contentObject);
        }
        builder.endArray();
        builder.endObject();
    }

    private static void writeContentBlock(XContentBuilder builder, ContentObject contentObject) throws IOException {
        switch (contentObject) {
            case ContentObjectText text -> writeTextBlock(builder, text.text());
            case ContentObjectImage image -> writeImageBlock(builder, image.imageUrl().url());
            case ContentObjectFile file -> writeDocumentBlock(builder, file.fileFields());
        }
    }

    private static void writeTextBlock(XContentBuilder builder, String text) throws IOException {
        builder.startObject();
        builder.field(TYPE_FIELD, TEXT_TYPE);
        builder.field(TEXT_FIELD, text);
        builder.endObject();
    }

    /**
     * Writes an Anthropic {@code image} content block. Anthropic requires an explicit media type on base64 image data, so a
     * base64 image must arrive as a data URI declaring it ({@code data:{MIME-type};base64,...}, the same contract
     * {@link InferenceString} enforces for base64 inputs) and maps onto a {@code base64} source; a plain HTTP(S) URL maps onto a
     * {@code url} source. Bare base64 without the data-URI prefix is rejected since the media type cannot be determined.
     */
    private static void writeImageBlock(XContentBuilder builder, String url) throws IOException {
        var dataUri = InferenceString.tryParseDataUri(url);
        if (dataUri == null && url.startsWith("http://") == false && url.startsWith("https://") == false) {
            throw new ElasticsearchStatusException(
                "Image URLs must be HTTP(S) URLs or base64 data URIs with the format [data:{MIME-type};base64,...] "
                    + "for the Anthropic chat completion API.",
                RestStatus.BAD_REQUEST
            );
        }
        builder.startObject();
        builder.field(TYPE_FIELD, IMAGE_TYPE);
        builder.startObject(SOURCE_FIELD);
        if (dataUri != null) {
            builder.field(TYPE_FIELD, BASE64_SOURCE_TYPE);
            builder.field(MEDIA_TYPE_FIELD, normalizeMediaType(dataUri.mediaType()));
            builder.field(DATA_FIELD, dataUri.base64Data());
        } else {
            builder.field(TYPE_FIELD, URL_SOURCE_TYPE);
            builder.field(URL_FIELD, url);
        }
        builder.endObject();
        builder.endObject();
    }

    /**
     * Writes an Anthropic {@code document} content block for a unified {@code file} content object. Anthropic requires the media
     * type, so {@code file_data} must be a data URI declaring it (the same contract {@link InferenceString} enforces for base64
     * inputs). PDFs map onto a {@code base64} source and plain text onto a {@code text} source carrying the decoded text - the
     * two document types Anthropic supports, matching the EIS gateway. The filename, when present, becomes the document's
     * {@code title}.
     */
    private static void writeDocumentBlock(XContentBuilder builder, ContentObjectFile.ContentObjectFileFields fileFields)
        throws IOException {
        if (fileFields.fileData() == null) {
            throw new ElasticsearchStatusException(
                "File content requires [file_data] for the Anthropic chat completion API.",
                RestStatus.BAD_REQUEST
            );
        }
        var dataUri = InferenceString.tryParseDataUri(fileFields.fileData());
        if (dataUri == null) {
            throw new ElasticsearchStatusException(
                "File data must be a base64 data URI with the format [data:{MIME-type};base64,...] "
                    + "for the Anthropic chat completion API.",
                RestStatus.BAD_REQUEST
            );
        }
        var mediaType = normalizeMediaType(dataUri.mediaType());
        builder.startObject();
        builder.field(TYPE_FIELD, DOCUMENT_TYPE);
        builder.startObject(SOURCE_FIELD);
        switch (mediaType) {
            case PDF_MEDIA_TYPE -> {
                builder.field(TYPE_FIELD, BASE64_SOURCE_TYPE);
                builder.field(MEDIA_TYPE_FIELD, PDF_MEDIA_TYPE);
                builder.field(DATA_FIELD, dataUri.base64Data());
            }
            case PLAIN_TEXT_MEDIA_TYPE -> {
                builder.field(TYPE_FIELD, TEXT_TYPE);
                builder.field(MEDIA_TYPE_FIELD, PLAIN_TEXT_MEDIA_TYPE);
                builder.field(DATA_FIELD, decodePlainTextData(dataUri));
            }
            default -> throw new ElasticsearchStatusException(
                Strings.format(
                    "Unsupported file media type [%s] for the Anthropic chat completion API; supported types are [%s, %s].",
                    mediaType,
                    PDF_MEDIA_TYPE,
                    PLAIN_TEXT_MEDIA_TYPE
                ),
                RestStatus.BAD_REQUEST
            );
        }
        builder.endObject();
        if (fileFields.filename() != null) {
            builder.field(TITLE_FIELD, fileFields.filename());
        }
        builder.endObject();
    }

    /** Anthropic's plain-text document source carries the decoded text, not the base64 payload. */
    private static String decodePlainTextData(InferenceString.DataUri dataUri) {
        try {
            return new String(Base64.getDecoder().decode(dataUri.base64Data()), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            throw new ElasticsearchStatusException("Invalid base64 payload in the file data URI.", RestStatus.BAD_REQUEST, e);
        }
    }

    /**
     * Normalizes a data URI's declared media type for Anthropic: RFC 2397 parameters (e.g. {@code ;charset=utf-8}) are stripped
     * and the common non-standard {@code image/jpg} alias is mapped to {@code image/jpeg}.
     */
    private static String normalizeMediaType(String mediaType) {
        var paramsStart = mediaType.indexOf(';');
        var normalized = (paramsStart >= 0 ? mediaType.substring(0, paramsStart) : mediaType).trim();
        return "image/jpg".equals(normalized) ? "image/jpeg" : normalized;
    }

    /**
     * Writes a tool call's {@code input}. OpenAI carries the tool arguments as a JSON-encoded string, whereas Anthropic expects the
     * decoded JSON object. Parses the string back into an object; a {@code null} or blank arguments string yields an empty object.
     */
    private static void writeToolCallInput(XContentBuilder builder, String arguments) throws IOException {
        builder.field(INPUT_FIELD);
        if (arguments == null || arguments.isBlank()) {
            builder.startObject().endObject();
            return;
        }
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, arguments)) {
            builder.map(parser.mapOrdered());
        }
    }

    /**
     * Writes the {@code tool_choice} field, translating OpenAI's representation into Anthropic's. Does nothing when
     * {@code toolChoice} is {@code null}.
     *
     * <ul>
     *     <li>Object {@code {"type":"function","function":{"name":"function_name"}}} becomes
     *         {@code {"type":"tool","name":"function_name"}}.</li>
     *     <li>String {@code "auto"}/{@code "none"} are passed through and {@code "required"} maps to Anthropic's {@code "any"}.</li>
     * </ul>
     *
     * @throws ElasticsearchStatusException if a string value is not one Anthropic understands.
     */
    public static void writeToolChoice(XContentBuilder builder, ToolChoice toolChoice) throws IOException {
        if (toolChoice == null) {
            return;
        }
        if (toolChoice instanceof ToolChoiceObject toolChoiceObject) {
            builder.startObject(TOOL_CHOICE_FIELD);
            builder.field(TYPE_FIELD, TOOL_CHOICE_TOOL_TYPE);
            if (toolChoiceObject.function() != null) {
                builder.field(NAME_FIELD, toolChoiceObject.function().name());
            }
            builder.endObject();
        } else if (toolChoice instanceof ToolChoiceString toolChoiceString) {
            String anthropicType = switch (toolChoiceString.value()) {
                case "none" -> "none";
                case "auto" -> "auto";
                case "required" -> "any";
                default -> throw new ElasticsearchStatusException(
                    Strings.format("Unsupported tool_choice value [%s] for the Anthropic chat completion API.", toolChoiceString.value()),
                    RestStatus.BAD_REQUEST
                );
            };
            builder.startObject(TOOL_CHOICE_FIELD);
            builder.field(TYPE_FIELD, anthropicType);
            builder.endObject();
        }
    }

    /**
     * Writes the {@code tools} array, mapping each unified tool's function definition onto Anthropic's
     * {@code name}/{@code description}/{@code input_schema} shape. Does nothing when {@code tools} is {@code null} or empty.
     *
     * <p>The OpenAI {@code strict} field has no Anthropic equivalent and is silently dropped (rather than rejected) so that
     * OpenAI-shaped requests remain drop-in compatible, for consistency with other inference services.
     */
    public static void writeTools(XContentBuilder builder, List<Tool> tools) throws IOException {
        if (tools == null || tools.isEmpty()) {
            return;
        }
        builder.startArray(TOOL_FIELD);
        for (var tool : tools) {
            var function = tool.function();
            builder.startObject();
            builder.field(NAME_FIELD, function.name());
            builder.field(DESCRIPTION_FIELD, function.description());
            writeInputSchema(builder, function.parameters());
            builder.endObject();
        }
        builder.endArray();
    }

    /**
     * Writes a tool's {@code input_schema}. Anthropic requires a JSON-Schema object on every tool, so this always emits an object
     * schema: {@code type} is forced to {@code "object"}, {@code properties} defaults to an empty object, {@code required} is
     * copied through when present, and any remaining JSON-Schema keywords are passed through unchanged. When {@code parameters}
     * is {@code null} (a parameterless tool) the minimal valid schema {@code {"type":"object","properties":{}}} is emitted.
     */
    private static void writeInputSchema(XContentBuilder builder, Map<String, Object> parameters) throws IOException {
        builder.startObject(INPUT_SCHEMA_FIELD);
        builder.field(TYPE_FIELD, OBJECT_TYPE);
        if (parameters == null) {
            builder.startObject(PROPERTIES_FIELD).endObject();
            builder.endObject();
            return;
        }
        var properties = parameters.get(PROPERTIES_FIELD);
        if (properties != null) {
            builder.field(PROPERTIES_FIELD, properties);
        } else {
            builder.startObject(PROPERTIES_FIELD).endObject();
        }
        var required = parameters.get(REQUIRED_FIELD);
        if (required != null) {
            builder.field(REQUIRED_FIELD, required);
        }
        // Preserve any other JSON-Schema keywords (e.g. additionalProperties, $defs); the incoming "type" is normalized to object.
        for (var entry : parameters.entrySet()) {
            var key = entry.getKey();
            if (TYPE_FIELD.equals(key) || PROPERTIES_FIELD.equals(key) || REQUIRED_FIELD.equals(key)) {
                continue;
            }
            builder.field(key, entry.getValue());
        }
        builder.endObject();
    }

    public static String extractText(Content content) {
        if (content instanceof ContentString(String text)) {
            return text;
        }
        if (content instanceof ContentObjects(List<ContentObject> contentObjects)) {
            var text = new StringBuilder();
            for (var contentObject : contentObjects) {
                if (contentObject instanceof ContentObject.ContentObjectText textObject) {
                    text.append(textObject.text());
                }
            }
            return text.toString();
        }
        return "";
    }

}
