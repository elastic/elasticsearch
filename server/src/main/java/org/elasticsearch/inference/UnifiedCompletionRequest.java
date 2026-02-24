/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.inference.UnifiedCompletionRequest.ContentObject.ContentObjectType.FILE;
import static org.elasticsearch.inference.UnifiedCompletionRequest.ContentObject.ContentObjectType.IMAGE_URL;
import static org.elasticsearch.inference.UnifiedCompletionRequest.ContentObject.ContentObjectType.TEXT;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public record UnifiedCompletionRequest(
    List<Message> messages,
    @Nullable String model,
    @Nullable Long maxCompletionTokens,
    @Nullable List<String> stop,
    @Nullable Float temperature,
    @Nullable ToolChoice toolChoice,
    @Nullable List<Tool> tools,
    @Nullable Float topP
) implements Writeable, ToXContentFragment {

    public static final String NAME_FIELD = "name";
    public static final String TOOL_CALL_ID_FIELD = "tool_call_id";
    public static final String TOOL_CALLS_FIELD = "tool_calls";
    public static final String ID_FIELD = "id";
    public static final String FUNCTION_FIELD = "function";
    public static final String ARGUMENTS_FIELD = "arguments";
    public static final String DESCRIPTION_FIELD = "description";
    public static final String PARAMETERS_FIELD = "parameters";
    public static final String STRICT_FIELD = "strict";
    public static final String TOP_P_FIELD = "top_p";
    public static final String MESSAGES_FIELD = "messages";
    private static final String ROLE_FIELD = "role";
    private static final String CONTENT_FIELD = "content";
    private static final String STOP_FIELD = "stop";
    public static final String TEMPERATURE_FIELD = "temperature";
    public static final String TOOL_CHOICE_FIELD = "tool_choice";
    public static final String TOOL_FIELD = "tools";
    private static final String TEXT_FIELD = "text";
    private static final String IMAGE_URL_FIELD = "image_url";
    private static final String URL_FIELD = "url";
    private static final String DETAIL_FIELD = "detail";
    private static final String FILE_FIELD = "file";
    private static final String FILE_DATA_FIELD = "file_data";
    private static final String FILE_ID_FIELD = "file_id";
    private static final String FILENAME_FIELD = "filename";
    public static final String TYPE_FIELD = "type";
    private static final String MODEL_FIELD = "model";
    private static final String MAX_COMPLETION_TOKENS_FIELD = "max_completion_tokens";
    private static final String MAX_TOKENS_FIELD = "max_tokens";

    public static TransportVersion MULTIMODAL_CHAT_COMPLETION_SUPPORT_ADDED = TransportVersion.fromName(
        "inference_api_multimodal_chat_completion"
    );

    /**
     * We currently allow providers to override the model id that is written to JSON.
     * Rather than use {@link #model()}, providers are expected to pass in the modelId via
     * {@link Params}.
     */
    private static final String MODEL_ID_PARAM = "model_id_value";
    /**
     * Some providers only support the now-deprecated {@link #MAX_TOKENS_FIELD}, others have migrated to
     * {@link #MAX_COMPLETION_TOKENS_FIELD}. Providers are expected to pass in their supported field name.
     */
    private static final String MAX_TOKENS_PARAM = "max_tokens_field";
    /**
     * Indicates whether to include the `stream_options` field in the JSON output.
     * Some providers do not support this field. In such cases, this parameter should be set to "false",
     * and the `stream_options` field will be excluded from the output.
     * For providers that do support stream options, this parameter is left unset (default behavior),
     * which implicitly includes the `stream_options` field in the output.
     */
    public static final String INCLUDE_STREAM_OPTIONS_PARAM = "include_stream_options";

    /**
     * Creates a {@link Params} that causes ToXContent to include the key values:
     * - Key: {@link #MODEL_FIELD}, Value: modelId, if modelId is not null
     * - Key: {@link #MAX_TOKENS_FIELD}, Value: {@link #maxCompletionTokens()}
     */
    public static Params withMaxTokens(@Nullable String modelId, Params params) {
        Map<String, String> entries = modelId != null
            ? Map.ofEntries(Map.entry(MODEL_ID_PARAM, modelId), Map.entry(MAX_TOKENS_PARAM, MAX_TOKENS_FIELD))
            : Map.ofEntries(Map.entry(MAX_TOKENS_PARAM, MAX_TOKENS_FIELD));
        return new DelegatingMapParams(entries, params);
    }

    /**
     * Creates a {@link Params} that causes ToXContent to include the key values:
     * - Key: {@link #MODEL_FIELD}, Value: modelId, if modelId is not null
     * - Key: {@link #MAX_TOKENS_FIELD}, Value: {@link #maxCompletionTokens()}
     * - Key: {@link #INCLUDE_STREAM_OPTIONS_PARAM}, Value: "false"
     */
    public static Params withMaxTokensAndSkipStreamOptionsField(@Nullable String modelId, Params params) {
        Map<String, String> entries = modelId != null
            ? Map.ofEntries(
                Map.entry(MODEL_ID_PARAM, modelId),
                Map.entry(MAX_TOKENS_PARAM, MAX_TOKENS_FIELD),
                Map.entry(INCLUDE_STREAM_OPTIONS_PARAM, Boolean.FALSE.toString())
            )
            : Map.ofEntries(
                Map.entry(MAX_TOKENS_PARAM, MAX_TOKENS_FIELD),
                Map.entry(INCLUDE_STREAM_OPTIONS_PARAM, Boolean.FALSE.toString())
            );
        return new DelegatingMapParams(entries, params);
    }

    /**
     * Creates a {@link Params} that causes ToXContent to include the key values:
     * - Key: {@link #MODEL_FIELD}, Value: modelId
     * - Key: {@link #MAX_COMPLETION_TOKENS_FIELD}, Value: {@link #maxCompletionTokens()}
     */
    public static Params withMaxCompletionTokens(String modelId, Params params) {
        return new DelegatingMapParams(
            Map.ofEntries(Map.entry(MODEL_ID_PARAM, modelId), Map.entry(MAX_TOKENS_PARAM, MAX_COMPLETION_TOKENS_FIELD)),
            params
        );
    }

    /**
     * Creates a {@link Params} that causes ToXContent to include the key values:
     * - Key: {@link #MAX_COMPLETION_TOKENS_FIELD}, Value: {@link #maxCompletionTokens()}
     */
    public static Params withMaxCompletionTokens(Params params) {
        return new DelegatingMapParams(Map.of(MAX_TOKENS_PARAM, MAX_COMPLETION_TOKENS_FIELD), params);
    }

    public sealed interface Content extends NamedWriteable, ToXContent permits ContentObjects, ContentString {
        boolean containsMultimodalContent();
    }

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<UnifiedCompletionRequest, Void> PARSER = new ConstructingObjectParser<>(
        UnifiedCompletionRequest.class.getSimpleName(),
        args -> new UnifiedCompletionRequest(
            (List<Message>) args[0],
            (String) args[1],
            (Long) args[2],
            (List<String>) args[3],
            (Float) args[4],
            (ToolChoice) args[5],
            (List<Tool>) args[6],
            (Float) args[7]
        )
    );

    static {
        PARSER.declareObjectArray(constructorArg(), Message.PARSER::apply, new ParseField("messages"));
        PARSER.declareString(optionalConstructorArg(), new ParseField("model"));
        PARSER.declareLong(optionalConstructorArg(), new ParseField("max_completion_tokens"));
        PARSER.declareStringArray(optionalConstructorArg(), new ParseField("stop"));
        PARSER.declareFloat(optionalConstructorArg(), new ParseField("temperature"));
        PARSER.declareField(
            optionalConstructorArg(),
            (p, c) -> parseToolChoice(p),
            new ParseField("tool_choice"),
            ObjectParser.ValueType.OBJECT_OR_STRING
        );
        PARSER.declareObjectArray(optionalConstructorArg(), Tool.PARSER::apply, new ParseField("tools"));
        PARSER.declareFloat(optionalConstructorArg(), new ParseField("top_p"));
    }

    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(Content.class, ContentObjects.NAME, ContentObjects::new),
            new NamedWriteableRegistry.Entry(Content.class, ContentString.NAME, ContentString::new),
            new NamedWriteableRegistry.Entry(ContentObject.class, ContentObjectText.NAME, ContentObjectText::new),
            new NamedWriteableRegistry.Entry(ContentObject.class, ContentObjectImage.NAME, ContentObjectImage::new),
            new NamedWriteableRegistry.Entry(ContentObject.class, ContentObjectFile.NAME, ContentObjectFile::new),
            new NamedWriteableRegistry.Entry(ToolChoice.class, ToolChoiceObject.NAME, ToolChoiceObject::new),
            new NamedWriteableRegistry.Entry(ToolChoice.class, ToolChoiceString.NAME, ToolChoiceString::new)
        );
    }

    public static UnifiedCompletionRequest of(List<Message> messages) {
        return new UnifiedCompletionRequest(messages, null, null, null, null, null, null, null);
    }

    public UnifiedCompletionRequest(StreamInput in) throws IOException {
        this(
            in.readCollectionAsImmutableList(Message::new),
            in.readOptionalString(),
            in.readOptionalVLong(),
            in.readOptionalStringCollectionAsList(),
            in.readOptionalFloat(),
            in.readOptionalNamedWriteable(ToolChoice.class),
            in.readOptionalCollectionAsList(Tool::new),
            in.readOptionalFloat()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(messages);
        out.writeOptionalString(model);
        out.writeOptionalVLong(maxCompletionTokens);
        out.writeOptionalStringCollection(stop);
        out.writeOptionalFloat(temperature);
        out.writeOptionalNamedWriteable(toolChoice);
        out.writeOptionalCollection(tools);
        out.writeOptionalFloat(topP);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(MESSAGES_FIELD, messages);
        if (stop != null && (stop.isEmpty() == false)) {
            builder.field(STOP_FIELD, stop);
        }
        if (temperature != null) {
            builder.field(TEMPERATURE_FIELD, temperature);
        }
        if (toolChoice != null) {
            toolChoice.toXContent(builder, params);
        }
        if (tools != null && (tools.isEmpty() == false)) {
            builder.field(TOOL_FIELD, tools);
        }
        if (topP != null) {
            builder.field(TOP_P_FIELD, topP);
        }
        // some providers only support the now-deprecated max_tokens, others have migrated to max_completion_tokens
        if (maxCompletionTokens != null && params.param(MAX_TOKENS_PARAM) != null) {
            builder.field(params.param(MAX_TOKENS_PARAM), maxCompletionTokens);
        }
        // some implementations handle modelId differently, for example OpenAI has a default in the server settings and override it there
        // so we allow implementations to pass in the model id via the params
        if (params.param(MODEL_ID_PARAM) != null) {
            builder.field(MODEL_FIELD, params.param(MODEL_ID_PARAM));
        }
        return builder;
    }

    public boolean containsMultimodalContent() {
        return messages().stream().anyMatch(m -> m.content().containsMultimodalContent());
    }

    public record Message(Content content, String role, @Nullable String toolCallId, @Nullable List<ToolCall> toolCalls)
        implements
            Writeable,
            ToXContentObject {

        @SuppressWarnings("unchecked")
        static final ConstructingObjectParser<Message, Void> PARSER = new ConstructingObjectParser<>(
            Message.class.getSimpleName(),
            args -> new Message((Content) args[0], (String) args[1], (String) args[2], (List<ToolCall>) args[3])
        );

        static {
            PARSER.declareField(
                optionalConstructorArg(),
                (p, c) -> parseContent(p),
                new ParseField("content"),
                ObjectParser.ValueType.VALUE_ARRAY
            );
            PARSER.declareString(constructorArg(), new ParseField("role"));
            PARSER.declareString(optionalConstructorArg(), new ParseField("tool_call_id"));
            PARSER.declareObjectArray(optionalConstructorArg(), ToolCall.PARSER::apply, new ParseField("tool_calls"));
        }

        private static Content parseContent(XContentParser parser) throws IOException {
            var token = parser.currentToken();
            if (token == XContentParser.Token.START_ARRAY) {
                var parsedContentObjects = XContentParserUtils.parseList(parser, (p) -> ContentObject.fromMap(p.map()));
                return new ContentObjects(parsedContentObjects);
            } else if (token == XContentParser.Token.VALUE_STRING) {
                return ContentString.of(parser);
            }

            throw new XContentParseException("Expected an array start token or a value string token but found token [" + token + "]");
        }

        public Message(StreamInput in) throws IOException {
            this(
                in.readOptionalNamedWriteable(Content.class),
                in.readString(),
                in.readOptionalString(),
                in.readOptionalCollectionAsList(ToolCall::new)
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalNamedWriteable(content);
            out.writeString(role);
            out.writeOptionalString(toolCallId);
            out.writeOptionalCollection(toolCalls);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();

            if (content != null) {
                content.toXContent(builder, params);
            }
            builder.field(ROLE_FIELD, role);
            if (toolCallId != null) {
                builder.field(TOOL_CALL_ID_FIELD, toolCallId);
            }
            if (toolCalls != null) {
                builder.field(TOOL_CALLS_FIELD, toolCalls);
            }

            return builder.endObject();
        }
    }

    public record ContentObjects(List<ContentObject> contentObjects) implements Content, NamedWriteable {

        public static final String NAME = "content_objects";

        public ContentObjects(StreamInput in) throws IOException {
            this(in.readNamedWriteableCollectionAsList(ContentObject.class));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeNamedWriteableCollection(contentObjects);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.field(CONTENT_FIELD, contentObjects);
        }

        @Override
        public boolean containsMultimodalContent() {
            return contentObjects.stream().anyMatch(o -> o.type().equals(TEXT) == false);
        }
    }

    public abstract static sealed class ContentObject implements NamedWriteable, ToXContent permits ContentObjectText, ContentObjectImage,
        ContentObjectFile {

        public enum ContentObjectType {
            TEXT,
            IMAGE_URL,
            FILE;

            @Override
            public String toString() {
                return name().toLowerCase(Locale.ROOT);
            }

            public static ContentObjectType fromString(String name) {
                try {
                    return valueOf(name.trim().toUpperCase(Locale.ROOT));
                } catch (IllegalArgumentException ex) {
                    throw getUnrecognizedTypeException(name, CONTENT_FIELD, ContentObjectType.class);
                }
            }
        }

        final ContentObjectType type;

        ContentObject(ContentObjectType type) {
            this.type = type;
        }

        @SuppressWarnings("unchecked")
        public static ContentObject fromMap(Map<String, Object> map) {
            String typeString = extractRequiredFieldOfType(map, TYPE_FIELD, String.class, CONTENT_FIELD);
            ContentObjectType type = ContentObjectType.fromString(typeString);
            ContentObject content = switch (type) {
                case TEXT -> new ContentObjectText(extractRequiredFieldOfType(map, TEXT_FIELD, String.class, CONTENT_FIELD));
                case IMAGE_URL -> {
                    Map<String, Object> imageUrlMap = extractRequiredFieldOfType(map, IMAGE_URL_FIELD, Map.class, CONTENT_FIELD);
                    yield new ContentObjectImage(ContentObjectImageUrl.fromMap(imageUrlMap));
                }
                case FILE -> {
                    Map<String, Object> fileFieldsMap = extractRequiredFieldOfType(map, FILE_FIELD, Map.class, CONTENT_FIELD);
                    yield new ContentObjectFile(ContentObjectFileFields.fromMap(fileFieldsMap));
                }
            };
            throwIfNotEmptyMap(map, CONTENT_FIELD);
            return content;
        }

        public ContentObjectType type() {
            return type;
        }
    }

    public static final class ContentObjectText extends ContentObject {
        public static final String NAME = "content_object_text";
        private final String text;

        public ContentObjectText(String text) {
            super(TEXT);
            this.text = text;
        }

        public ContentObjectText(StreamInput in) throws IOException {
            this(in.readString());
            if (in.getTransportVersion().supports(MULTIMODAL_CHAT_COMPLETION_SUPPORT_ADDED) == false) {
                in.readString();
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getTransportVersion().supports(MULTIMODAL_CHAT_COMPLETION_SUPPORT_ADDED)) {
                out.writeString(text);
            } else {
                out.writeString(text);
                out.writeString(TEXT.toString());
            }
        }

        public String toString() {
            return text + ":" + type;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(TEXT_FIELD, text);
            builder.field(TYPE_FIELD, type.toString());
            return builder.endObject();
        }

        public String text() {
            return text;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj == null || obj.getClass() != this.getClass()) return false;
            var that = (ContentObjectText) obj;
            return Objects.equals(this.text, that.text);
        }

        @Override
        public int hashCode() {
            return Objects.hash(text);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }
    }

    public static final class ContentObjectImage extends ContentObject {
        public static final String NAME = "content_object_image";
        private final ContentObjectImageUrl imageUrl;

        public ContentObjectImage(ContentObjectImageUrl imageUrl) {
            super(IMAGE_URL);
            this.imageUrl = imageUrl;
        }

        public ContentObjectImage(StreamInput in) throws IOException {
            this(new ContentObjectImageUrl(in));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getTransportVersion().supports(MULTIMODAL_CHAT_COMPLETION_SUPPORT_ADDED) == false) {
                throw new ElasticsearchStatusException(
                    "Cannot send a multimodal chat completion request to an older node. "
                        + "Please wait until all nodes are upgraded before using multimodal chat completion inputs",
                    RestStatus.BAD_REQUEST
                );
            }
            imageUrl.writeTo(out);
        }

        public String toString() {
            return imageUrl + ":" + type;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(IMAGE_URL_FIELD, imageUrl);
            builder.field(TYPE_FIELD, type.toString());
            return builder.endObject();
        }

        public ContentObjectImageUrl imageUrl() {
            return imageUrl;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj == null || obj.getClass() != this.getClass()) return false;
            var that = (ContentObjectImage) obj;
            return Objects.equals(this.imageUrl, that.imageUrl);
        }

        @Override
        public int hashCode() {
            return Objects.hash(imageUrl);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }
    }

    public record ContentObjectImageUrl(String url, @Nullable ImageUrlDetail detail) implements Writeable, ToXContentObject {
        public static ContentObjectImageUrl fromMap(Map<String, Object> map) {
            String url = extractRequiredFieldOfType(map, URL_FIELD, String.class, URL_FIELD);
            String detailString = extractOptionalFieldOfType(map, DETAIL_FIELD, String.class, URL_FIELD);
            ImageUrlDetail detail = detailString == null ? null : ImageUrlDetail.fromString(detailString);
            throwIfNotEmptyMap(map, IMAGE_URL_FIELD);
            return new ContentObjectImageUrl(url, detail);
        }

        public ContentObjectImageUrl(StreamInput in) throws IOException {
            this(in.readString(), in.readOptionalEnum(ImageUrlDetail.class));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(url);
            out.writeOptionalEnum(detail);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(URL_FIELD, url);
            if (detail != null) {
                builder.field(DETAIL_FIELD, detail);
            }
            return builder.endObject();
        }
    }

    public enum ImageUrlDetail {
        AUTO,
        LOW,
        HIGH;

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }

        public static ImageUrlDetail fromString(String name) {
            try {
                return valueOf(name.trim().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException ex) {
                throw getUnrecognizedTypeException(name, URL_FIELD, ImageUrlDetail.class);
            }
        }
    }

    public static final class ContentObjectFile extends ContentObject {
        public static final String NAME = "content_object_file";
        private final ContentObjectFileFields fileFields;

        public ContentObjectFile(ContentObjectFileFields fileFields) {
            super(FILE);
            this.fileFields = fileFields;
        }

        public ContentObjectFile(StreamInput in) throws IOException {
            this(new ContentObjectFileFields(in));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getTransportVersion().supports(MULTIMODAL_CHAT_COMPLETION_SUPPORT_ADDED) == false) {
                throw new ElasticsearchStatusException(
                    "Cannot send a multimodal chat completion request to an older node. "
                        + "Please wait until all nodes are upgraded before using multimodal chat completion inputs",
                    RestStatus.BAD_REQUEST
                );
            }
            fileFields.writeTo(out);
        }

        public String toString() {
            return fileFields + ":" + type;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(FILE_FIELD, fileFields);
            builder.field(TYPE_FIELD, type.toString());
            return builder.endObject();
        }

        public ContentObjectFileFields fileFields() {
            return fileFields;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj == null || obj.getClass() != this.getClass()) return false;
            var that = (ContentObjectFile) obj;
            return Objects.equals(this.fileFields, that.fileFields);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fileFields);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }
    }

    // The file_id field is not currently supported, but it's part of the OpenAI schema, so it's treated as an optional field for
    // serialization. file_id not being supported means that file_data and filename are effectively required despite being optional in the
    // OpenAI schema, but they are also treated as optional for serialization, in order to make it possible to add support for file_id in
    // future without needing to introduce a new transport version or worry about backward compatibility.
    public record ContentObjectFileFields(@Nullable String fileData, @Nullable String fileId, @Nullable String filename)
        implements
            Writeable,
            ToXContentObject {

        public static ContentObjectFileFields fromMap(Map<String, Object> map) {
            String fileData = extractRequiredFieldOfType(map, FILE_DATA_FIELD, String.class, FILE_FIELD);
            String filename = extractRequiredFieldOfType(map, FILENAME_FIELD, String.class, FILE_FIELD);
            if (map.containsKey(FILE_ID_FIELD)) {
                throw new ElasticsearchStatusException(
                    Strings.format(
                        "Field [%s] is not supported for content of type [%s]",
                        FILE_ID_FIELD,
                        ContentObject.ContentObjectType.FILE
                    ),
                    RestStatus.BAD_REQUEST
                );
            }
            throwIfNotEmptyMap(map, FILE_FIELD);
            return new ContentObjectFileFields(fileData, null, filename);
        }

        public ContentObjectFileFields(StreamInput in) throws IOException {
            this(in.readOptionalString(), in.readOptionalString(), in.readOptionalString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(fileData);
            out.writeOptionalString(fileId);
            out.writeOptionalString(filename);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (fileData != null) {
                builder.field(FILE_DATA_FIELD, fileData);
            }
            if (fileId != null) {
                builder.field(FILE_ID_FIELD, fileId);
            }
            if (filename != null) {
                builder.field(FILENAME_FIELD, filename);
            }
            return builder.endObject();
        }
    }

    public record ContentString(String content) implements Content, NamedWriteable {
        public static final String NAME = "content_string";

        public static ContentString of(XContentParser parser) throws IOException {
            var content = parser.text();
            return new ContentString(content);
        }

        public ContentString(StreamInput in) throws IOException {
            this(in.readString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(content);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        public String toString() {
            return content;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.field(CONTENT_FIELD, content);
        }

        @Override
        public boolean containsMultimodalContent() {
            return false;
        }
    }

    public record ToolCall(String id, FunctionField function, String type) implements Writeable, ToXContentObject {

        static final ConstructingObjectParser<ToolCall, Void> PARSER = new ConstructingObjectParser<>(
            ToolCall.class.getSimpleName(),
            args -> new ToolCall((String) args[0], (FunctionField) args[1], (String) args[2])
        );

        static {
            PARSER.declareString(constructorArg(), new ParseField("id"));
            PARSER.declareObject(constructorArg(), FunctionField.PARSER::apply, new ParseField("function"));
            PARSER.declareString(constructorArg(), new ParseField("type"));
        }

        public ToolCall(StreamInput in) throws IOException {
            this(in.readString(), new FunctionField(in), in.readString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(id);
            function.writeTo(out);
            out.writeString(type);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(ID_FIELD, id);
            builder.field(FUNCTION_FIELD, function);
            builder.field(TYPE_FIELD, type);
            return builder.endObject();
        }

        public record FunctionField(String arguments, String name) implements Writeable, ToXContentObject {
            static final ConstructingObjectParser<FunctionField, Void> PARSER = new ConstructingObjectParser<>(
                "tool_call_function_field",
                args -> new FunctionField((String) args[0], (String) args[1])
            );

            static {
                PARSER.declareString(constructorArg(), new ParseField("arguments"));
                PARSER.declareString(constructorArg(), new ParseField("name"));
            }

            public FunctionField(StreamInput in) throws IOException {
                this(in.readString(), in.readString());
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(arguments);
                out.writeString(name);
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.field(ARGUMENTS_FIELD, arguments);
                builder.field(NAME_FIELD, name);
                return builder.endObject();
            }
        }
    }

    private static ToolChoice parseToolChoice(XContentParser parser) throws IOException {
        var token = parser.currentToken();
        if (token == XContentParser.Token.START_OBJECT) {
            return ToolChoiceObject.PARSER.apply(parser, null);
        } else if (token == XContentParser.Token.VALUE_STRING) {
            return ToolChoiceString.of(parser);
        }

        throw new XContentParseException("Unsupported token [" + token + "]");
    }

    public sealed interface ToolChoice extends NamedWriteable, ToXContent permits ToolChoiceObject, ToolChoiceString {}

    public record ToolChoiceObject(String type, FunctionField function) implements ToolChoice, NamedWriteable {

        public static final String NAME = "tool_choice_object";

        static final ConstructingObjectParser<ToolChoiceObject, Void> PARSER = new ConstructingObjectParser<>(
            ToolChoiceObject.class.getSimpleName(),
            args -> new ToolChoiceObject((String) args[0], (FunctionField) args[1])
        );

        static {
            PARSER.declareString(constructorArg(), new ParseField("type"));
            PARSER.declareObject(constructorArg(), FunctionField.PARSER::apply, new ParseField("function"));
        }

        public ToolChoiceObject(StreamInput in) throws IOException {
            this(in.readString(), new FunctionField(in));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(type);
            function.writeTo(out);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(TOOL_CHOICE_FIELD);
            builder.field(TYPE_FIELD, type);
            builder.field(FUNCTION_FIELD, function);
            return builder.endObject();
        }

        public record FunctionField(String name) implements Writeable, ToXContentObject {
            static final ConstructingObjectParser<FunctionField, Void> PARSER = new ConstructingObjectParser<>(
                "tool_choice_function_field",
                args -> new FunctionField((String) args[0])
            );

            static {
                PARSER.declareString(constructorArg(), new ParseField("name"));
            }

            public FunctionField(StreamInput in) throws IOException {
                this(in.readString());
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(name);
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return builder.startObject().field(NAME_FIELD, name).endObject();
            }
        }
    }

    public record ToolChoiceString(String value) implements ToolChoice, NamedWriteable {
        public static final String NAME = "tool_choice_string";

        public static ToolChoiceString of(XContentParser parser) throws IOException {
            var content = parser.text();
            return new ToolChoiceString(content);
        }

        public ToolChoiceString(StreamInput in) throws IOException {
            this(in.readString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(value);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.field(TOOL_CHOICE_FIELD, value);
        }
    }

    public record Tool(String type, FunctionField function) implements Writeable, ToXContentObject {

        static final ConstructingObjectParser<Tool, Void> PARSER = new ConstructingObjectParser<>(
            Tool.class.getSimpleName(),
            args -> new Tool((String) args[0], (FunctionField) args[1])
        );

        static {
            PARSER.declareString(constructorArg(), new ParseField("type"));
            PARSER.declareObject(constructorArg(), FunctionField.PARSER::apply, new ParseField("function"));
        }

        public Tool(StreamInput in) throws IOException {
            this(in.readString(), new FunctionField(in));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(type);
            function.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();

            builder.field(TYPE_FIELD, type);
            builder.field(FUNCTION_FIELD, function);

            return builder.endObject();
        }

        public record FunctionField(
            @Nullable String description,
            String name,
            @Nullable Map<String, Object> parameters,
            @Nullable Boolean strict
        ) implements Writeable, ToXContentObject {

            @SuppressWarnings("unchecked")
            static final ConstructingObjectParser<FunctionField, Void> PARSER = new ConstructingObjectParser<>(
                "tool_function_field",
                args -> new FunctionField((String) args[0], (String) args[1], (Map<String, Object>) args[2], (Boolean) args[3])
            );

            static {
                PARSER.declareString(optionalConstructorArg(), new ParseField("description"));
                PARSER.declareString(constructorArg(), new ParseField("name"));
                PARSER.declareObject(optionalConstructorArg(), (p, c) -> p.mapOrdered(), new ParseField("parameters"));
                PARSER.declareBoolean(optionalConstructorArg(), new ParseField("strict"));
            }

            public FunctionField(StreamInput in) throws IOException {
                this(in.readOptionalString(), in.readString(), in.readGenericMap(), in.readOptionalBoolean());
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeOptionalString(description);
                out.writeString(name);
                out.writeGenericMap(parameters);
                out.writeOptionalBoolean(strict);
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.field(DESCRIPTION_FIELD, description);
                builder.field(NAME_FIELD, name);
                builder.field(PARAMETERS_FIELD, parameters);
                if (strict != null) {
                    builder.field(STRICT_FIELD, strict);
                }
                return builder.endObject();
            }
        }
    }

    public static <T> T extractRequiredFieldOfType(Map<String, Object> sourceMap, String key, Class<T> type, String containingObject) {
        return extractFieldOfType(sourceMap, key, type, true, containingObject);
    }

    public static <T> T extractOptionalFieldOfType(Map<String, Object> sourceMap, String key, Class<T> type, String containingObject) {
        return extractFieldOfType(sourceMap, key, type, false, containingObject);
    }

    @SuppressWarnings("unchecked")
    private static <T> T extractFieldOfType(
        Map<String, Object> sourceMap,
        String key,
        Class<T> type,
        boolean required,
        String containingObject
    ) {
        Object o = sourceMap.remove(key);
        if (o == null) {
            if (required) {
                throw new ElasticsearchStatusException(
                    Strings.format("Field [%s] in object [%s] is required but was not found", key, containingObject),
                    RestStatus.BAD_REQUEST
                );
            } else {
                return null;
            }
        }

        if (type.isAssignableFrom(o.getClass())) {
            return (T) o;
        } else {
            throw new ElasticsearchStatusException(
                Strings.format(
                    "Field [%s] in object [%s] is not of the expected type. Expected [%s] but was [%s]",
                    key,
                    containingObject,
                    type.getSimpleName(),
                    o.getClass().getSimpleName()
                ),
                RestStatus.BAD_REQUEST
            );
        }
    }

    public static void throwIfNotEmptyMap(Map<String, Object> map, String containingObject) {
        if (map != null && map.isEmpty() == false) {
            throw new ElasticsearchStatusException(
                Strings.format("[%s] contains unknown fields %s", containingObject, map.keySet()),
                RestStatus.BAD_REQUEST
            );
        }
    }

    private static <E extends Enum<E>> ElasticsearchStatusException getUnrecognizedTypeException(
        String name,
        String containingObject,
        Class<E> clazz
    ) {
        return new ElasticsearchStatusException(
            Strings.format("Unrecognized type [%s] in object [%s], must be one of %s", name, containingObject, EnumSet.allOf(clazz)),
            RestStatus.BAD_REQUEST
        );
    }
}
