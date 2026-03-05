/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
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
import org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.MAX_COMPLETION_TOKENS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.MAX_TOKENS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.MESSAGES_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.MODEL_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.STOP_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.TEMPERATURE_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.TOOL_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.TOP_P_FIELD;
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

    /**
     * We currently allow providers to override the model id that is written to JSON.
     * Rather than use {@link #model()}, providers are expected to pass in the modelId via
     * {@link Params}.
     */
    private static final String MODEL_ID_PARAM = "model_id_value";
    /**
     * Some providers only support the now-deprecated {@link UnifiedCompletionRequestUtils#MAX_TOKENS_FIELD}, others have migrated to
     * {@link UnifiedCompletionRequestUtils#MAX_COMPLETION_TOKENS_FIELD}. Providers are expected to pass in their supported field name.
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
     * - Key: {@link UnifiedCompletionRequestUtils#MODEL_FIELD}, Value: modelId, if modelId is not null
     * - Key: {@link UnifiedCompletionRequestUtils#MAX_TOKENS_FIELD}, Value: {@link #maxCompletionTokens()}
     */
    public static Params withMaxTokens(@Nullable String modelId, Params params) {
        Map<String, String> entries = modelId != null
            ? Map.ofEntries(Map.entry(MODEL_ID_PARAM, modelId), Map.entry(MAX_TOKENS_PARAM, MAX_TOKENS_FIELD))
            : Map.ofEntries(Map.entry(MAX_TOKENS_PARAM, MAX_TOKENS_FIELD));
        return new DelegatingMapParams(entries, params);
    }

    /**
     * Creates a {@link Params} that causes ToXContent to include the key values:
     * - Key: {@link UnifiedCompletionRequestUtils#MODEL_FIELD}, Value: modelId, if modelId is not null
     * - Key: {@link UnifiedCompletionRequestUtils#MAX_TOKENS_FIELD}, Value: {@link #maxCompletionTokens()}
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
     * - Key: {@link UnifiedCompletionRequestUtils#MODEL_FIELD}, Value: modelId
     * - Key: {@link UnifiedCompletionRequestUtils#MAX_COMPLETION_TOKENS_FIELD}, Value: {@link #maxCompletionTokens()}
     */
    public static Params withMaxCompletionTokens(String modelId, Params params) {
        return new DelegatingMapParams(
            Map.ofEntries(Map.entry(MODEL_ID_PARAM, modelId), Map.entry(MAX_TOKENS_PARAM, MAX_COMPLETION_TOKENS_FIELD)),
            params
        );
    }

    /**
     * Creates a {@link Params} that causes ToXContent to include the key values:
     * - Key: {@link UnifiedCompletionRequestUtils#MAX_COMPLETION_TOKENS_FIELD}, Value: {@link #maxCompletionTokens()}
     */
    public static Params withMaxCompletionTokens(Params params) {
        return new DelegatingMapParams(Map.of(MAX_TOKENS_PARAM, MAX_COMPLETION_TOKENS_FIELD), params);
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

    private static ToolChoice parseToolChoice(XContentParser parser) throws IOException {
        var token = parser.currentToken();
        if (token == XContentParser.Token.START_OBJECT) {
            return ToolChoiceObject.PARSER.apply(parser, null);
        } else if (token == XContentParser.Token.VALUE_STRING) {
            return ToolChoiceString.of(parser);
        }

        throw new XContentParseException("Unsupported token [" + token + "]");
    }
}
