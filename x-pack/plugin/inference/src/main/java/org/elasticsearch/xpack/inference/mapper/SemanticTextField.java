/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.support.MapXContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * A {@link ToXContentObject} that is used to represent the transformation of the semantic text field's inputs.
 * The resulting object preserves the original input under the {@link SemanticTextField#TEXT_FIELD} and exposes
 * the inference results under the {@link SemanticTextField#INFERENCE_FIELD}.
 *
 * @param fieldName The original field name.
 * @param originalValues The original values associated with the field name for indices created before
 *                       {@link IndexVersions#INFERENCE_METADATA_FIELDS_BACKPORT}, null otherwise.
 * @param inference The inference result.
 * @param contentType The {@link XContentType} used to store the embeddings chunks.
 */
public record SemanticTextField(
    boolean useLegacyFormat,
    String fieldName,
    @Nullable List<String> originalValues,
    InferenceResult inference,
    XContentType contentType
) implements ToXContentObject {

    static final String TEXT_FIELD = "text";
    static final String INFERENCE_FIELD = "inference";
    static final String INFERENCE_ID_FIELD = "inference_id";
    static final String SEARCH_INFERENCE_ID_FIELD = "search_inference_id";
    static final String CHUNKS_FIELD = "chunks";
    static final String CHUNKED_EMBEDDINGS_FIELD = "embeddings";
    public static final String CHUNKED_TEXT_FIELD = "text";
    static final String CHUNKED_OFFSET_FIELD = "offset";
    static final String CHUNKED_START_OFFSET_FIELD = "start_offset";
    static final String CHUNKED_END_OFFSET_FIELD = "end_offset";
    static final String MODEL_SETTINGS_FIELD = "model_settings";

    public record InferenceResult(String inferenceId, MinimalServiceSettings modelSettings, Map<String, List<Chunk>> chunks) {}

    public record Chunk(@Nullable String text, int startOffset, int endOffset, BytesReference rawEmbeddings) {}

    public record Offset(String sourceFieldName, int startOffset, int endOffset) {}

    public static String getOriginalTextFieldName(String fieldName) {
        return fieldName + "." + TEXT_FIELD;
    }

    public static String getInferenceFieldName(String fieldName) {
        return fieldName + "." + INFERENCE_FIELD;
    }

    public static String getChunksFieldName(String fieldName) {
        return getInferenceFieldName(fieldName) + "." + CHUNKS_FIELD;
    }

    public static String getEmbeddingsFieldName(String fieldName) {
        return getChunksFieldName(fieldName) + "." + CHUNKED_EMBEDDINGS_FIELD;
    }

    public static String getOffsetsFieldName(String fieldName) {
        return getChunksFieldName(fieldName) + "." + CHUNKED_OFFSET_FIELD;
    }

    record ParserContext(boolean useLegacyFormat, String fieldName, XContentType xContentType) {}

    static SemanticTextField parse(XContentParser parser, ParserContext context) throws IOException {
        return SEMANTIC_TEXT_FIELD_PARSER.parse(parser, context);
    }

    static MinimalServiceSettings parseModelSettingsFromMap(Object node) {
        if (node == null) {
            return null;
        }
        try {
            Map<String, Object> map = XContentMapValues.nodeMapValue(node, MODEL_SETTINGS_FIELD);
            XContentParser parser = new MapXContentParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.IGNORE_DEPRECATIONS,
                map,
                XContentType.JSON
            );
            return MinimalServiceSettings.parse(parser);
        } catch (Exception exc) {
            throw new ElasticsearchException(exc);
        }
    }

    @Override
    public List<String> originalValues() {
        return originalValues != null ? originalValues : Collections.emptyList();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        List<String> originalValues = originalValues();
        if (useLegacyFormat && originalValues.isEmpty() == false) {
            builder.field(TEXT_FIELD, originalValues.size() == 1 ? originalValues.get(0) : originalValues);
        }
        builder.startObject(INFERENCE_FIELD);
        builder.field(INFERENCE_ID_FIELD, inference.inferenceId);
        builder.field(MODEL_SETTINGS_FIELD, inference.modelSettings);
        if (useLegacyFormat) {
            builder.startArray(CHUNKS_FIELD);
        } else {
            builder.startObject(CHUNKS_FIELD);
        }
        for (var entry : inference.chunks.entrySet()) {
            if (useLegacyFormat == false) {
                builder.startArray(entry.getKey());
            }
            for (var chunk : entry.getValue()) {
                builder.startObject();
                if (useLegacyFormat) {
                    builder.field(TEXT_FIELD, chunk.text);
                } else {
                    builder.field(CHUNKED_START_OFFSET_FIELD, chunk.startOffset);
                    builder.field(CHUNKED_END_OFFSET_FIELD, chunk.endOffset);
                }
                XContentParser parser = XContentHelper.createParserNotCompressed(
                    XContentParserConfiguration.EMPTY,
                    chunk.rawEmbeddings,
                    contentType
                );
                builder.field(CHUNKED_EMBEDDINGS_FIELD).copyCurrentStructure(parser);
                builder.endObject();
            }
            if (useLegacyFormat == false) {
                builder.endArray();
            }
        }
        if (useLegacyFormat) {
            builder.endArray();
        } else {
            builder.endObject();
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<SemanticTextField, ParserContext> SEMANTIC_TEXT_FIELD_PARSER =
        new ConstructingObjectParser<>(SemanticTextFieldMapper.CONTENT_TYPE, true, (args, context) -> {
            List<String> originalValues = (List<String>) args[0];
            if (context.useLegacyFormat() == false) {
                if (originalValues != null && originalValues.isEmpty() == false) {
                    throw new IllegalArgumentException("Unknown field [" + TEXT_FIELD + "]");
                }
                originalValues = null;
            }
            return new SemanticTextField(
                context.useLegacyFormat(),
                context.fieldName(),
                originalValues,
                (InferenceResult) args[1],
                context.xContentType()
            );
        });

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<InferenceResult, ParserContext> INFERENCE_RESULT_PARSER = new ConstructingObjectParser<>(
        INFERENCE_FIELD,
        true,
        args -> new InferenceResult((String) args[0], (MinimalServiceSettings) args[1], (Map<String, List<Chunk>>) args[2])
    );

    private static final ConstructingObjectParser<Chunk, ParserContext> CHUNKS_PARSER = new ConstructingObjectParser<>(
        CHUNKS_FIELD,
        true,
        (args, context) -> {
            String text = (String) args[0];
            if (context.useLegacyFormat() && text == null) {
                throw new IllegalArgumentException("Missing chunk text");
            }
            return new Chunk(text, args[1] != null ? (int) args[1] : -1, args[2] != null ? (int) args[2] : -1, (BytesReference) args[3]);
        }
    );

    static {
        SEMANTIC_TEXT_FIELD_PARSER.declareStringArray(optionalConstructorArg(), new ParseField(TEXT_FIELD));
        SEMANTIC_TEXT_FIELD_PARSER.declareObject(constructorArg(), INFERENCE_RESULT_PARSER, new ParseField(INFERENCE_FIELD));

        INFERENCE_RESULT_PARSER.declareString(constructorArg(), new ParseField(INFERENCE_ID_FIELD));
        INFERENCE_RESULT_PARSER.declareObjectOrNull(
            constructorArg(),
            (p, c) -> MinimalServiceSettings.parse(p),
            null,
            new ParseField(MODEL_SETTINGS_FIELD)
        );
        INFERENCE_RESULT_PARSER.declareField(constructorArg(), (p, c) -> {
            if (c.useLegacyFormat()) {
                return Map.of(c.fieldName, parseChunksArrayLegacy(p, c));
            }
            return parseChunksMap(p, c);
        }, new ParseField(CHUNKS_FIELD), ObjectParser.ValueType.OBJECT_ARRAY);

        CHUNKS_PARSER.declareString(optionalConstructorArg(), new ParseField(TEXT_FIELD));
        CHUNKS_PARSER.declareInt(optionalConstructorArg(), new ParseField(CHUNKED_START_OFFSET_FIELD));
        CHUNKS_PARSER.declareInt(optionalConstructorArg(), new ParseField(CHUNKED_END_OFFSET_FIELD));
        CHUNKS_PARSER.declareField(constructorArg(), (p, c) -> {
            XContentBuilder b = XContentBuilder.builder(p.contentType().xContent());
            b.copyCurrentStructure(p);
            return BytesReference.bytes(b);
        }, new ParseField(CHUNKED_EMBEDDINGS_FIELD), ObjectParser.ValueType.OBJECT_ARRAY);
    }

    private static Map<String, List<Chunk>> parseChunksMap(XContentParser parser, ParserContext context) throws IOException {
        Map<String, List<Chunk>> resultMap = new LinkedHashMap<>();
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
            String fieldName = parser.currentName();
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.nextToken(), parser);
            var chunks = resultMap.computeIfAbsent(fieldName, k -> new ArrayList<>());
            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                chunks.add(CHUNKS_PARSER.parse(parser, context));
            }
        }
        return resultMap;
    }

    private static List<Chunk> parseChunksArrayLegacy(XContentParser parser, ParserContext context) throws IOException {
        List<Chunk> results = new ArrayList<>();
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            results.add(CHUNKS_PARSER.parse(parser, context));
        }
        return results;
    }

    /**
     * Converts the provided {@link ChunkedInference} into a list of {@link Chunk}.
     */
    public static List<Chunk> toSemanticTextFieldChunks(
        String input,
        int offsetAdjustment,
        ChunkedInference results,
        XContentType contentType,
        boolean useLegacyFormat
    ) throws IOException {
        List<Chunk> chunks = new ArrayList<>();
        Iterator<ChunkedInference.Chunk> it = results.chunksAsMatchedTextAndByteReference(contentType.xContent());
        while (it.hasNext()) {
            chunks.add(toSemanticTextFieldChunk(input, offsetAdjustment, it.next(), useLegacyFormat));
        }
        return chunks;
    }

    public static Chunk toSemanticTextFieldChunk(
        String input,
        int offsetAdjustment,
        ChunkedInference.Chunk chunk,
        boolean useLegacyFormat
    ) {
        String text = null;
        int startOffset = -1;
        int endOffset = -1;
        if (useLegacyFormat) {
            text = input.substring(chunk.textOffset().start(), chunk.textOffset().end());
        } else {
            startOffset = chunk.textOffset().start() + offsetAdjustment;
            endOffset = chunk.textOffset().end() + offsetAdjustment;
        }

        return new Chunk(text, startOffset, endOffset, chunk.bytesReference());
    }
}
