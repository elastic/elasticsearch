/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Represents the staged (uncommitted) state of a BYO semantic_text field during
 * a multi-part upload. Instances are stored under
 * {@code _inference_metadata.<field>._staged} and accumulate chunks across
 * successive {@code stage} requests until a {@code commit} or {@code cancel}
 * action finalises the session.
 *
 * @param text         the full original text, set at {@code stage_init} time
 * @param textLength   character length of {@code text}
 * @param lastModified timestamp of the most recent {@code stage} request, used
 *                     for TTL-based cleanup of abandoned sessions
 * @param chunks       accumulated chunks with character offsets and embeddings
 */
public record StagedSemanticField(String text, int textLength, Instant lastModified, List<SemanticTextField.Chunk> chunks)
    implements
        ToXContentObject {

    static final String TEXT_FIELD = "text";
    static final String TEXT_LENGTH_FIELD = "text_length";
    static final String LAST_MODIFIED_FIELD = "last_modified";
    static final String CHUNKS_FIELD = "chunks";
    static final String START_OFFSET_FIELD = "start_offset";
    static final String END_OFFSET_FIELD = "end_offset";
    static final String EMBEDDINGS_FIELD = "embeddings";

    public StagedSemanticField {
        Objects.requireNonNull(text, "text must not be null");
        Objects.requireNonNull(lastModified, "lastModified must not be null");
        Objects.requireNonNull(chunks, "chunks must not be null");
    }

    /**
     * Returns a new instance with {@code newChunks} appended to the existing
     * chunk list and {@code newTimestamp} as the updated {@link #lastModified}.
     *
     * @param newChunks     chunks to append; must not be {@code null}
     * @param newTimestamp  timestamp to record as the last modification time
     * @return a new {@link StagedSemanticField} with combined chunks
     */
    public StagedSemanticField withAppendedChunks(List<SemanticTextField.Chunk> newChunks, Instant newTimestamp) {
        Objects.requireNonNull(newChunks, "newChunks must not be null");
        Objects.requireNonNull(newTimestamp, "newTimestamp must not be null");
        List<SemanticTextField.Chunk> combined = new ArrayList<>(chunks.size() + newChunks.size());
        combined.addAll(chunks);
        combined.addAll(newChunks);
        return new StagedSemanticField(text, textLength, newTimestamp, combined);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TEXT_FIELD, text);
        builder.field(TEXT_LENGTH_FIELD, textLength);
        builder.field(LAST_MODIFIED_FIELD, lastModified.toString());
        builder.startArray(CHUNKS_FIELD);
        for (SemanticTextField.Chunk chunk : chunks) {
            builder.startObject();
            builder.field(START_OFFSET_FIELD, chunk.startOffset());
            builder.field(END_OFFSET_FIELD, chunk.endOffset());
            XContentParser embeddingParser = XContentHelper.createParserNotCompressed(
                XContentParserConfiguration.EMPTY,
                chunk.rawEmbeddings(),
                XContentType.JSON
            );
            builder.field(EMBEDDINGS_FIELD).copyCurrentStructure(embeddingParser);
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    /**
     * Parses a {@link StagedSemanticField} from the current position of {@code parser}.
     *
     * @param parser the parser positioned at the start of the staged field object
     * @return the parsed instance
     * @throws IOException on parse error
     */
    public static StagedSemanticField fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<StagedSemanticField, Void> PARSER = new ConstructingObjectParser<>(
        "staged_semantic_field",
        true,
        args -> {
            String text = (String) args[0];
            Integer textLength = (Integer) args[1];
            String lastModifiedStr = (String) args[2];
            List<SemanticTextField.Chunk> chunks = (List<SemanticTextField.Chunk>) args[3];
            if (text == null) {
                throw new IllegalArgumentException("Missing required field [" + TEXT_FIELD + "]");
            }
            return new StagedSemanticField(
                text,
                textLength != null ? textLength : text.length(),
                Instant.parse(lastModifiedStr),
                chunks != null ? chunks : List.of()
            );
        }
    );

    private static final ConstructingObjectParser<SemanticTextField.Chunk, Void> CHUNK_PARSER = new ConstructingObjectParser<>(
        "staged_semantic_field_chunk",
        true,
        args -> {
            Integer startOffset = (Integer) args[0];
            Integer endOffset = (Integer) args[1];
            BytesReference rawEmbeddings = (BytesReference) args[2];
            return new SemanticTextField.Chunk(startOffset, endOffset, rawEmbeddings);
        }
    );

    static {
        PARSER.declareString(constructorArg(), new ParseField(TEXT_FIELD));
        PARSER.declareInt(optionalConstructorArg(), new ParseField(TEXT_LENGTH_FIELD));
        PARSER.declareString(constructorArg(), new ParseField(LAST_MODIFIED_FIELD));
        PARSER.declareObjectArray(optionalConstructorArg(), CHUNK_PARSER, new ParseField(CHUNKS_FIELD));

        CHUNK_PARSER.declareInt(constructorArg(), new ParseField(START_OFFSET_FIELD));
        CHUNK_PARSER.declareInt(constructorArg(), new ParseField(END_OFFSET_FIELD));
        CHUNK_PARSER.declareField(constructorArg(), (p, c) -> {
            XContentBuilder b = XContentBuilder.builder(p.contentType().xContent());
            b.copyCurrentStructure(p);
            return BytesReference.bytes(b);
        }, new ParseField(EMBEDDINGS_FIELD), ObjectParser.ValueType.OBJECT_ARRAY);
    }
}
