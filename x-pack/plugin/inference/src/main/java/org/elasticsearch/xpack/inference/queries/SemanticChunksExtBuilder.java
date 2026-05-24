/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.SearchExtBuilder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Carries chunk-scoring configuration ({@code minScore} and {@code chunksPerDoc}) from the
 * search request to the fetch phase, where {@code SemanticChunksFetchSubPhase} uses it to
 * select and score individual semantic chunks per hit.
 */
public class SemanticChunksExtBuilder extends SearchExtBuilder {

    public static final String NAME = "semantic_chunks";

    private static final ParseField FIELD_NAME_FIELD = new ParseField("field_name");
    private static final ParseField MIN_SCORE_FIELD = new ParseField("min_score");
    private static final ParseField CHUNKS_PER_DOC_FIELD = new ParseField("chunks_per_doc");

    private static final ConstructingObjectParser<SemanticChunksExtBuilder, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        false,
        args -> new SemanticChunksExtBuilder((String) args[0], (Float) args[1], (Integer) args[2])
    );

    static {
        PARSER.declareString(constructorArg(), FIELD_NAME_FIELD);
        PARSER.declareFloat(optionalConstructorArg(), MIN_SCORE_FIELD);
        PARSER.declareInt(optionalConstructorArg(), CHUNKS_PER_DOC_FIELD);
    }

    private final String fieldName;
    @Nullable
    private final Float minScore;
    @Nullable
    private final Integer chunksPerDoc;

    public SemanticChunksExtBuilder(String fieldName, @Nullable Float minScore, @Nullable Integer chunksPerDoc) {
        if (fieldName == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires a field_name value");
        }
        if (minScore != null && minScore < 0) {
            throw new IllegalArgumentException("[" + NAME + "] min_score must be non-negative, got [" + minScore + "]");
        }
        if (chunksPerDoc != null && chunksPerDoc < 1) {
            throw new IllegalArgumentException("[" + NAME + "] chunks_per_doc must be at least 1, got [" + chunksPerDoc + "]");
        }
        this.fieldName = fieldName;
        this.minScore = minScore;
        this.chunksPerDoc = chunksPerDoc;
    }

    public SemanticChunksExtBuilder(StreamInput in) throws IOException {
        this.fieldName = in.readString();
        this.minScore = in.readOptionalFloat();
        this.chunksPerDoc = in.readOptionalInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeOptionalFloat(minScore);
        out.writeOptionalInt(chunksPerDoc);
    }

    public String fieldName() {
        return fieldName;
    }

    @Nullable
    public Float minScore() {
        return minScore;
    }

    @Nullable
    public Integer chunksPerDoc() {
        return chunksPerDoc;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(FIELD_NAME_FIELD.getPreferredName(), fieldName);
        if (minScore != null) {
            builder.field(MIN_SCORE_FIELD.getPreferredName(), minScore);
        }
        if (chunksPerDoc != null) {
            builder.field(CHUNKS_PER_DOC_FIELD.getPreferredName(), chunksPerDoc);
        }
        builder.endObject();
        return builder;
    }

    public static SemanticChunksExtBuilder fromXContent(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SemanticChunksExtBuilder that = (SemanticChunksExtBuilder) o;
        return Objects.equals(fieldName, that.fieldName)
            && Objects.equals(minScore, that.minScore)
            && Objects.equals(chunksPerDoc, that.chunksPerDoc);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, minScore, chunksPerDoc);
    }
}
