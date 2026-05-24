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
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Carries chunk-scoring configuration from the query rewrite phase to the fetch phase.
 * Each entry holds the field name, optional {@code min_score}, and optional {@code chunks_per_doc}
 * for a single semantic query. Multiple entries are supported for compound queries containing
 * several semantic sub-queries.
 *
 * <p>Instances are registered on {@link org.elasticsearch.index.query.QueryRewriteContext#addRewriteSearchExt}
 * during {@code SemanticQueryBuilder} rewrite, then propagated to the search context by
 * {@code SearchService}. The fetch sub-phase reads them via
 * {@link org.elasticsearch.search.fetch.FetchContext#getSearchExt}.
 */
public class SemanticChunksExtBuilder extends SearchExtBuilder {

    public static final String NAME = "semantic_chunks";

    private final List<ChunkConfig> configs;

    /**
     * A single chunk-scoring configuration for one semantic field.
     */
    public record ChunkConfig(String fieldName, @Nullable Float minScore, @Nullable Integer chunksPerDoc) {
        public ChunkConfig {
            if (fieldName == null) {
                throw new IllegalArgumentException("[" + NAME + "] requires a field_name value");
            }
            if (minScore != null && minScore < 0) {
                throw new IllegalArgumentException("[" + NAME + "] min_score must be non-negative, got [" + minScore + "]");
            }
            if (chunksPerDoc != null && chunksPerDoc < 1) {
                throw new IllegalArgumentException("[" + NAME + "] chunks_per_doc must be at least 1, got [" + chunksPerDoc + "]");
            }
        }
    }

    public SemanticChunksExtBuilder(List<ChunkConfig> configs) {
        if (configs == null || configs.isEmpty()) {
            throw new IllegalArgumentException("[" + NAME + "] requires at least one chunk config");
        }
        this.configs = List.copyOf(configs);
    }

    /**
     * Creates an ext builder with a single chunk config. Used during query rewrite when a single
     * {@code SemanticQueryBuilder} registers its config. Multiple single-config builders are
     * merged via {@link #merge(SemanticChunksExtBuilder)}.
     */
    public SemanticChunksExtBuilder(String fieldName, @Nullable Float minScore, @Nullable Integer chunksPerDoc) {
        this(List.of(new ChunkConfig(fieldName, minScore, chunksPerDoc)));
    }

    public SemanticChunksExtBuilder(StreamInput in) throws IOException {
        int size = in.readVInt();
        List<ChunkConfig> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            list.add(new ChunkConfig(in.readString(), in.readOptionalFloat(), in.readOptionalInt()));
        }
        this.configs = Collections.unmodifiableList(list);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(configs.size());
        for (ChunkConfig config : configs) {
            out.writeString(config.fieldName());
            out.writeOptionalFloat(config.minScore());
            out.writeOptionalInt(config.chunksPerDoc());
        }
    }

    public List<ChunkConfig> configs() {
        return configs;
    }

    /**
     * Returns a new ext builder that contains all configs from this builder and the other.
     * Used to accumulate configs when multiple semantic queries register exts during rewrite.
     */
    public SemanticChunksExtBuilder merge(SemanticChunksExtBuilder other) {
        List<ChunkConfig> merged = new ArrayList<>(this.configs.size() + other.configs.size());
        merged.addAll(this.configs);
        merged.addAll(other.configs);
        return new SemanticChunksExtBuilder(merged);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startArray("configs");
        for (ChunkConfig config : configs) {
            builder.startObject();
            builder.field("field_name", config.fieldName());
            if (config.minScore() != null) {
                builder.field("min_score", config.minScore());
            }
            if (config.chunksPerDoc() != null) {
                builder.field("chunks_per_doc", config.chunksPerDoc());
            }
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    public static SemanticChunksExtBuilder fromXContent(XContentParser parser) throws IOException {
        // This ext builder is not expected to be parsed from XContent in the REST layer.
        // It is only created internally during query rewrite.
        throw new UnsupportedOperationException("[" + NAME + "] is not parseable from XContent");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SemanticChunksExtBuilder that = (SemanticChunksExtBuilder) o;
        return Objects.equals(configs, that.configs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(configs);
    }
}
