/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.mapper.IgnoredSourceFieldMapper;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.support.MapXContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.inference.mapper.EmbeddingsSemanticFieldValueFetcher.advanceToEmbeddingsValue;
import static org.elasticsearch.xpack.inference.mapper.EmbeddingsSemanticFieldValueFetcher.parseEmbeddings;
import static org.elasticsearch.xpack.inference.mapper.SemanticTextField.CHUNKED_EMBEDDINGS_FIELD;

/**
 * A {@link ValueFetcher} for the {@code embeddings} fetch format on legacy-format {@code semantic_text} fields.
 * <p>
 * In the legacy format, sparse-vector embeddings are not stored as Lucene stored fields (they are indexed but not stored,
 * so they cannot be recovered via the synthetic-field-loader path used by {@link EmbeddingsSemanticFieldValueFetcher}).
 * Instead, the full {@code {text, inference: {...}}} object is kept verbatim in the document's stored {@code _source},
 * and the embeddings are extracted directly from there.
 * </p>
 */
class LegacyEmbeddingsSemanticFieldValueFetcher implements ValueFetcher {
    private final SemanticFieldMapper.SemanticFieldType fieldType;
    private final IgnoredSourceFieldMapper.IgnoredSourceFormat ignoredSourceFormat;

    LegacyEmbeddingsSemanticFieldValueFetcher(
        SemanticFieldMapper.SemanticFieldType fieldType,
        IgnoredSourceFieldMapper.IgnoredSourceFormat ignoredSourceFormat
    ) {
        this.fieldType = fieldType;
        this.ignoredSourceFormat = ignoredSourceFormat;
    }

    @Override
    public List<Object> fetchValues(Source source, int doc, List<Object> ignoredValues) throws IOException {
        if (fieldType.getModelSettings() == null) {
            return List.of();
        }

        List<Map<?, ?>> nestedSources = XContentMapValues.extractNestedSources(fieldType.getChunksField().fullPath(), source.source());
        if (nestedSources == null) {
            return List.of();
        }

        List<Object> embeddings = new ArrayList<>(nestedSources.size());
        for (Map<?, ?> chunk : nestedSources) {
            Object rawEmbeddings = chunk.get(CHUNKED_EMBEDDINGS_FIELD);
            if (rawEmbeddings == null) {
                throw new IllegalStateException("Chunk is missing value for [" + CHUNKED_EMBEDDINGS_FIELD + "] field");
            }
            embeddings.add(readParsedEmbeddings(rawEmbeddings, source.sourceContentType(), fieldType));
        }
        return embeddings;
    }

    @Override
    public StoredFieldsSpec storedFieldsSpec() {
        // Legacy-format semantic_text has no native synthetic source support, so under synthetic source the field's value is recovered
        // from ignored source.
        return StoredFieldsSpec.withSourcePaths(ignoredSourceFormat, Set.of(fieldType.name()));
    }

    private static Object readParsedEmbeddings(
        Object rawEmbeddings,
        XContentType xContentType,
        SemanticFieldMapper.SemanticFieldType fieldType
    ) throws IOException {
        // MapXContentParser is map-backed, so the value is wrapped in a single-entry map. The key is skipped by
        // advanceToEmbeddingsValue and is never read.
        try (
            XContentParser parser = new MapXContentParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.IGNORE_DEPRECATIONS,
                Map.of(CHUNKED_EMBEDDINGS_FIELD, rawEmbeddings),
                xContentType
            )
        ) {
            advanceToEmbeddingsValue(parser);
            return parseEmbeddings(parser, fieldType);
        }
    }
}
