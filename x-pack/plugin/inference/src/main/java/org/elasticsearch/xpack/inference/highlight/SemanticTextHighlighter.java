/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.highlight;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.subphase.highlight.DefaultHighlighter;
import org.elasticsearch.search.fetch.subphase.highlight.FieldHighlightContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightUtils;
import org.elasticsearch.search.fetch.subphase.highlight.Highlighter;
import org.elasticsearch.xcontent.Text;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper.SemanticTextFieldType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.lucene.search.uhighlight.CustomUnifiedHighlighter.MULTIVAL_SEP_CHAR;
import static org.elasticsearch.xpack.inference.common.chunks.SemanticTextChunkUtils.OffsetAndScore;
import static org.elasticsearch.xpack.inference.common.chunks.SemanticTextChunkUtils.extractOffsetAndScores;
import static org.elasticsearch.xpack.inference.common.chunks.SemanticTextChunkUtils.getContentFromLegacyNestedSources;
import static org.elasticsearch.xpack.inference.common.chunks.SemanticTextChunkUtils.queries;

/**
 * A {@link Highlighter} designed for the {@link SemanticTextFieldMapper}.
 * This highlighter extracts semantic queries and evaluates them against each chunk produced by the semantic text field.
 * It returns the top-scoring chunks as snippets, optionally sorted by their scores.
 */
public class SemanticTextHighlighter implements Highlighter {
    public static final String NAME = "semantic";

    @Override
    public boolean canHighlight(MappedFieldType fieldType) {
        return fieldType instanceof SemanticTextFieldType;
    }

    @Override
    public HighlightField highlight(FieldHighlightContext fieldContext) throws IOException {
        if (canHighlight(fieldContext.fieldType) == false) {
            return null;
        }
        SemanticTextFieldType fieldType = (SemanticTextFieldType) fieldContext.fieldType;
        if (fieldType.getModelSettings() == null || fieldType.getEmbeddingsField() == null) {
            // nothing indexed yet
            return null;
        }

        List<Query> queries = queries(fieldType.getEmbeddingsField(), fieldType.getModelSettings().taskType(), fieldContext.query);
        if (queries.isEmpty()) {
            // nothing to highlight
            return null;
        }

        int numberOfFragments = fieldContext.field.fieldOptions().numberOfFragments() <= 0
            ? 1 // we return the best fragment by default
            : fieldContext.field.fieldOptions().numberOfFragments();

        List<OffsetAndScore> chunks = extractOffsetAndScores(
            fieldContext.context.getSearchExecutionContext(),
            fieldContext.hitContext.reader(),
            fieldType,
            fieldContext.hitContext.docId(),
            queries
        );
        if (chunks.size() == 0) {
            return null;
        }

        chunks.sort(Comparator.comparingDouble(OffsetAndScore::score).reversed());
        int size = Math.min(chunks.size(), numberOfFragments);
        if (fieldContext.field.fieldOptions().scoreOrdered() == false) {
            chunks = chunks.subList(0, size);
            chunks.sort(Comparator.comparingInt(OffsetAndScore::index));
        }
        Text[] snippets = new Text[size];
        final Function<OffsetAndScore, String> offsetToContent;
        if (fieldType.useLegacyFormat()) {
            List<Map<?, ?>> nestedSources = XContentMapValues.extractNestedSources(
                fieldType.getChunksField().fullPath(),
                fieldContext.hitContext.source().source()
            );
            offsetToContent = entry -> getContentFromLegacyNestedSources(fieldType.name(), entry, nestedSources);
        } else {
            Map<String, String> fieldToContent = new HashMap<>();
            offsetToContent = entry -> {
                String content = fieldToContent.computeIfAbsent(entry.offset().field(), key -> {
                    try {
                        return extractFieldContent(
                            fieldContext.context.getSearchExecutionContext(),
                            fieldContext.hitContext,
                            entry.offset().field()
                        );
                    } catch (IOException e) {
                        throw new UncheckedIOException("Error extracting field content from field " + entry.offset().field(), e);
                    }
                });
                return content.substring(entry.offset().start(), entry.offset().end());
            };
        }
        for (int i = 0; i < size; i++) {
            var chunk = chunks.get(i);
            String content = offsetToContent.apply(chunk);
            if (content == null) {
                throw new IllegalStateException(
                    String.format(
                        Locale.ROOT,
                        "Invalid content detected for field [%s]: missing text for the chunk at offset [%d].",
                        fieldType.name(),
                        chunk.offset().start()
                    )
                );
            }
            snippets[i] = new Text(content);
        }
        return new HighlightField(fieldContext.fieldName, snippets);
    }

    private String extractFieldContent(SearchExecutionContext searchContext, FetchSubPhase.HitContext hitContext, String sourceField)
        throws IOException {
        var sourceFieldType = searchContext.getMappingLookup().getFieldType(sourceField);
        if (sourceFieldType == null) {
            return null;
        }

        var values = HighlightUtils.loadFieldValues(sourceFieldType, searchContext, hitContext)
            .stream()
            .<Object>map((s) -> DefaultHighlighter.convertFieldValue(sourceFieldType, s))
            .toList();
        if (values.size() == 0) {
            return null;
        }
        return DefaultHighlighter.mergeFieldValues(values, MULTIVAL_SEP_CHAR);
    }
}
