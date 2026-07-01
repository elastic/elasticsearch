/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.highlight;

import org.apache.lucene.search.Query;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.fetch.subphase.highlight.FieldHighlightContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.fetch.subphase.highlight.Highlighter;
import org.elasticsearch.xcontent.Text;
import org.elasticsearch.xpack.inference.mapper.SemanticFieldMapper.SemanticFieldType;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper.SemanticTextFieldType;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

import static org.elasticsearch.xpack.inference.common.chunks.SemanticTextChunkUtils.OffsetAndScore;
import static org.elasticsearch.xpack.inference.common.chunks.SemanticTextChunkUtils.extractOffsetAndScores;
import static org.elasticsearch.xpack.inference.common.chunks.SemanticTextChunkUtils.extractQueries;

/**
 * A {@link Highlighter} designed for the {@link SemanticTextFieldMapper}.
 * This highlighter extracts semantic queries and evaluates them against each chunk produced by the semantic text field.
 * It returns the top-scoring chunks as snippets, optionally sorted by their scores.
 */
public class SemanticTextHighlighter implements Highlighter {
    public static final String NAME = "semantic";

    @Override
    public boolean canHighlight(MappedFieldType fieldType) {
        return fieldType instanceof SemanticFieldType;
    }

    @Override
    public boolean canHighlightWithoutSource(MappedFieldType fieldType, SearchExecutionContext context) {
        if (canHighlight(fieldType) == false) {
            return false;
        }
        // Highlighting loads each inference source field; it can avoid _source only if every one is retrievable from doc values.
        // The source field is usually the semantic field itself, but copy_to can add others that still need _source.
        var inferenceField = context.getMappingLookup().inferenceFields().get(fieldType.name());
        if (inferenceField == null) {
            return false;
        }
        for (String sourceField : inferenceField.getSourceFields()) {
            MappedFieldType sourceFieldType = context.getFieldType(sourceField);
            if (sourceFieldType == null || sourceFieldType.valueFetcher(context, null).storedFieldsSpec().requiresSource()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public HighlightField highlight(FieldHighlightContext fieldContext) throws IOException {
        if (canHighlight(fieldContext.fieldType) == false) {
            return null;
        }
        SemanticFieldType fieldType = (SemanticFieldType) fieldContext.fieldType;
        if (fieldType.getModelSettings() == null || fieldType.getEmbeddingsField() == null) {
            // nothing indexed yet
            return null;
        }

        List<Query> queries = extractQueries(fieldType.getEmbeddingsField(), fieldType.getModelSettings().taskType(), fieldContext.query);
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

        ChunkContentExtractor contentExtractor = fieldType instanceof SemanticTextFieldType stft && stft.useLegacyFormat()
            ? new LegacyChunkContentExtractor(stft, fieldContext)
            : new SemanticChunkContentExtractor(fieldContext);
        Text[] snippets = new Text[size];
        for (int i = 0; i < size; i++) {
            var chunk = chunks.get(i);

            String content;
            try {
                content = contentExtractor.getContent(chunk);
            } catch (Exception e) {
                throw new IllegalStateException("Internal error encountered while highlighting on field [" + fieldType.name() + "]", e);
            }

            snippets[i] = new Text(content);
        }

        return new HighlightField(fieldContext.fieldName, snippets);
    }
}
