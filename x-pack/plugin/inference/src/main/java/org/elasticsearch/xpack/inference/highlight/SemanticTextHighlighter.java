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
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.subphase.highlight.DefaultHighlighter;
import org.elasticsearch.search.fetch.subphase.highlight.FieldHighlightContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightUtils;
import org.elasticsearch.search.fetch.subphase.highlight.Highlighter;
import org.elasticsearch.xcontent.Text;
import org.elasticsearch.xpack.inference.mapper.SemanticFieldMapper.SemanticFieldType;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper.SemanticTextFieldType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.lucene.search.uhighlight.CustomUnifiedHighlighter.MULTIVAL_SEP_CHAR;
import static org.elasticsearch.xpack.inference.common.chunks.SemanticTextChunkUtils.OffsetAndScore;
import static org.elasticsearch.xpack.inference.common.chunks.SemanticTextChunkUtils.extractOffsetAndScores;
import static org.elasticsearch.xpack.inference.common.chunks.SemanticTextChunkUtils.extractQueries;
import static org.elasticsearch.xpack.inference.common.chunks.SemanticTextChunkUtils.getContentFromLegacyNestedSources;

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
        Text[] snippets = new Text[size];
        final Function<OffsetAndScore, String> offsetToContent;
        if (fieldType instanceof SemanticTextFieldType stft && stft.useLegacyFormat()) {
            List<Map<?, ?>> nestedSources = XContentMapValues.extractNestedSources(
                fieldType.getChunksField().fullPath(),
                fieldContext.hitContext.source().source()
            );
            offsetToContent = entry -> getContentFromLegacyNestedSources(fieldType.name(), entry, nestedSources);
        } else {
            Map<String, SemanticFieldContent> fieldToContent = new HashMap<>();
            offsetToContent = entry -> {
                SemanticFieldContent content = fieldToContent.computeIfAbsent(entry.offset().field(), key -> {
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

                String offsetContent;
                if (entry.offset().inputIndex() != null) {
                    InferenceString inferenceString = content.inferenceStringValues().get(entry.offset().inputIndex());
                    if (inferenceString == null) {
                        throw new IllegalStateException(
                            "Invalid content detected for field ["
                                + entry.offset().field()
                                + "]: missing InferenceString value at index ["
                                + entry.offset().inputIndex()
                                + "]"
                        );
                    }
                    offsetContent = content.inferenceStringValues().get(entry.offset().inputIndex()).value();
                } else {
                    if (content.textValues().length() < entry.offset().end()) {
                        throw new IllegalStateException(
                            "Invalid content detected for field ["
                                + entry.offset().field()
                                + "]: missing text for the chunk at offset ["
                                + entry.offset().start()
                                + ", "
                                + entry.offset().end()
                                + "]"
                        );
                    }
                    offsetContent = content.textValues().substring(entry.offset().start(), entry.offset().end());
                }
                return offsetContent;
            };
        }
        for (int i = 0; i < size; i++) {
            var chunk = chunks.get(i);
            String content = offsetToContent.apply(chunk);
            if (content == null) {
                throw new IllegalStateException(
                    "Invalid content detected for field [" + fieldType.name() + "]: missing text for the chunk " + chunk
                );
            }
            snippets[i] = new Text(content);
        }
        return new HighlightField(fieldContext.fieldName, snippets);
    }

    private SemanticFieldContent extractFieldContent(
        SearchExecutionContext searchContext,
        FetchSubPhase.HitContext hitContext,
        String sourceField
    ) throws IOException {
        var sourceFieldType = searchContext.getMappingLookup().getFieldType(sourceField);
        if (sourceFieldType == null) {
            throw new IllegalStateException("Field [" + sourceField + "] is not mapped");
        }

        List<Object> textValues = new ArrayList<>();
        Map<Integer, InferenceString> inferenceStringValues = new HashMap<>();
        List<Object> rawFieldValues = HighlightUtils.loadFieldValues(sourceFieldType, searchContext, hitContext);

        int valueIndex = 0;
        for (Object rawFieldValue : rawFieldValues) {
            if (rawFieldValue instanceof InferenceString inferenceString) {
                inferenceStringValues.put(valueIndex, inferenceString);
            } else {
                textValues.add(DefaultHighlighter.convertFieldValue(sourceFieldType, rawFieldValue));
            }

            valueIndex++;
        }

        return new SemanticFieldContent(DefaultHighlighter.mergeFieldValues(textValues, MULTIVAL_SEP_CHAR), inferenceStringValues);
    }

    private record SemanticFieldContent(String textValues, Map<Integer, InferenceString> inferenceStringValues) {
        private SemanticFieldContent {
            Objects.requireNonNull(textValues);
            Objects.requireNonNull(inferenceStringValues);
        }
    }
}
