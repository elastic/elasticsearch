/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.highlight;

import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.subphase.highlight.FieldHighlightContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightUtils;
import org.elasticsearch.xpack.inference.common.chunks.SemanticTextChunkUtils;
import org.elasticsearch.xpack.inference.mapper.OffsetSourceFieldMapper;
import org.elasticsearch.xpack.inference.mapper.SemanticFieldContent;
import org.elasticsearch.xpack.inference.mapper.SemanticTextUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class SemanticChunkContentExtractor implements ChunkContentExtractor {
    private final FieldHighlightContext context;
    private final Map<String, SemanticFieldContent> fieldToContent;

    SemanticChunkContentExtractor(FieldHighlightContext context) {
        this.context = context;
        this.fieldToContent = new HashMap<>();
    }

    @Override
    public String getContent(SemanticTextChunkUtils.OffsetAndScore chunk) throws IOException {
        OffsetSourceFieldMapper.OffsetSource offset = chunk.offset();
        SemanticFieldContent content = fieldToContent.get(offset.field());
        if (content == null) {
            content = extractFieldContent(context.context.getSearchExecutionContext(), context.hitContext, offset.field());
            fieldToContent.put(offset.field(), content);
        }

        try {
            Object resolved = content.resolve(offset);

            if (resolved instanceof Map<?, ?> map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> stringKeyedMap = (Map<String, Object>) map;
                return SemanticTextUtils.parseInferenceStringValue(stringKeyedMap).value();
            } else if (resolved instanceof String string) {
                return string;
            } else {
                throw new IllegalStateException("Unexpected resolved data type [" + resolved.getClass() + "]");
            }
        } catch (Exception e) {
            throw new IllegalStateException("Invalid content detected for field [" + offset.field() + "]", e);
        }
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

        List<Object> rawFieldValues = HighlightUtils.loadFieldValues(sourceFieldType, searchContext, hitContext);
        return new SemanticFieldContent(rawFieldValues);
    }
}
