/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.highlight;

import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.search.fetch.subphase.highlight.FieldHighlightContext;
import org.elasticsearch.xpack.inference.common.chunks.SemanticTextChunkUtils;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;

import java.util.List;
import java.util.Map;

class LegacyChunkContentExtractor implements ChunkContentExtractor {
    private final SemanticTextFieldMapper.SemanticTextFieldType fieldType;
    private final FieldHighlightContext context;

    private List<Map<?, ?>> nestedSources;

    LegacyChunkContentExtractor(SemanticTextFieldMapper.SemanticTextFieldType fieldType, FieldHighlightContext context) {
        this.fieldType = fieldType;
        this.context = context;
    }

    @Override
    public String getContent(SemanticTextChunkUtils.OffsetAndScore chunk) {
        initializeNestedSources();
        return SemanticTextChunkUtils.getContentFromLegacyNestedSources(fieldType.name(), chunk, nestedSources);
    }

    private void initializeNestedSources() {
        if (nestedSources == null) {
            nestedSources = XContentMapValues.extractNestedSources(
                fieldType.getChunksField().fullPath(),
                context.hitContext.source().source()
            );

            if (nestedSources == null) {
                throw new IllegalStateException(
                    "No nested sources found for [" + fieldType.typeName() + "] field [" + fieldType.name() + "]"
                );
            }
        }
    }
}
