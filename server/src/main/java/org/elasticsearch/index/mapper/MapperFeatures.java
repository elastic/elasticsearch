/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;

import java.util.Set;

/**
 * Spec for mapper-related features.
 */
public class MapperFeatures implements FeatureSpecification {
    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(
            IgnoredSourceFieldMapper.TRACK_IGNORED_SOURCE,
            PassThroughObjectMapper.PASS_THROUGH_PRIORITY,
            RangeFieldMapper.NULL_VALUES_OFF_BY_ONE_FIX,
            SourceFieldMapper.SYNTHETIC_SOURCE_FALLBACK,
            DenseVectorFieldMapper.INT4_QUANTIZATION,
            DenseVectorFieldMapper.BIT_VECTORS,
            DocumentMapper.INDEX_SORTING_ON_NESTED,
            KeywordFieldMapper.KEYWORD_DIMENSION_IGNORE_ABOVE,
            IndexModeFieldMapper.QUERYING_INDEX_MODE,
            NodeMappingStats.SEGMENT_LEVEL_FIELDS_STATS,
            BooleanFieldMapper.BOOLEAN_DIMENSION,
            ObjectMapper.SUBOBJECTS_AUTO,
            SourceFieldMapper.SYNTHETIC_SOURCE_STORED_FIELDS_ADVANCE_FIX
        );
    }
}
