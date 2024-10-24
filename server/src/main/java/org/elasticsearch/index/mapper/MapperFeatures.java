/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;

import java.util.Set;

/**
 * Spec for mapper-related features.
 */
public class MapperFeatures implements FeatureSpecification {

    // Used to avoid noise in mixed cluster and rest compatibility tests. Must not be backported to 8.x branch.
    // This label gets added to tests with such failures before merging with main, then removed when backported to 8.x.
    public static final NodeFeature BWC_WORKAROUND_9_0 = new NodeFeature("mapper.bwc_workaround_9_0");

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(
            BWC_WORKAROUND_9_0,
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
            ObjectMapper.SUBOBJECTS_AUTO_FIXES,
            KeywordFieldMapper.KEYWORD_NORMALIZER_SYNTHETIC_SOURCE,
            SourceFieldMapper.SYNTHETIC_SOURCE_STORED_FIELDS_ADVANCE_FIX,
            Mapper.SYNTHETIC_SOURCE_KEEP_FEATURE,
            SourceFieldMapper.SYNTHETIC_SOURCE_WITH_COPY_TO_AND_DOC_VALUES_FALSE_SUPPORT,
            SourceFieldMapper.SYNTHETIC_SOURCE_COPY_TO_FIX,
            FlattenedFieldMapper.IGNORE_ABOVE_SUPPORT,
            IndexSettings.IGNORE_ABOVE_INDEX_LEVEL_SETTING,
            SourceFieldMapper.SYNTHETIC_SOURCE_COPY_TO_INSIDE_OBJECTS_FIX,
            TimeSeriesRoutingHashFieldMapper.TS_ROUTING_HASH_FIELD_PARSES_BYTES_REF,
            FlattenedFieldMapper.IGNORE_ABOVE_WITH_ARRAYS_SUPPORT,
            DenseVectorFieldMapper.BBQ_FORMAT
        );
    }

    @Override
    public Set<NodeFeature> getTestFeatures() {
        return Set.of(
            RangeFieldMapper.DATE_RANGE_INDEXING_FIX,
            IgnoredSourceFieldMapper.DONT_EXPAND_DOTS_IN_IGNORED_SOURCE,
            SourceFieldMapper.REMOVE_SYNTHETIC_SOURCE_ONLY_VALIDATION,
            IgnoredSourceFieldMapper.ALWAYS_STORE_OBJECT_ARRAYS_IN_NESTED_OBJECTS
        );
    }
}
