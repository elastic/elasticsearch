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
import org.elasticsearch.index.codec.vectors.es93.ES93GenericFlatVectorsFormat;

import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.RESCORE_VECTOR_QUANTIZED_VECTOR_MAPPING;
import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.RESCORE_ZERO_VECTOR_QUANTIZED_VECTOR_MAPPING;
import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.USE_DEFAULT_OVERSAMPLE_VALUE_FOR_BBQ;
import static org.elasticsearch.index.mapper.vectors.SparseVectorFieldMapper.SPARSE_VECTOR_INDEX_OPTIONS_FEATURE;

/**
 * Spec for mapper-related features.
 */
public class MapperFeatures implements FeatureSpecification {

    public static final NodeFeature CONSTANT_KEYWORD_SYNTHETIC_SOURCE_WRITE_FIX = new NodeFeature(
        "mapper.constant_keyword.synthetic_source_write_fix"
    );

    public static final NodeFeature COUNTED_KEYWORD_SYNTHETIC_SOURCE_NATIVE_SUPPORT = new NodeFeature(
        "mapper.counted_keyword.synthetic_source_native_support"
    );

    public static final NodeFeature TSDB_NESTED_FIELD_SUPPORT = new NodeFeature("mapper.tsdb_nested_field_support");
    public static final NodeFeature META_FETCH_FIELDS_ERROR_CODE_CHANGED = new NodeFeature("meta_fetch_fields_error_code_changed");
    public static final NodeFeature SPARSE_VECTOR_STORE_SUPPORT = new NodeFeature("mapper.sparse_vector.store_support");
    public static final NodeFeature SORT_FIELDS_CHECK_FOR_NESTED_OBJECT_FIX = new NodeFeature("mapper.nested.sorting_fields_check_fix");
    public static final NodeFeature DYNAMIC_HANDLING_IN_COPY_TO = new NodeFeature("mapper.copy_to.dynamic_handling");
    public static final NodeFeature DOC_VALUES_SKIPPER = new NodeFeature("mapper.doc_values_skipper");
    public static final NodeFeature MATCH_ONLY_TEXT_BLOCK_LOADER_FIX = new NodeFeature("mapper.match_only_text_block_loader_fix");

    static final NodeFeature UKNOWN_FIELD_MAPPING_UPDATE_ERROR_MESSAGE = new NodeFeature(
        "mapper.unknown_field_mapping_update_error_message"
    );
    static final NodeFeature NPE_ON_DIMS_UPDATE_FIX = new NodeFeature("mapper.npe_on_dims_update_fix");
    static final NodeFeature IVF_FORMAT_CLUSTER_FEATURE = new NodeFeature("mapper.ivf_format_cluster_feature");
    static final NodeFeature IVF_NESTED_SUPPORT = new NodeFeature("mapper.ivf_nested_support");
    static final NodeFeature BBQ_DISK_SUPPORT = new NodeFeature("mapper.bbq_disk_support");
    static final NodeFeature SEARCH_LOAD_PER_SHARD = new NodeFeature("mapper.search_load_per_shard");
    static final NodeFeature PATTERN_TEXT = new NodeFeature("mapper.patterned_text");
    static final NodeFeature IGNORED_SOURCE_FIELDS_PER_ENTRY = new NodeFeature("mapper.ignored_source_fields_per_entry");
    static final NodeFeature MULTI_FIELD_UNICODE_OPTIMISATION_FIX = new NodeFeature("mapper.multi_field.unicode_optimisation_fix");
    static final NodeFeature PATTERN_TEXT_RENAME = new NodeFeature("mapper.pattern_text_rename");
    static final NodeFeature DISKBBQ_ON_DISK_RESCORING = new NodeFeature("mapper.vectors.diskbbq_on_disk_rescoring");
    static final NodeFeature PROVIDE_INDEX_SORT_SETTING_DEFAULTS = new NodeFeature("mapper.provide_index_sort_setting_defaults");
    static final NodeFeature INDEX_MAPPING_IGNORE_DYNAMIC_BEYOND_FIELD_NAME_LIMIT = new NodeFeature(
        "mapper.ignore_dynamic_field_names_beyond_limit"
    );
    static final NodeFeature EXCLUDE_VECTORS_DOCVALUE_BUGFIX = new NodeFeature("mapper.exclude_vectors_docvalue_bugfix");
    static final NodeFeature BASE64_DENSE_VECTORS = new NodeFeature("mapper.base64_dense_vectors");
    public static final NodeFeature GENERIC_VECTOR_FORMAT = new NodeFeature("mapper.vectors.generic_vector_format");
    public static final NodeFeature FIX_DENSE_VECTOR_WRONG_FIELDS = new NodeFeature("mapper.fix_dense_vector_wrong_fields");
    static final NodeFeature BBQ_DISK_STATS_SUPPORT = new NodeFeature("mapper.bbq_disk_stats_support");

    @Override
    public Set<NodeFeature> getTestFeatures() {
        var features = Set.of(
            RangeFieldMapper.DATE_RANGE_INDEXING_FIX,
            IgnoredSourceFieldMapper.DONT_EXPAND_DOTS_IN_IGNORED_SOURCE,
            SourceFieldMapper.REMOVE_SYNTHETIC_SOURCE_ONLY_VALIDATION,
            SourceFieldMapper.SOURCE_MODE_FROM_INDEX_SETTING,
            IgnoredSourceFieldMapper.IGNORED_SOURCE_AS_TOP_LEVEL_METADATA_ARRAY_FIELD,
            IgnoredSourceFieldMapper.ALWAYS_STORE_OBJECT_ARRAYS_IN_NESTED_OBJECTS,
            MapperService.LOGSDB_DEFAULT_IGNORE_DYNAMIC_BEYOND_LIMIT,
            DocumentParser.FIX_PARSING_SUBOBJECTS_FALSE_DYNAMIC_FALSE,
            CONSTANT_KEYWORD_SYNTHETIC_SOURCE_WRITE_FIX,
            META_FETCH_FIELDS_ERROR_CODE_CHANGED,
            SPARSE_VECTOR_STORE_SUPPORT,
            COUNTED_KEYWORD_SYNTHETIC_SOURCE_NATIVE_SUPPORT,
            SORT_FIELDS_CHECK_FOR_NESTED_OBJECT_FIX,
            DYNAMIC_HANDLING_IN_COPY_TO,
            TSDB_NESTED_FIELD_SUPPORT,
            SourceFieldMapper.SYNTHETIC_RECOVERY_SOURCE,
            ObjectMapper.SUBOBJECTS_FALSE_MAPPING_UPDATE_FIX,
            UKNOWN_FIELD_MAPPING_UPDATE_ERROR_MESSAGE,
            DOC_VALUES_SKIPPER,
            RESCORE_VECTOR_QUANTIZED_VECTOR_MAPPING,
            DateFieldMapper.INVALID_DATE_FIX,
            NPE_ON_DIMS_UPDATE_FIX,
            RESCORE_ZERO_VECTOR_QUANTIZED_VECTOR_MAPPING,
            USE_DEFAULT_OVERSAMPLE_VALUE_FOR_BBQ,
            IVF_FORMAT_CLUSTER_FEATURE,
            IVF_NESTED_SUPPORT,
            BBQ_DISK_SUPPORT,
            SEARCH_LOAD_PER_SHARD,
            SPARSE_VECTOR_INDEX_OPTIONS_FEATURE,
            PATTERN_TEXT,
            IGNORED_SOURCE_FIELDS_PER_ENTRY,
            MULTI_FIELD_UNICODE_OPTIMISATION_FIX,
            MATCH_ONLY_TEXT_BLOCK_LOADER_FIX,
            PATTERN_TEXT_RENAME,
            DISKBBQ_ON_DISK_RESCORING,
            PROVIDE_INDEX_SORT_SETTING_DEFAULTS,
            INDEX_MAPPING_IGNORE_DYNAMIC_BEYOND_FIELD_NAME_LIMIT,
            EXCLUDE_VECTORS_DOCVALUE_BUGFIX,
            BASE64_DENSE_VECTORS,
            FIX_DENSE_VECTOR_WRONG_FIELDS,
            BBQ_DISK_STATS_SUPPORT
        );
        if (ES93GenericFlatVectorsFormat.GENERIC_VECTOR_FORMAT.isEnabled()) {
            features = new HashSet<>(features);
            features.add(GENERIC_VECTOR_FORMAT);
        }
        return features;
    }
}
