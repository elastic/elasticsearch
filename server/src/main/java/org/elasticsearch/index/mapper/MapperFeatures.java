/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.Build;
import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.HashSet;
import java.util.Set;

/**
 * Spec for mapper-related features.
 */
public class MapperFeatures implements FeatureSpecification {

    // Features moved from their source mapper classes: declarations live here so that
    // all mapper NodeFeatures are registered in one place.
    public static final NodeFeature DATE_RANGE_INDEXING_FIX = new NodeFeature("mapper.range.date_range_indexing_fix");
    public static final NodeFeature DONT_EXPAND_DOTS_IN_IGNORED_SOURCE = new NodeFeature("mapper.ignored_source.dont_expand_dots");
    public static final NodeFeature IGNORED_SOURCE_AS_TOP_LEVEL_METADATA_ARRAY_FIELD = new NodeFeature(
        "mapper.ignored_source_as_top_level_metadata_array_field"
    );
    public static final NodeFeature ALWAYS_STORE_OBJECT_ARRAYS_IN_NESTED_OBJECTS = new NodeFeature(
        "mapper.ignored_source.always_store_object_arrays_in_nested"
    );
    public static final NodeFeature REMOVE_SYNTHETIC_SOURCE_ONLY_VALIDATION = new NodeFeature(
        "mapper.source.remove_synthetic_source_only_validation"
    );
    public static final NodeFeature SOURCE_MODE_FROM_INDEX_SETTING = new NodeFeature("mapper.source.mode_from_index_setting");
    public static final NodeFeature SYNTHETIC_RECOVERY_SOURCE = new NodeFeature("mapper.synthetic_recovery_source");
    public static final NodeFeature LOGSDB_DEFAULT_IGNORE_DYNAMIC_BEYOND_LIMIT = new NodeFeature(
        "mapper.logsdb_default_ignore_dynamic_beyond_limit"
    );
    public static final NodeFeature FIX_PARSING_SUBOBJECTS_FALSE_DYNAMIC_FALSE = new NodeFeature(
        "mapper.fix_parsing_subobjects_false_dynamic_false"
    );
    public static final NodeFeature SUBOBJECTS_FALSE_MAPPING_UPDATE_FIX = new NodeFeature("mapper.subobjects_false_mapping_update_fix");
    public static final NodeFeature INVALID_DATE_FIX = new NodeFeature("mapper.range.invalid_date_fix");
    public static final NodeFeature ROUTING_AS_DOC_VALUES = new NodeFeature("mapper.routing_as_doc_values");
    public static final NodeFeature ROUTING_AS_DOC_VALUES_BY_DEFAULT = new NodeFeature("mapper.routing_as_doc_values_by_default");
    public static final NodeFeature ID_FIELD_MODE_MAPPING_ATTRIBUTE = new NodeFeature("mapper.id_field.mode_mapping_attribute");
    public static final NodeFeature FLATTENED_MAPPED_SUBFIELDS_FEATURE = new NodeFeature("mapper.flattened.mapped_subfields");
    public static final NodeFeature FLATTENED_PASSTHROUGH_FEATURE = new NodeFeature("mapper.flattened.passthrough");
    public static final NodeFeature FLATTENED_COLUMNAR_DOCUMENT_ORDER = new NodeFeature("mapper.flattened.columnar_document_order");
    public static final NodeFeature RESCORE_VECTOR_QUANTIZED_VECTOR_MAPPING = new NodeFeature("mapper.dense_vector.rescore_vector");
    public static final NodeFeature RESCORE_ZERO_VECTOR_QUANTIZED_VECTOR_MAPPING = new NodeFeature(
        "mapper.dense_vector.rescore_zero_vector"
    );
    public static final NodeFeature USE_DEFAULT_OVERSAMPLE_VALUE_FOR_BBQ = new NodeFeature(
        "mapper.dense_vector.default_oversample_value_for_bbq"
    );
    public static final NodeFeature SPARSE_VECTOR_INDEX_OPTIONS_FEATURE = new NodeFeature("sparse_vector.index_options_supported");

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
    public static final NodeFeature MATCH_ONLY_TEXT_DOC_VALUES_PREFIX_WILDCARD_REGEXP = new NodeFeature(
        "mapper.match_only_text.doc_values_prefix_wildcard_regexp"
    );

    public static final NodeFeature UKNOWN_FIELD_MAPPING_UPDATE_ERROR_MESSAGE = new NodeFeature(
        "mapper.unknown_field_mapping_update_error_message"
    );
    public static final NodeFeature NPE_ON_DIMS_UPDATE_FIX = new NodeFeature("mapper.npe_on_dims_update_fix");
    public static final NodeFeature IVF_FORMAT_CLUSTER_FEATURE = new NodeFeature("mapper.ivf_format_cluster_feature");
    public static final NodeFeature IVF_NESTED_SUPPORT = new NodeFeature("mapper.ivf_nested_support");
    public static final NodeFeature BBQ_DISK_SUPPORT = new NodeFeature("mapper.bbq_disk_support");
    public static final NodeFeature BBQ_DISK_BYTE_SUPPORT = new NodeFeature("mapper.bbq_disk_byte_support");
    public static final NodeFeature ASH_QUANTIZATION_TYPE_SUPPORT = new NodeFeature("mapper.ash_quantization_type_support");
    public static final NodeFeature SEARCH_LOAD_PER_SHARD = new NodeFeature("mapper.search_load_per_shard");
    public static final NodeFeature PATTERN_TEXT = new NodeFeature("mapper.patterned_text");
    public static final NodeFeature IGNORED_SOURCE_FIELDS_PER_ENTRY = new NodeFeature("mapper.ignored_source_fields_per_entry");
    public static final NodeFeature MULTI_FIELD_UNICODE_OPTIMISATION_FIX = new NodeFeature("mapper.multi_field.unicode_optimisation_fix");
    public static final NodeFeature PATTERN_TEXT_RENAME = new NodeFeature("mapper.pattern_text_rename");
    public static final NodeFeature DISKBBQ_ON_DISK_RESCORING = new NodeFeature("mapper.vectors.diskbbq_on_disk_rescoring");
    public static final NodeFeature PROVIDE_INDEX_SORT_SETTING_DEFAULTS = new NodeFeature("mapper.provide_index_sort_setting_defaults");
    public static final NodeFeature INDEX_MAPPING_IGNORE_DYNAMIC_BEYOND_FIELD_NAME_LIMIT = new NodeFeature(
        "mapper.ignore_dynamic_field_names_beyond_limit"
    );
    public static final NodeFeature EXCLUDE_VECTORS_DOCVALUE_BUGFIX = new NodeFeature("mapper.exclude_vectors_docvalue_bugfix");
    public static final NodeFeature BASE64_DENSE_VECTORS = new NodeFeature("mapper.base64_dense_vectors");
    public static final NodeFeature GENERIC_VECTOR_FORMAT = new NodeFeature("mapper.vectors.generic_vector_format");
    public static final NodeFeature FIX_DENSE_VECTOR_WRONG_FIELDS = new NodeFeature("mapper.fix_dense_vector_wrong_fields");
    public static final NodeFeature BBQ_DISK_STATS_SUPPORT = new NodeFeature("mapper.bbq_disk_stats_support");
    public static final NodeFeature SKIPPERS_ON_UNINDEXED_FIELDS = new NodeFeature("mapper.skippers_on_unindexed_fields");
    public static final NodeFeature STORED_FIELDS_SPEC_MERGE_BUG = new NodeFeature("mapper.stored_fields_spec_merge_bug");
    public static final NodeFeature EXPONENTIAL_HISTOGRAM_TYPE = new NodeFeature("mapper.exponential_histogram_type");
    public static final NodeFeature STORE_HIGH_CARDINALITY_KEYWORDS_IN_BINARY_DOC_VALUES = new NodeFeature(
        "mapper.keyword.store_high_cardinality_in_binary_doc_values"
    );
    public static final NodeFeature HIGH_CARDINALITY_LENGTH_FUNCTION_FUSE_TO_LOAD = new NodeFeature(
        "mapper.keyword.high_cardinality_length_function_fuse_to_load"
    );
    public static final NodeFeature MV_MIN_FUNCTION_FUSE_TO_LOAD = new NodeFeature("mapper.keyword.mv_min_function_fuse_to_load");
    public static final NodeFeature MV_MAX_FUNCTION_FUSE_TO_LOAD = new NodeFeature("mapper.keyword.mv_max_function_fuse_to_load");
    public static final NodeFeature TDIGEST_TYPE = new NodeFeature("mapper.tdigest_type");
    public static final NodeFeature TEXT_FIELD_DOC_VALUES = new NodeFeature("mapper.text.doc_values");
    public static final NodeFeature TEXT_FIELD_DOC_VALUES_PREFIX_WILDCARD_REGEXP = new NodeFeature(
        "mapper.text.doc_values_prefix_wildcard_regexp"
    );
    public static final NodeFeature DENSE_VECTOR_DYNAMIC_TEMPLATE_DOTTED_FIELD_FIX = new NodeFeature(
        "mapper.dense_vector.dynamic_template_dotted_field_fix"
    );
    public static final NodeFeature DOC_VALUES_MULTI_VALUE = new NodeFeature("mapper.doc_values.multi_value");
    public static final NodeFeature DOC_VALUES_MULTI_VALUE_ENFORCEMENT = new NodeFeature("mapper.doc_values.multi_value_enforcement");
    public static final NodeFeature DOC_VALUES_MULTI_VALUE_RENAME = new NodeFeature("mapper.doc_values.multi_value_rename");
    public static final NodeFeature DOC_VALUES_MULTI_VALUE_INDEX_SETTING = new NodeFeature("mapper.doc_values.multi_value_index_setting");
    public static final NodeFeature DOC_VALUES_MULTI_VALUE_FALSE_ALIAS = new NodeFeature("mapper.doc_values.multi_value_false_alias");
    public static final NodeFeature DOC_VALUES_EXTENDED_FORM_ONLY_IN_COLUMNAR = new NodeFeature(
        "mapper.doc_values.extended_form_only_in_columnar"
    );
    public static final NodeFeature DOC_VALUES_NULLABILITY = new NodeFeature("mapper.doc_values.nullability");
    public static final NodeFeature DENSE_VECTOR_DYNAMIC_TEMPLATE_NESTED_OBJECT_FIX = new NodeFeature(
        "mapper.dense_vector.dynamic_template_nested_object_fix"
    );
    public static final NodeFeature ARRAY_OBJECTS_LIMIT = new NodeFeature("mapper.array_objects_limit");
    public static final NodeFeature ES940_DISK_BBQ = new NodeFeature("mapper.es940_disk_bbq");
    public static final NodeFeature IP_MAPPER_CARDINALITY_OPTION = new NodeFeature("mapper.ip.doc_values_cardinality_option");
    public static final NodeFeature IGNORED_VALUES_STORED_IN_BINARY_DV = new NodeFeature("mapper.doc_values.ignored_values_in_binary_dv");
    public static final NodeFeature KEYWORD_NORMALIZER_SKIP_STORE_SETTING = new NodeFeature("mapper.keyword.normalizer_skip_store_setting");
    public static final NodeFeature KEYWORD_MULTI_FIELDS_NOT_STORED_WHEN_IGNORED = new NodeFeature(
        "mapper.keyword.multi_fields_not_stored_when_ignored"
    );
    public static final NodeFeature ANALYZER_WRAPPER_RELOADABLE_SEARCH_ANALYZER = new NodeFeature(
        "mapper.analyzer-wrapper.reloadable_search_analyzer"
    );
    public static final NodeFeature STORE_NOT_ALLOWED_IN_COLUMNAR_INDEX_MODE = new NodeFeature("mapper.columnar.store_not_allowed");
    public static final NodeFeature KEYWORD_DV_CASE_INSENSITIVE_REGEXP = new NodeFeature(
        "mapper.keyword.doc_values_case_insensitive_regexp"
    );
    public static final NodeFeature COLUMNAR_REJECTS_RUNTIME_DYNAMIC = new NodeFeature("mapper.columnar_rejects_runtime_dynamic");
    public static final NodeFeature COLUMNAR_ACCEPTS_SUBOBJECTS_FALSE = new NodeFeature("mapper.columnar.accepts_subobjects_false");
    public static final NodeFeature COLUMNAR_MAINTAIN_ARRAY_ORDER = new NodeFeature("mapper.columnar.maintain_array_order");
    public static final NodeFeature KEYWORD_COLUMNAR_DEFAULT_HIGH_CARDINALITY = new NodeFeature(
        "mapper.keyword.columnar_default_high_cardinality"
    );
    public static final NodeFeature TEXT_FIELDS_ENABLE_DOC_VALUES_BY_DEFAULT_IN_COLUMNAR_MODE = new NodeFeature(
        "mapper.text_fields.enable_doc_values_by_default_in_columnar_mode"
    );
    public static final NodeFeature COLUMNAR_MAINTAIN_ARRAY_ORDER_IP_TEXT = new NodeFeature("mapper.columnar.maintain_array_order_ip_text");
    public static final NodeFeature COLUMNAR_INLINE_ARRAY_ORDER_BINARY_DOC_VALUES = new NodeFeature(
        "mapper.columnar.inline_array_order_binary_doc_values"
    );
    public static final NodeFeature COLUMNAR_IP_INLINE_ARRAY_ORDER_BINARY_DOC_VALUES = new NodeFeature(
        "mapper.columnar.ip_inline_array_order_binary_doc_values"
    );
    public static final NodeFeature COLUMNAR_DROPS_DYNAMIC_FALSE_FIELDS = new NodeFeature("mapper.columnar.drops_dynamic_false_fields");
    public static final NodeFeature COLUMNAR_SUPPORTS_SHAPE_FIELDS = new NodeFeature("mapper.columnar.supports_shape_fields");
    public static final NodeFeature TSDB_METRIC_TEMPORALITY_SUPPORT = new NodeFeature("mapper.tsdb.metric_temporality_support");
    static final NodeFeature DUPLICATE_DYNAMIC_TEMPLATE_NAMES_WARNING = new NodeFeature("mapper.dynamic_template.warn_on_duplicate_names");

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(TSDB_METRIC_TEMPORALITY_SUPPORT);
    }

    @Override
    public Set<NodeFeature> getTestFeatures() {
        Set<NodeFeature> features = new HashSet<>(
            Set.of(
                DATE_RANGE_INDEXING_FIX,
                DONT_EXPAND_DOTS_IN_IGNORED_SOURCE,
                REMOVE_SYNTHETIC_SOURCE_ONLY_VALIDATION,
                SOURCE_MODE_FROM_INDEX_SETTING,
                IGNORED_SOURCE_AS_TOP_LEVEL_METADATA_ARRAY_FIELD,
                ALWAYS_STORE_OBJECT_ARRAYS_IN_NESTED_OBJECTS,
                LOGSDB_DEFAULT_IGNORE_DYNAMIC_BEYOND_LIMIT,
                FIX_PARSING_SUBOBJECTS_FALSE_DYNAMIC_FALSE,
                CONSTANT_KEYWORD_SYNTHETIC_SOURCE_WRITE_FIX,
                META_FETCH_FIELDS_ERROR_CODE_CHANGED,
                SPARSE_VECTOR_STORE_SUPPORT,
                COUNTED_KEYWORD_SYNTHETIC_SOURCE_NATIVE_SUPPORT,
                SORT_FIELDS_CHECK_FOR_NESTED_OBJECT_FIX,
                DYNAMIC_HANDLING_IN_COPY_TO,
                TSDB_NESTED_FIELD_SUPPORT,
                SYNTHETIC_RECOVERY_SOURCE,
                SUBOBJECTS_FALSE_MAPPING_UPDATE_FIX,
                UKNOWN_FIELD_MAPPING_UPDATE_ERROR_MESSAGE,
                DOC_VALUES_SKIPPER,
                RESCORE_VECTOR_QUANTIZED_VECTOR_MAPPING,
                INVALID_DATE_FIX,
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
                MATCH_ONLY_TEXT_DOC_VALUES_PREFIX_WILDCARD_REGEXP,
                PATTERN_TEXT_RENAME,
                DISKBBQ_ON_DISK_RESCORING,
                PROVIDE_INDEX_SORT_SETTING_DEFAULTS,
                INDEX_MAPPING_IGNORE_DYNAMIC_BEYOND_FIELD_NAME_LIMIT,
                EXCLUDE_VECTORS_DOCVALUE_BUGFIX,
                BASE64_DENSE_VECTORS,
                FIX_DENSE_VECTOR_WRONG_FIELDS,
                BBQ_DISK_STATS_SUPPORT,
                SKIPPERS_ON_UNINDEXED_FIELDS,
                STORED_FIELDS_SPEC_MERGE_BUG,
                GENERIC_VECTOR_FORMAT,
                EXPONENTIAL_HISTOGRAM_TYPE,
                STORE_HIGH_CARDINALITY_KEYWORDS_IN_BINARY_DOC_VALUES,
                HIGH_CARDINALITY_LENGTH_FUNCTION_FUSE_TO_LOAD,
                MV_MIN_FUNCTION_FUSE_TO_LOAD,
                MV_MAX_FUNCTION_FUSE_TO_LOAD,
                TDIGEST_TYPE,
                TEXT_FIELD_DOC_VALUES,
                TEXT_FIELD_DOC_VALUES_PREFIX_WILDCARD_REGEXP,
                DENSE_VECTOR_DYNAMIC_TEMPLATE_DOTTED_FIELD_FIX,
                DOC_VALUES_MULTI_VALUE,
                DOC_VALUES_MULTI_VALUE_ENFORCEMENT,
                DOC_VALUES_MULTI_VALUE_RENAME,
                DENSE_VECTOR_DYNAMIC_TEMPLATE_NESTED_OBJECT_FIX,
                FLATTENED_MAPPED_SUBFIELDS_FEATURE,
                FLATTENED_COLUMNAR_DOCUMENT_ORDER,
                ARRAY_OBJECTS_LIMIT,
                ES940_DISK_BBQ,
                FLATTENED_PASSTHROUGH_FEATURE,
                IGNORED_VALUES_STORED_IN_BINARY_DV,
                IP_MAPPER_CARDINALITY_OPTION,
                KEYWORD_NORMALIZER_SKIP_STORE_SETTING,
                KEYWORD_MULTI_FIELDS_NOT_STORED_WHEN_IGNORED,
                ANALYZER_WRAPPER_RELOADABLE_SEARCH_ANALYZER,
                ROUTING_AS_DOC_VALUES,
                ID_FIELD_MODE_MAPPING_ATTRIBUTE,
                ROUTING_AS_DOC_VALUES_BY_DEFAULT,
                STORE_NOT_ALLOWED_IN_COLUMNAR_INDEX_MODE,
                KEYWORD_DV_CASE_INSENSITIVE_REGEXP,
                COLUMNAR_MAINTAIN_ARRAY_ORDER,
                COLUMNAR_REJECTS_RUNTIME_DYNAMIC,
                COLUMNAR_ACCEPTS_SUBOBJECTS_FALSE,
                KEYWORD_COLUMNAR_DEFAULT_HIGH_CARDINALITY,
                TEXT_FIELDS_ENABLE_DOC_VALUES_BY_DEFAULT_IN_COLUMNAR_MODE,
                COLUMNAR_MAINTAIN_ARRAY_ORDER_IP_TEXT,
                COLUMNAR_INLINE_ARRAY_ORDER_BINARY_DOC_VALUES,
                COLUMNAR_IP_INLINE_ARRAY_ORDER_BINARY_DOC_VALUES,
                COLUMNAR_DROPS_DYNAMIC_FALSE_FIELDS,
                COLUMNAR_SUPPORTS_SHAPE_FIELDS,
                DOC_VALUES_MULTI_VALUE_INDEX_SETTING,
                DOC_VALUES_MULTI_VALUE_FALSE_ALIAS,
                DOC_VALUES_EXTENDED_FORM_ONLY_IN_COLUMNAR,
                DOC_VALUES_NULLABILITY,
                DUPLICATE_DYNAMIC_TEMPLATE_NAMES_WARNING
            )
        );
        if (Build.current().isSnapshot()) {
            features.addAll(Set.of(BBQ_DISK_BYTE_SUPPORT, ASH_QUANTIZATION_TYPE_SUPPORT));
        }
        return Set.copyOf(features);
    }
}
