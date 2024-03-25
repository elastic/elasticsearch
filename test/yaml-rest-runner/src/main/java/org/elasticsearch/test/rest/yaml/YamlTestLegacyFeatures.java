/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.rest.yaml;

import org.elasticsearch.Version;
import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Map;

/**
 * This class groups historical features that have been removed from the production codebase, but are still used by YAML test
 * to support BwC. Rather than leaving them in the main src we group them here, so it's clear they are not used in production code anymore.
 */
public class YamlTestLegacyFeatures implements FeatureSpecification {
    private static final NodeFeature BULK_AUTO_ID = new NodeFeature("bulk_auto_id");
    private static final NodeFeature BULK_REQUIRE_ALIAS = new NodeFeature("bulk_require_alias");
    private static final NodeFeature BULK_DYNAMIC_TEMPLATE_OP_TYPE = new NodeFeature("bulk_dynamic_template_op_type");
    private static final NodeFeature BULK_DYNAMIC_TEMPLATE_DOCUMENT_PARSE_EXCEPTION = new NodeFeature(
        "bulk_dynamic_template_document_parse_exception"
    );

    private static final NodeFeature BULK_PIPELINE_VALIDATE = new NodeFeature("bulk_pipeline_validate");

    private static final NodeFeature CAT_ALIASES_SHOW_WRITE_INDEX = new NodeFeature("cat_aliases_show_write_index");
    private static final NodeFeature CAT_ALIASES_HIDDEN = new NodeFeature("cat_aliases_hidden");
    private static final NodeFeature CAT_ALIASES_LOCAL_DEPRECATED = new NodeFeature("cat_aliases_local_deprecated");

    private static final NodeFeature CAT_ALLOCATION_NODE_ROLE = new NodeFeature("cat_allocation_node_role");

    private static final NodeFeature CAT_INDICES_DATASET_SIZE = new NodeFeature("cat_indices_dataset_size");
    private static final NodeFeature CAT_INDICES_VALIDATE_HEALTH_PARAM = new NodeFeature("cat_indices_validate_health_param");

    private static final NodeFeature CAT_PLUGINS_NEW_FORMAT = new NodeFeature("cat_plugins_new_format");

    private static final NodeFeature CAT_RECOVERY_NEW_BYTES_FORMAT = new NodeFeature("cat_recovery_new_bytes_format");

    private static final NodeFeature CAT_SHARDS_DATASET_SIZE = new NodeFeature("cat_shards_dataset_size");
    private static final NodeFeature CAT_SHARDS_FIX_HIDDEN_INDICES = new NodeFeature("cat_shards_fix_hidden_indices");

    private static final NodeFeature CAT_TASKS_X_OPAQUE_ID = new NodeFeature("cat_tasks_x_opaque_id");

    private static final NodeFeature CAT_TEMPLATE_NAME_VALIDATION = new NodeFeature("cat_template_name_validation");

    private static final NodeFeature CLUSTER_TEMPLATES_DELETE_MULTIPLE = new NodeFeature("cluster_templates_delete_multiple");

    private static final NodeFeature CLUSTER_ALLOCATION_ROLE = new NodeFeature("cluster_allocation_role");

    private static final NodeFeature CLUSTER_DESIRED_BALANCE = new NodeFeature("cluster_desired_balance");
    private static final NodeFeature CLUSTER_DESIRED_BALANCE_STATS = new NodeFeature("cluster_desired_balance_stats");
    private static final NodeFeature CLUSTER_DESIRED_BALANCE_EXTENDED = new NodeFeature("cluster_desired_balance_extended");
    private static final NodeFeature CLUSTER_DESIRED_BALANCE_STATS_UNDESIRED_COUNT = new NodeFeature(
        "cluster_desired_balance_stats_undesired_count"
    );

    private static final NodeFeature CLUSTER_DESIRED_NODES_OLD = new NodeFeature("cluster_desired_nodes_old");
    private static final NodeFeature CLUSTER_DESIRED_NODES_DRY_RUN = new NodeFeature("cluster_desired_nodes_dry_run");
    private static final NodeFeature CLUSTER_DESIRED_NODES_NO_SETTINGS_VALIDATION = new NodeFeature(
        "cluster_desired_nodes_no_settings_validation"
    );
    
    private static final NodeFeature CLUSTER_HEALTH_INDICES_OPTIONS = new NodeFeature("cluster_health_indices_options");

    private static final NodeFeature CLUSTER_INFO = new NodeFeature("cluster_info");
    private static final NodeFeature CLUSTER_INFO_EXTENDED = new NodeFeature("cluster_info_extended");

    private static final NodeFeature CLUSTER_PREVALIDATE_NODE_REMOVAL = new NodeFeature("cluster_prevalidate_node_removal");
    private static final NodeFeature CLUSTER_PREVALIDATE_NODE_REMOVAL_REASON = new NodeFeature("cluster_prevalidate_node_removal_reason");

    private static final NodeFeature CLUSTER_STATS_PACKAGING_TYPES = new NodeFeature("cluster_stats_packaging_types");
    private static final NodeFeature CLUSTER_STATS_RUNTIME_FIELDS = new NodeFeature("cluster_stats_runtime_fields");
    private static final NodeFeature CLUSTER_STATS_INDEXING_PRESSURE = new NodeFeature("cluster_stats_indexing_pressure");
    private static final NodeFeature CLUSTER_STATS_MAPPING_SIZES = new NodeFeature("cluster_stats_mapping_sizes");
    private static final NodeFeature CLUSTER_STATS_SNAPSHOTS = new NodeFeature("cluster_stats_snapshots");
    private static final NodeFeature CLUSTER_STATS_DENSE_VECTORS = new NodeFeature("cluster_stats_dense_vectors");

    private static final NodeFeature DATASTREAM_LIFECYCLE = new NodeFeature("datastream_lifecycle");

    private static final NodeFeature TEMPLATES_V2 = new NodeFeature("templates_v2");
    private static final NodeFeature TEMPLATES_V2_MERGE = new NodeFeature("templates_v2_merge");
    private static final NodeFeature TEMPLATES_V2_IGNORE_MISSING = new NodeFeature("templates_v2_ignore_missing");

    private static final NodeFeature INDICES_REPLICATE_CLOSED = new NodeFeature("indices_replicate_closed");

    private static final NodeFeature FEATURES_GET = new NodeFeature("features_get");
    private static final NodeFeature FEATURES_RESET = new NodeFeature("features_reset");

    private static final NodeFeature FIELD_CAPS_UNMAPPED = new NodeFeature("field_caps_unmapped");
    private static final NodeFeature FIELD_CAPS_METADATA = new NodeFeature("field_caps_metadata");
    private static final NodeFeature FIELD_CAPS_INDEX_FILTER = new NodeFeature("field_caps_index_filter");
    private static final NodeFeature FIELD_CAPS_DOC_VALUES = new NodeFeature("field_caps_doc_values");
    private static final NodeFeature FIELD_CAPS_TIMESERIES = new NodeFeature("field_caps_timeseries");
    private static final NodeFeature FIELD_CAPS_FIELD_TYPE_FILTER = new NodeFeature("field_caps_field_type_filter");
    private static final NodeFeature FIELD_CAPS_FIELDS = new NodeFeature("field_caps_fields");

    private static final NodeFeature SYNTHETIC_SOURCE = new NodeFeature("get_synthetic");

    private static final NodeFeature GET_IGNORE_MALFORMED = new NodeFeature("get_ignore_malformed");
    private static final NodeFeature GET_SYNTHETIC_V2 = new NodeFeature("get_synthetic_v2"); // dense vectors, ignore_above, get stored
    private static final NodeFeature GET_SYNTHETIC_V3 = new NodeFeature("get_synthetic_v3"); // _doc_count, ignore_malformed
    private static final NodeFeature GET_SYNTHETIC_FLATTENED = new NodeFeature("get_synthetic_flattened");

    private static final NodeFeature HEALTH_REPORT = new NodeFeature("health_report");

    private static final NodeFeature INDEX_REQUIRE_ALIAS = new NodeFeature("index_require_alias");
    private static final NodeFeature INDEX_FIX_ARRAY_NULL = new NodeFeature("index_fix_array_null");
    private static final NodeFeature INDEX_DATE_NANOS_VALIDATION = new NodeFeature("index_date_nanos_validation");

    private static final NodeFeature INDICES_FLUSH = new NodeFeature("indices_flush");
    private static final NodeFeature INDICES_CLONE = new NodeFeature("indices_clone");
    private static final NodeFeature INDICES_BLOCKS = new NodeFeature("indices_blocks");

    private static final NodeFeature INDICES_ALIAS_MUST_EXIST = new NodeFeature("indices_alias_must_exist");
    private static final NodeFeature INDICES_ALIAS_DATE_MATH = new NodeFeature("indices_alias_date_math");

    private static final NodeFeature INDICES_DYNAMIC_MAPPING_ARRAY = new NodeFeature("indices_dynamic_mapping_array");

    private static final NodeFeature INDICES_FORCE_MERGE = new NodeFeature("indices_force_merge");
    private static final NodeFeature INDICES_FORCE_MERGE_WAIT = new NodeFeature("indices_force_merge_wait");

    private static final NodeFeature INDICES_RECOVERY_DETAILED = new NodeFeature("indices_recovery_detailed");
    private static final NodeFeature INDICES_TEMPLATES_DELETE_MULTIPLE = new NodeFeature("indices_templates_delete_multiple");
    private static final NodeFeature INDICES_PUT_ALIAS_DATE_NANOS_FILTER = new NodeFeature("indices_put_alias_date_nanos_filter");
    private static final NodeFeature INDICES_CLOSE_WAIT_DEPRECATED = new NodeFeature("indices_close_wait_deprecated");

    private static final NodeFeature INDICES_GET_INDEX_TEMPLATE_VALIDATION = new NodeFeature("indices_get_index_template_validation");
    private static final NodeFeature INDICES_GET_MAPPING_REJECT_LOCAL = new NodeFeature("indices_get_mapping_reject_local");
    private static final NodeFeature INDICES_GET_FEATURES = new NodeFeature("indices_get_features");
    private static final NodeFeature INDICES_GET_MAPPING_FIX_WILDCARDS = new NodeFeature("indices_get_mapping_fix_wildcards");
    private static final NodeFeature INDICES_GET_ALIAS_LOCAL_DEPRECATED = new NodeFeature("indices_get_alias_local_deprecated");

    private static final NodeFeature INDICES_RESOLVE = new NodeFeature("indices_resolve");
    private static final NodeFeature INDICES_RESOLVE_SYSTEM = new NodeFeature("indices_resolve_system");

    private static final NodeFeature INDICES_ROLLOVER_MAX_PRIMARY_SIZE = new NodeFeature("indices_rollover_max_primary_size");
    private static final NodeFeature INDICES_ROLLOVER_MAX_PRIMARIES = new NodeFeature("indices_rollover_max_primaries");
    private static final NodeFeature INDICES_ROLLOVER_CONDITIONS = new NodeFeature("indices_rollover_conditions");

    private static final NodeFeature INDICES_SIMULATE_INDEX_TEMPLATE = new NodeFeature("indices_simulate_index_template");
    private static final NodeFeature INDICES_SIMULATE_TEMPLATE = new NodeFeature("indices_simulate_template");
    private static final NodeFeature INDICES_SIMULATE_TEMPLATE_REPLACE = new NodeFeature("indices_simulate_template_replace");
    private static final NodeFeature INDICES_SIMULATE_INDEX_TEMPLATE_IF_EXISTS = new NodeFeature(
        "indices_simulate_index_template_if_exists"
    );
    private static final NodeFeature INDICES_SIMULATE_TEMPLATE_LIFECYCLE = new NodeFeature("indices_simulate_template_lifecycle");

    private static final NodeFeature INDICES_SPLIT_ROUTING = new NodeFeature("indices_split_routing");

    private static final NodeFeature INDICES_STATS_FORBID_CLOSED = new NodeFeature("indices_stats_forbid_closed");
    private static final NodeFeature INDICES_STATS_FILE_STATS = new NodeFeature("indices_stats_file_stats");
    private static final NodeFeature INDICES_STATS_DISK = new NodeFeature("indices_stats_disk");
    private static final NodeFeature INDICES_STATS_NO_TRANSLOG_RETENTION = new NodeFeature("indices_stats_no_translog_retention");
    private static final NodeFeature INDICES_STATS_DISK_STAR = new NodeFeature("indices_stats_disk_star");
    private static final NodeFeature INDICES_STATS_DISK_DENSE_VECTORS = new NodeFeature("indices_stats_disk_dense_vectors");
    private static final NodeFeature INDICES_STATS_WRITE_LOAD = new NodeFeature("indices_stats_write_load");
    private static final NodeFeature INDICES_STATS_GLOBAL_ORDINALS = new NodeFeature("indices_stats_global_ordinals");
    private static final NodeFeature INDICES_STATS_IDLE = new NodeFeature("indices_stats_idle");

    private static final NodeFeature ALLOWED_WARNINGS_REGEX = new NodeFeature("allowed_warnings_regex");

    @Override
    public Map<NodeFeature, Version> getHistoricalFeatures() {
        return Map.ofEntries(
            Map.entry(BULK_AUTO_ID, Version.V_7_5_0),
            Map.entry(BULK_REQUIRE_ALIAS, Version.V_7_10_0),
            Map.entry(BULK_PIPELINE_VALIDATE, Version.V_7_9_1),
            Map.entry(BULK_DYNAMIC_TEMPLATE_OP_TYPE, Version.V_8_6_1),
            Map.entry(BULK_DYNAMIC_TEMPLATE_DOCUMENT_PARSE_EXCEPTION, Version.V_8_8_0),

            Map.entry(CAT_ALIASES_SHOW_WRITE_INDEX, Version.V_7_4_0),
            Map.entry(CAT_ALIASES_HIDDEN, Version.V_7_7_0),
            Map.entry(CAT_ALIASES_LOCAL_DEPRECATED, Version.V_8_12_0),

            Map.entry(CAT_ALLOCATION_NODE_ROLE, Version.V_8_10_0),

            Map.entry(CAT_INDICES_VALIDATE_HEALTH_PARAM, Version.V_7_8_0),
            Map.entry(CAT_INDICES_DATASET_SIZE, Version.V_8_11_0),

            Map.entry(CAT_PLUGINS_NEW_FORMAT, Version.V_7_12_0),

            Map.entry(CAT_RECOVERY_NEW_BYTES_FORMAT, Version.V_8_0_0),

            Map.entry(CAT_SHARDS_FIX_HIDDEN_INDICES, Version.V_8_3_0),
            Map.entry(CAT_SHARDS_DATASET_SIZE, Version.V_8_11_0),

            Map.entry(CAT_TASKS_X_OPAQUE_ID, Version.V_7_10_0),

            Map.entry(CAT_TEMPLATE_NAME_VALIDATION, Version.V_7_16_0),

            Map.entry(CLUSTER_TEMPLATES_DELETE_MULTIPLE, Version.V_8_0_0),
            Map.entry(CLUSTER_ALLOCATION_ROLE, Version.V_8_11_0),

            Map.entry(CLUSTER_DESIRED_BALANCE, Version.V_8_6_0),
            Map.entry(CLUSTER_DESIRED_BALANCE_STATS, Version.V_8_7_0),
            Map.entry(CLUSTER_DESIRED_BALANCE_EXTENDED, Version.V_8_8_0),
            Map.entry(CLUSTER_DESIRED_BALANCE_STATS_UNDESIRED_COUNT, Version.V_8_12_0),

            Map.entry(CLUSTER_DESIRED_NODES_OLD, Version.V_8_3_0),
            Map.entry(CLUSTER_DESIRED_NODES_DRY_RUN, Version.V_8_4_0),
            Map.entry(CLUSTER_DESIRED_NODES_NO_SETTINGS_VALIDATION, Version.V_8_10_0),

            Map.entry(CLUSTER_HEALTH_INDICES_OPTIONS, Version.V_7_2_0),

            Map.entry(CLUSTER_INFO, Version.V_8_8_0),
            Map.entry(CLUSTER_INFO_EXTENDED, Version.V_8_9_0),

            Map.entry(CLUSTER_PREVALIDATE_NODE_REMOVAL, Version.V_8_6_0),
            Map.entry(CLUSTER_PREVALIDATE_NODE_REMOVAL_REASON, Version.V_8_7_0),

            Map.entry(CLUSTER_STATS_PACKAGING_TYPES, Version.V_7_2_0),
            Map.entry(CLUSTER_STATS_RUNTIME_FIELDS, Version.V_7_13_0),
            Map.entry(CLUSTER_STATS_INDEXING_PRESSURE, Version.V_8_1_0),
            Map.entry(CLUSTER_STATS_MAPPING_SIZES, Version.V_8_4_0),
            Map.entry(CLUSTER_STATS_SNAPSHOTS, Version.V_8_8_0),
            Map.entry(CLUSTER_STATS_DENSE_VECTORS, Version.V_8_10_0),

            Map.entry(DATASTREAM_LIFECYCLE, Version.V_8_11_0),

            Map.entry(INDICES_REPLICATE_CLOSED, Version.V_7_2_0),

            Map.entry(TEMPLATES_V2, Version.V_7_8_0),
            Map.entry(TEMPLATES_V2_MERGE, Version.V_7_9_0),
            Map.entry(TEMPLATES_V2_IGNORE_MISSING, Version.V_8_7_0),

            Map.entry(FEATURES_GET, Version.V_7_12_0),
            Map.entry(FEATURES_RESET, Version.V_7_13_0),

            Map.entry(FIELD_CAPS_UNMAPPED, Version.V_7_2_0),
            Map.entry(FIELD_CAPS_METADATA, Version.V_7_6_0),
            Map.entry(FIELD_CAPS_INDEX_FILTER, Version.V_7_9_0),
            Map.entry(FIELD_CAPS_DOC_VALUES, Version.V_8_1_0),
            Map.entry(FIELD_CAPS_TIMESERIES, Version.V_8_1_0),
            Map.entry(FIELD_CAPS_FIELD_TYPE_FILTER, Version.V_8_2_0),
            Map.entry(FIELD_CAPS_FIELDS, Version.V_8_5_0),

            Map.entry(SYNTHETIC_SOURCE, Version.V_8_4_0),

            Map.entry(GET_IGNORE_MALFORMED, Version.V_8_5_0),
            Map.entry(GET_SYNTHETIC_V2, Version.V_8_5_0),
            Map.entry(GET_SYNTHETIC_V3, Version.V_8_6_0),
            Map.entry(GET_SYNTHETIC_FLATTENED, Version.V_8_8_0),

            Map.entry(HEALTH_REPORT, Version.V_8_7_0),

            Map.entry(INDEX_REQUIRE_ALIAS, Version.V_7_10_0),
            Map.entry(INDEX_FIX_ARRAY_NULL, Version.V_8_3_0),
            Map.entry(INDEX_DATE_NANOS_VALIDATION, Version.V_8_8_0),

            Map.entry(INDICES_FLUSH, Version.V_7_2_0),
            Map.entry(INDICES_CLONE, Version.V_7_4_0),
            Map.entry(INDICES_BLOCKS, Version.V_7_9_0),

            Map.entry(INDICES_ALIAS_MUST_EXIST, Version.V_7_11_0),
            Map.entry(INDICES_ALIAS_DATE_MATH, Version.V_7_13_0),

            Map.entry(INDICES_DYNAMIC_MAPPING_ARRAY, Version.V_8_9_0),

            Map.entry(INDICES_FORCE_MERGE, Version.V_8_0_0),
            Map.entry(INDICES_FORCE_MERGE_WAIT, Version.V_8_1_0),

            Map.entry(INDICES_RECOVERY_DETAILED, Version.V_7_3_0),
            Map.entry(INDICES_TEMPLATES_DELETE_MULTIPLE, Version.V_8_0_0),
            Map.entry(INDICES_PUT_ALIAS_DATE_NANOS_FILTER, Version.V_8_0_0),
            Map.entry(INDICES_CLOSE_WAIT_DEPRECATED, Version.V_8_0_0),

            Map.entry(INDICES_GET_INDEX_TEMPLATE_VALIDATION, Version.V_7_16_0),
            Map.entry(INDICES_GET_MAPPING_REJECT_LOCAL, Version.V_8_0_0),
            Map.entry(INDICES_GET_FEATURES, Version.V_8_1_0),
            Map.entry(INDICES_GET_MAPPING_FIX_WILDCARDS, Version.V_8_6_0),
            Map.entry(INDICES_GET_ALIAS_LOCAL_DEPRECATED, Version.V_8_12_0),

            Map.entry(INDICES_RESOLVE, Version.V_7_9_0),
            Map.entry(INDICES_RESOLVE_SYSTEM, Version.V_8_2_0),

            Map.entry(INDICES_ROLLOVER_MAX_PRIMARY_SIZE, Version.V_7_12_0),
            Map.entry(INDICES_ROLLOVER_MAX_PRIMARIES, Version.V_8_2_0),
            Map.entry(INDICES_ROLLOVER_CONDITIONS, Version.V_8_4_0),

            Map.entry(INDICES_SIMULATE_INDEX_TEMPLATE, Version.V_7_8_0),
            Map.entry(INDICES_SIMULATE_TEMPLATE, Version.V_7_9_0),
            Map.entry(INDICES_SIMULATE_TEMPLATE_REPLACE, Version.V_8_0_0),
            Map.entry(INDICES_SIMULATE_INDEX_TEMPLATE_IF_EXISTS, Version.V_8_2_0),
            Map.entry(INDICES_SIMULATE_TEMPLATE_LIFECYCLE, Version.V_8_11_0),

            Map.entry(INDICES_SPLIT_ROUTING, Version.V_8_5_0),

            Map.entry(INDICES_STATS_FORBID_CLOSED, Version.V_7_2_0),
            Map.entry(INDICES_STATS_FILE_STATS, Version.V_7_13_0),
            Map.entry(INDICES_STATS_DISK, Version.V_7_15_0),
            Map.entry(INDICES_STATS_NO_TRANSLOG_RETENTION, Version.V_8_0_0),
            Map.entry(INDICES_STATS_DISK_STAR, Version.V_8_2_0),
            Map.entry(INDICES_STATS_DISK_DENSE_VECTORS, Version.V_8_4_0),
            Map.entry(INDICES_STATS_WRITE_LOAD, Version.V_8_6_0),
            Map.entry(INDICES_STATS_GLOBAL_ORDINALS, Version.V_8_8_0),
            Map.entry(INDICES_STATS_IDLE, Version.V_8_9_0),

            Map.entry(ALLOWED_WARNINGS_REGEX, Version.V_8_3_0)
        );
    }
}
