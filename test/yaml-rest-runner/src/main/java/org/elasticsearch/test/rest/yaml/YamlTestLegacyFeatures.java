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
    private static final NodeFeature CLUSTER_DESIRED_NODES = new NodeFeature("cluster_desired_nodes");

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

    private static final NodeFeature INDICES_REPLICATE_CLOSED = new NodeFeature("indices_replicate_closed");

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
            Map.entry(CLUSTER_DESIRED_NODES, Version.V_8_13_0),

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

            Map.entry(TEMPLATES_V2, Version.V_7_8_0)
        );
    }
}
