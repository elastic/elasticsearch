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
    private static final NodeFeature CAT_INDICES_REPLICATE_CLOSED = new NodeFeature("cat_indices_replicate_closed");
    private static final NodeFeature CAT_INDICES_VALIDATE_HEALTH_PARAM = new NodeFeature("cat_indices_validate_health_param");

    private static final NodeFeature CAT_PLUGINS_NEW_FORMAT = new NodeFeature("cat_plugins_new_format");

    private static final NodeFeature CAT_RECOVERY_NEW_BYTES_FORMAT = new NodeFeature("cat_recovery_new_bytes_format");

    private static final NodeFeature CAT_SHARDS_DATASET_SIZE = new NodeFeature("cat_shards_dataset_size");
    private static final NodeFeature CAT_SHARDS_FIX_HIDDEN_INDICES = new NodeFeature("cat_shards_fix_hidden_indices");

    private static final NodeFeature CAT_TASKS_X_OPAQUE_ID = new NodeFeature("cat_tasks_x_opaque_id");

    private static final NodeFeature CAT_TEMPLATES_V2 = new NodeFeature("cat_templates_v2");
    private static final NodeFeature CAT_TEMPLATE_NAME_VALIDATION = new NodeFeature("cat_template_name_validation");

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

            Map.entry(CAT_INDICES_REPLICATE_CLOSED, Version.V_7_2_0),
            Map.entry(CAT_INDICES_VALIDATE_HEALTH_PARAM, Version.V_7_8_0),
            Map.entry(CAT_INDICES_DATASET_SIZE, Version.V_8_11_0),

            Map.entry(CAT_PLUGINS_NEW_FORMAT, Version.V_7_12_0),

            Map.entry(CAT_RECOVERY_NEW_BYTES_FORMAT, Version.V_8_0_0),

            Map.entry(CAT_SHARDS_FIX_HIDDEN_INDICES, Version.V_8_3_0),
            Map.entry(CAT_SHARDS_DATASET_SIZE, Version.V_8_11_0),

            Map.entry(CAT_TASKS_X_OPAQUE_ID, Version.V_7_10_0),

            Map.entry(CAT_TEMPLATES_V2, Version.V_7_8_0),
            Map.entry(CAT_TEMPLATE_NAME_VALIDATION, Version.V_7_16_0)
        );
    }
}
