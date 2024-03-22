/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.rest;

import org.elasticsearch.Version;
import org.elasticsearch.core.UpdateForV9;
import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Map;

import static java.util.Map.entry;
import static org.elasticsearch.cluster.ClusterState.VERSION_INTRODUCING_TRANSPORT_VERSIONS;

/**
 * This class groups historical features that have been removed from the production codebase, but are still used by the test
 * framework to support BwC tests. Rather than leaving them in the main src we group them here, so it's clear they are not used in
 * production code anymore.
 */
public class RestTestLegacyFeatures implements FeatureSpecification {
    public static final NodeFeature ML_STATE_RESET_FALLBACK_ON_DISABLED = new NodeFeature("ml.state_reset_fallback_on_disabled");
    @UpdateForV9
    public static final NodeFeature FEATURE_STATE_RESET_SUPPORTED = new NodeFeature("system_indices.feature_state_reset_supported");
    public static final NodeFeature SYSTEM_INDICES_REST_ACCESS_ENFORCED = new NodeFeature("system_indices.rest_access_enforced");
    @UpdateForV9
    public static final NodeFeature SYSTEM_INDICES_REST_ACCESS_DEPRECATED = new NodeFeature("system_indices.rest_access_deprecated");
    @UpdateForV9
    public static final NodeFeature HIDDEN_INDICES_SUPPORTED = new NodeFeature("indices.hidden_supported");
    @UpdateForV9
    public static final NodeFeature COMPONENT_TEMPLATE_SUPPORTED = new NodeFeature("indices.component_template_supported");
    @UpdateForV9
    public static final NodeFeature DELETE_TEMPLATE_MULTIPLE_NAMES_SUPPORTED = new NodeFeature(
        "indices.delete_template_multiple_names_supported"
    );
    public static final NodeFeature ML_NEW_MEMORY_FORMAT = new NodeFeature("ml.new_memory_format");
    @UpdateForV9
    public static final NodeFeature SUPPORTS_VENDOR_XCONTENT_TYPES = new NodeFeature("rest.supports_vendor_xcontent_types");
    @UpdateForV9
    public static final NodeFeature SUPPORTS_TRUE_BINARY_RESPONSES = new NodeFeature("rest.supports_true_binary_responses");

    /** These are "pure test" features: normally we would not need them, and test for TransportVersion/fallback to Version (see for example
     * {@code ESRestTestCase#minimumTransportVersion()}. However, some tests explicitly check and validate the content of a response, so
     * we need these features to support them.
     */
    public static final NodeFeature TRANSPORT_VERSION_SUPPORTED = new NodeFeature("transport_version_supported");
    public static final NodeFeature STATE_REPLACED_TRANSPORT_VERSION_WITH_NODES_VERSION = new NodeFeature(
        "state.transport_version_to_nodes_version"
    );

    // Ref: https://github.com/elastic/elasticsearch/pull/86416
    public static final NodeFeature ML_MEMORY_OVERHEAD_FIXED = new NodeFeature("ml.memory_overhead_fixed");

    // QA - rolling upgrade tests
    public static final NodeFeature DESIRED_NODE_API_SUPPORTED = new NodeFeature("desired_node_supported");
    public static final NodeFeature SECURITY_UPDATE_API_KEY = new NodeFeature("security.api_key_update");
    public static final NodeFeature SECURITY_BULK_UPDATE_API_KEY = new NodeFeature("security.api_key_bulk_update");
    @UpdateForV9
    public static final NodeFeature WATCHES_VERSION_IN_META = new NodeFeature("watcher.version_in_meta");
    @UpdateForV9
    public static final NodeFeature SECURITY_ROLE_DESCRIPTORS_OPTIONAL = new NodeFeature("security.role_descriptors_optional");
    @UpdateForV9
    public static final NodeFeature SEARCH_AGGREGATIONS_FORCE_INTERVAL_SELECTION_DATE_HISTOGRAM = new NodeFeature(
        "search.aggregations.force_interval_selection_on_date_histogram"
    );
    @UpdateForV9
    public static final NodeFeature TRANSFORM_NEW_API_ENDPOINT = new NodeFeature("transform.new_api_endpoint");
    // Ref: https://github.com/elastic/elasticsearch/pull/65205
    @UpdateForV9
    public static final NodeFeature ML_INDICES_HIDDEN = new NodeFeature("ml.indices_hidden");
    @UpdateForV9
    public static final NodeFeature ML_ANALYTICS_MAPPINGS = new NodeFeature("ml.analytics_mappings");

    public static final NodeFeature TSDB_NEW_INDEX_FORMAT = new NodeFeature("indices.tsdb_new_format");
    public static final NodeFeature TSDB_GENERALLY_AVAILABLE = new NodeFeature("indices.tsdb_supported");

    /*
     * A composable index template with no template defined in the body is mistakenly always assumed to not be a time series template.
     * Fixed in #98840
     */
    public static final NodeFeature TSDB_EMPTY_TEMPLATE_FIXED = new NodeFeature("indices.tsdb_empty_composable_template_fixed");
    public static final NodeFeature SYNTHETIC_SOURCE_SUPPORTED = new NodeFeature("indices.synthetic_source");

    public static final NodeFeature DESIRED_BALANCED_ALLOCATOR_SUPPORTED = new NodeFeature("allocator.desired_balance");

    /*
     * Cancel shard allocation command is broken for initial desired balance versions
     * and might allocate shard on the node where it is not supposed to be. This
     * is fixed by https://github.com/elastic/elasticsearch/pull/93635.
     */
    public static final NodeFeature DESIRED_BALANCED_ALLOCATOR_FIXED = new NodeFeature("allocator.desired_balance_fixed");
    public static final NodeFeature INDEXING_SLOWLOG_LEVEL_SETTING_REMOVED = new NodeFeature("settings.indexing_slowlog_level_removed");
    public static final NodeFeature DEPRECATION_WARNINGS_LEAK_FIXED = new NodeFeature("deprecation_warnings_leak_fixed");

    // QA - Full cluster restart
    @UpdateForV9
    public static final NodeFeature REPLICATION_OF_CLOSED_INDICES = new NodeFeature("indices.closed_replication_supported");
    @UpdateForV9
    public static final NodeFeature TASK_INDEX_SYSTEM_INDEX = new NodeFeature("tasks.moved_to_system_index");
    @UpdateForV9
    public static final NodeFeature SOFT_DELETES_ENFORCED = new NodeFeature("indices.soft_deletes_enforced");
    @UpdateForV9
    public static final NodeFeature NEW_TRANSPORT_COMPRESSED_SETTING = new NodeFeature("transport.new_compressed_setting");
    @UpdateForV9
    public static final NodeFeature SHUTDOWN_SUPPORTED = new NodeFeature("shutdown.supported");
    @UpdateForV9
    public static final NodeFeature SERVICE_ACCOUNTS_SUPPORTED = new NodeFeature("auth.service_accounts_supported");
    @UpdateForV9
    public static final NodeFeature TRANSFORM_SUPPORTED = new NodeFeature("transform.supported");
    @UpdateForV9
    public static final NodeFeature SLM_SUPPORTED = new NodeFeature("slm.supported");
    @UpdateForV9
    public static final NodeFeature DATA_STREAMS_SUPPORTED = new NodeFeature("data_stream.supported");
    @UpdateForV9
    public static final NodeFeature NEW_DATA_STREAMS_INDEX_NAME_FORMAT = new NodeFeature("data_stream.new_index_name_format");
    @UpdateForV9
    public static final NodeFeature DISABLE_FIELD_NAMES_FIELD_REMOVED = new NodeFeature("disable_of_field_names_field_removed");
    @UpdateForV9
    public static final NodeFeature ML_NLP_SUPPORTED = new NodeFeature("ml.nlp_supported");

    // YAML
    public static final NodeFeature REST_ELASTIC_PRODUCT_HEADER_PRESENT = new NodeFeature("action.rest.product_header_present");

    @Override
    public Map<NodeFeature, Version> getHistoricalFeatures() {
        return Map.ofEntries(
            entry(FEATURE_STATE_RESET_SUPPORTED, Version.V_7_13_0),
            entry(SYSTEM_INDICES_REST_ACCESS_ENFORCED, Version.V_8_0_0),
            entry(SYSTEM_INDICES_REST_ACCESS_DEPRECATED, Version.V_7_10_0),
            entry(HIDDEN_INDICES_SUPPORTED, Version.V_7_7_0),
            entry(COMPONENT_TEMPLATE_SUPPORTED, Version.V_7_8_0),
            entry(DELETE_TEMPLATE_MULTIPLE_NAMES_SUPPORTED, Version.V_7_13_0),
            entry(ML_STATE_RESET_FALLBACK_ON_DISABLED, Version.V_8_7_0),
            entry(SECURITY_UPDATE_API_KEY, Version.V_8_4_0),
            entry(SECURITY_BULK_UPDATE_API_KEY, Version.V_8_5_0),
            entry(ML_NEW_MEMORY_FORMAT, Version.V_8_11_0),
            entry(SUPPORTS_VENDOR_XCONTENT_TYPES, Version.V_7_11_0),
            entry(SUPPORTS_TRUE_BINARY_RESPONSES, Version.V_7_7_0),
            entry(TRANSPORT_VERSION_SUPPORTED, VERSION_INTRODUCING_TRANSPORT_VERSIONS),
            entry(STATE_REPLACED_TRANSPORT_VERSION_WITH_NODES_VERSION, Version.V_8_11_0),
            entry(ML_MEMORY_OVERHEAD_FIXED, Version.V_8_2_1),
            entry(WATCHES_VERSION_IN_META, Version.V_7_13_0),
            entry(SECURITY_ROLE_DESCRIPTORS_OPTIONAL, Version.V_7_3_0),
            entry(SEARCH_AGGREGATIONS_FORCE_INTERVAL_SELECTION_DATE_HISTOGRAM, Version.V_7_2_0),
            entry(TRANSFORM_NEW_API_ENDPOINT, Version.V_7_5_0),
            entry(ML_INDICES_HIDDEN, Version.V_7_7_0),
            entry(ML_ANALYTICS_MAPPINGS, Version.V_7_3_0),
            entry(REST_ELASTIC_PRODUCT_HEADER_PRESENT, Version.V_8_0_1),
            entry(DESIRED_NODE_API_SUPPORTED, Version.V_8_1_0),
            entry(TSDB_NEW_INDEX_FORMAT, Version.V_8_2_0),
            entry(SYNTHETIC_SOURCE_SUPPORTED, Version.V_8_4_0),
            entry(DESIRED_BALANCED_ALLOCATOR_SUPPORTED, Version.V_8_6_0),
            entry(DESIRED_BALANCED_ALLOCATOR_FIXED, Version.V_8_7_1),
            entry(TSDB_GENERALLY_AVAILABLE, Version.V_8_7_0),
            entry(TSDB_EMPTY_TEMPLATE_FIXED, Version.V_8_11_0),
            entry(INDEXING_SLOWLOG_LEVEL_SETTING_REMOVED, Version.V_8_0_0),
            entry(DEPRECATION_WARNINGS_LEAK_FIXED, Version.V_7_17_9),
            entry(REPLICATION_OF_CLOSED_INDICES, Version.V_7_2_0),
            entry(TASK_INDEX_SYSTEM_INDEX, Version.V_7_10_0),
            entry(SOFT_DELETES_ENFORCED, Version.V_8_0_0),
            entry(NEW_TRANSPORT_COMPRESSED_SETTING, Version.V_7_14_0),
            entry(SHUTDOWN_SUPPORTED, Version.V_7_15_0),
            entry(SERVICE_ACCOUNTS_SUPPORTED, Version.V_7_13_0),
            entry(TRANSFORM_SUPPORTED, Version.V_7_2_0),
            entry(SLM_SUPPORTED, Version.V_7_4_0),
            entry(DATA_STREAMS_SUPPORTED, Version.V_7_9_0),
            entry(NEW_DATA_STREAMS_INDEX_NAME_FORMAT, Version.V_7_11_0),
            entry(DISABLE_FIELD_NAMES_FIELD_REMOVED, Version.V_8_0_0),
            entry(ML_NLP_SUPPORTED, Version.V_8_0_0)
        );
    }
}
