/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.rest;

import org.elasticsearch.Version;
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
    public static final NodeFeature ML_NEW_MEMORY_FORMAT = new NodeFeature("ml.new_memory_format");

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

    public static final NodeFeature TSDB_NEW_INDEX_FORMAT = new NodeFeature("indices.tsdb_new_format");
    public static final NodeFeature TSDB_GENERALLY_AVAILABLE = new NodeFeature("indices.tsdb_supported");

    public static final NodeFeature TSDB_DOWNSAMPLING_STABLE = new NodeFeature("indices.tsdb_downsampling_stable");

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

    /*
     * Starting with 8.11, cluster state has minimum system index mappings versions (#99307) and the system index mappings upgrade service
     * started using them to determine when to update mappings for system indices. See https://github.com/elastic/elasticsearch/pull/99668
     */
    public static final NodeFeature MAPPINGS_UPGRADE_SERVICE_USES_MAPPINGS_VERSION = new NodeFeature(
        "mappings.upgrade_service_uses_mappings_version"
    );

    // YAML
    public static final NodeFeature REST_ELASTIC_PRODUCT_HEADER_PRESENT = new NodeFeature("action.rest.product_header_present");

    @Override
    public Map<NodeFeature, Version> getHistoricalFeatures() {
        return Map.ofEntries(
            entry(ML_STATE_RESET_FALLBACK_ON_DISABLED, Version.V_8_7_0),
            entry(SECURITY_UPDATE_API_KEY, Version.V_8_4_0),
            entry(SECURITY_BULK_UPDATE_API_KEY, Version.V_8_5_0),
            entry(ML_NEW_MEMORY_FORMAT, Version.V_8_11_0),
            entry(TRANSPORT_VERSION_SUPPORTED, VERSION_INTRODUCING_TRANSPORT_VERSIONS),
            entry(STATE_REPLACED_TRANSPORT_VERSION_WITH_NODES_VERSION, Version.V_8_11_0),
            entry(ML_MEMORY_OVERHEAD_FIXED, Version.V_8_2_1),
            entry(REST_ELASTIC_PRODUCT_HEADER_PRESENT, Version.V_8_0_1),
            entry(DESIRED_NODE_API_SUPPORTED, Version.V_8_1_0),
            entry(TSDB_NEW_INDEX_FORMAT, Version.V_8_2_0),
            entry(SYNTHETIC_SOURCE_SUPPORTED, Version.V_8_4_0),
            entry(DESIRED_BALANCED_ALLOCATOR_SUPPORTED, Version.V_8_6_0),
            entry(DESIRED_BALANCED_ALLOCATOR_FIXED, Version.V_8_7_1),
            entry(TSDB_GENERALLY_AVAILABLE, Version.V_8_7_0),
            entry(TSDB_DOWNSAMPLING_STABLE, Version.V_8_10_0),
            entry(TSDB_EMPTY_TEMPLATE_FIXED, Version.V_8_11_0),
            entry(INDEXING_SLOWLOG_LEVEL_SETTING_REMOVED, Version.V_8_0_0),
            entry(MAPPINGS_UPGRADE_SERVICE_USES_MAPPINGS_VERSION, Version.V_8_11_0)
        );
    }
}
