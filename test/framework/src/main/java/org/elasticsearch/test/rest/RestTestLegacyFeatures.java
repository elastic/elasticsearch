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
            entry(TRANSPORT_VERSION_SUPPORTED, VERSION_INTRODUCING_TRANSPORT_VERSIONS),
            entry(STATE_REPLACED_TRANSPORT_VERSION_WITH_NODES_VERSION, Version.V_8_11_0),
            entry(ML_MEMORY_OVERHEAD_FIXED, Version.V_8_2_1),
            entry(WATCHES_VERSION_IN_META, Version.V_7_13_0),
            entry(SECURITY_ROLE_DESCRIPTORS_OPTIONAL, Version.V_7_3_0),
            entry(SEARCH_AGGREGATIONS_FORCE_INTERVAL_SELECTION_DATE_HISTOGRAM, Version.V_7_2_0),
            entry(TRANSFORM_NEW_API_ENDPOINT, Version.V_7_5_0),
            entry(ML_INDICES_HIDDEN, Version.V_7_7_0),
            entry(ML_ANALYTICS_MAPPINGS, Version.V_7_3_0),
            entry(REST_ELASTIC_PRODUCT_HEADER_PRESENT, Version.V_8_0_1)
        );
    }
}
