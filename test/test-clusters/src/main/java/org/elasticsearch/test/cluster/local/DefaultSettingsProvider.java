/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.cluster.local;

import org.elasticsearch.test.cluster.SettingsProvider;
import org.elasticsearch.test.cluster.local.LocalClusterSpec.LocalNodeSpec;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class DefaultSettingsProvider implements SettingsProvider {

    private static final String MULTI_NODE_DISCOVERY_TYPE = "multi-node";
    private static final String DISCOVERY_TYPE_SETTING = "discovery.type";

    @Override
    public Map<String, String> get(LocalNodeSpec nodeSpec) {
        Map<String, String> settings = new HashMap<>();

        settings.put("node.attr.testattr", "test");
        settings.put("node.portsfile", "true");
        settings.put("http.port", "0");
        settings.put("transport.port", "0");
        settings.put("network.host", "_local_");

        if (nodeSpec.getDistributionType() == DistributionType.INTEG_TEST) {
            settings.put("xpack.security.enabled", "false");
        } else {
            // Disable deprecation indexing which is enabled by default in 7.16
            if (nodeSpec.getVersion().onOrAfter("7.16.0")) {
                settings.put("cluster.deprecation_indexing.enabled", "false");
            }
        }

        // Default the watermarks to absurdly low to prevent the tests from failing on nodes without enough disk space
        settings.put("cluster.routing.allocation.disk.watermark.low", "1b");
        settings.put("cluster.routing.allocation.disk.watermark.high", "1b");
        settings.put("cluster.routing.allocation.disk.watermark.flood_stage", "1b");

        // increase script compilation limit since tests can rapid-fire script compilations
        if (nodeSpec.getVersion().onOrAfter("7.9.0")) {
            settings.put("script.disable_max_compilations_rate", "true");
        } else {
            settings.put("script.max_compilations_rate", "2048/1m");
        }

        // Temporarily disable the real memory usage circuit breaker. It depends on real memory usage which we have no full control
        // over and the REST client will not retry on circuit breaking exceptions yet (see #31986 for details). Once the REST client
        // can retry on circuit breaking exceptions, we can revert again to the default configuration.
        settings.put("indices.breaker.total.use_real_memory", "false");

        // Don't wait for state, just start up quickly. This will also allow new and old nodes in the BWC case to become the master
        settings.put("discovery.initial_state_timeout", "0s");

        if (nodeSpec.getVersion().getMajor() >= 8) {
            settings.put("cluster.service.slow_task_logging_threshold", "5s");
            settings.put("cluster.service.slow_master_task_logging_threshold", "5s");
        }

        settings.put("action.destructive_requires_name", "false");

        // Setup cluster discovery
        String masterEligibleNodes = nodeSpec.getCluster()
            .getNodes()
            .stream()
            .filter(LocalNodeSpec::isMasterEligible)
            .map(LocalNodeSpec::getName)
            .filter(Objects::nonNull)
            .collect(Collectors.joining(","));

        if (isMultiNodeCluster(nodeSpec.getCluster())) {
            if (masterEligibleNodes.isEmpty()) {
                throw new IllegalStateException(
                    "Cannot start multi-node cluster '" + nodeSpec.getCluster().getName() + "' as configured with no master-eligible nodes."
                );
            }

            settings.put("cluster.initial_master_nodes", "[" + masterEligibleNodes + "]");
            settings.put("discovery.seed_providers", "file");
            settings.put("discovery.seed_hosts", "[]");
        }

        return settings;
    }

    private boolean isMultiNodeCluster(LocalClusterSpec cluster) {
        return cluster.getNodes().size() > 1
            || cluster.getNodes().get(0).getSetting(DISCOVERY_TYPE_SETTING, MULTI_NODE_DISCOVERY_TYPE).equals(MULTI_NODE_DISCOVERY_TYPE);
    }
}
