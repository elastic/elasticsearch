/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.cluster.stateless.local;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.test.cluster.local.DefaultSettingsProvider;
import org.elasticsearch.test.cluster.local.LocalClusterSpec;
import org.elasticsearch.test.cluster.util.Version;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class DefaultStatelessSettingsProvider extends DefaultSettingsProvider {
    private static final Logger LOGGER = LogManager.getLogger(DefaultStatelessSettingsProvider.class);

    private static final List<String> EXCLUDED_SETTINGS = List.of("cluster.deprecation_indexing.enabled");

    /// Master-side recovery throttling settings that leave throttling to the data nodes.
    private static final Map<String, String> UNTHROTTLED_MASTER_RECOVERY_SETTINGS = Map.of(
        "cluster.routing.allocation.node_concurrent_recoveries",
        String.valueOf(Integer.MAX_VALUE),
        "cluster.routing.allocation.node_concurrent_incoming_recoveries",
        String.valueOf(Integer.MAX_VALUE),
        "cluster.routing.allocation.node_concurrent_outgoing_recoveries",
        String.valueOf(Integer.MAX_VALUE),
        "cluster.routing.allocation.node_initial_primaries_recoveries",
        String.valueOf(Integer.MAX_VALUE),
        "cluster.routing.allocation.min_heap_required_for_concurrent_primary_recoveries",
        "0b",
        "cluster.routing.allocation.concurrent_primary_recoveries_per_heap_gb",
        "1000000",
        "cluster.routing.allocation.cluster_concurrent_rebalance",
        "-1"
    );

    @Override
    public Map<String, String> get(LocalClusterSpec.LocalNodeSpec nodeSpec) {
        Map<String, String> defaultSettings = super.get(nodeSpec);
        initUploadMaxCommitsSettings(defaultSettings);
        initDataNodeRecoveryThrottlingSettings(nodeSpec, defaultSettings);
        EXCLUDED_SETTINGS.forEach(defaultSettings::remove);
        return defaultSettings;
    }

    private boolean initialized;
    private int uploadMaxCommits;

    private synchronized void initUploadMaxCommitsSettings(Map<String, String> settings) {
        if (initialized == false) {
            final Random random = RandomizedContext.current().getRandom();
            initialized = true;
            uploadMaxCommits = random.nextBoolean() ? random.nextInt(1, 10) : 100;
        }
        settings.put("stateless.upload.max_commits", String.valueOf(uploadMaxCommits));
    }

    /// How recoveries are throttled in the test cluster.
    ///
    /// - `MASTER`: master only, with its default limits.
    /// - `DNRT_PROD`: DNRT with the target serverless production limits, the master does not throttle.
    /// - `DNRT_TIGHT`: DNRT with very low limits, the master does not throttle.
    private enum RecoveryThrottlingMode {
        MASTER,
        DNRT_PROD,
        DNRT_TIGHT
    }

    /// First version in which the stateless plugin registers all the DNRT settings used below.
    private static final Version DNRT_SETTINGS_MIN_VERSION = Version.fromString("9.7.0");

    /// Data node recovery throttling (DNRT) settings applied to every node, drawn on first use.
    private Map<String, String> dataNodeRecoveryThrottlingSettings;

    private synchronized void initDataNodeRecoveryThrottlingSettings(
        LocalClusterSpec.LocalNodeSpec nodeSpec,
        Map<String, String> settings
    ) {
        if (dataNodeRecoveryThrottlingSettings == null) {
            final Random random = RandomizedContext.current().getRandom();
            // Older versions (e.g. in rolling upgrade tests) do not register all the DNRT settings.
            // Decided once, so that the nodes keep agreeing after they are upgraded.
            final boolean allNodesSupportDNRTSettings = nodeSpec.getCluster()
                .getNodes()
                .stream()
                .allMatch(node -> node.getVersion().onOrAfter(DNRT_SETTINGS_MIN_VERSION));
            final RecoveryThrottlingMode mode = allNodesSupportDNRTSettings
                ? RandomPicks.randomFrom(random, RecoveryThrottlingMode.values())
                : RecoveryThrottlingMode.MASTER;
            dataNodeRecoveryThrottlingSettings = dataNodeRecoveryThrottlingSettings(mode, random);
            LOGGER.info("Using data node recovery throttling mode [{}] with settings {}", mode, dataNodeRecoveryThrottlingSettings);
        }
        settings.putAll(dataNodeRecoveryThrottlingSettings);
    }

    private static Map<String, String> dataNodeRecoveryThrottlingSettings(RecoveryThrottlingMode mode, Random random) {
        final Map<String, String> settings = new HashMap<>();
        switch (mode) {
            case MASTER -> {
                // Keep the default master-side throttling and leave DNRT unbounded
            }
            case DNRT_PROD -> {
                // Target values for `serverless-default-settings.yml`. Recovery gates are left disabled.
                settings.putAll(UNTHROTTLED_MASTER_RECOVERY_SETTINGS);
                settings.put("indices.recovery.max_concurrent_incoming_recoveries", "32");
                settings.put("indices.recovery.max_concurrent_incoming_recoveries_per_heap_gb", "1.5");
                settings.put("indices.recovery.max_non_critical_incoming_recoveries_proportion", "0.5");
                settings.put("indices.recovery.max_concurrent_outgoing_recoveries", "16");
                settings.put("indices.recovery.max_concurrent_outgoing_recoveries_per_heap_gb", "0.75");
                settings.put("indices.recovery.enable_direct_cancellations", "true");
            }
            case DNRT_TIGHT -> {
                settings.putAll(UNTHROTTLED_MASTER_RECOVERY_SETTINGS);
                settings.put("indices.recovery.max_concurrent_incoming_recoveries", String.valueOf(random.nextInt(1, 4)));
                settings.put("indices.recovery.max_non_critical_incoming_recoveries_proportion", random.nextInt(1, 101) + "%");
                settings.put("indices.recovery.max_concurrent_outgoing_recoveries", String.valueOf(random.nextInt(1, 4)));
                settings.put("indices.recovery.enable_direct_cancellations", String.valueOf(random.nextBoolean()));
            }
        }
        return Map.copyOf(settings);
    }
}
