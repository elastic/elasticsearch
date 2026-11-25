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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class DefaultStatelessSettingsProvider extends DefaultSettingsProvider {
    private static final Logger LOGGER = LogManager.getLogger(DefaultStatelessSettingsProvider.class);

    private static final List<String> EXCLUDED_SETTINGS = List.of("cluster.deprecation_indexing.enabled");

    /// Data node recovery throttling (DNRT) settings that leave throttling to the master.
    private static final Map<String, String> UNBOUNDED_DATA_NODE_RECOVERY_SETTINGS = Map.of(
        "indices.recovery.max_concurrent_incoming_recoveries",
        String.valueOf(Integer.MAX_VALUE),
        "indices.recovery.max_non_critical_incoming_recoveries_proportion",
        "100%",
        "indices.recovery.max_concurrent_outgoing_recoveries",
        String.valueOf(Integer.MAX_VALUE),
        "indices.recovery.enable_direct_cancellations",
        "false"
    );

    /// Master-side recovery throttling settings that leave throttling to the data nodes.
    private static final Map<String, String> UNTHROTTLED_MASTER_RECOVERY_SETTINGS = Map.of(
        "cluster.routing.allocation.node_concurrent_recoveries",
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
        initRandomizedSettings(defaultSettings);
        EXCLUDED_SETTINGS.forEach(defaultSettings::remove);
        return defaultSettings;
    }

    private boolean initialized;
    private int uploadMaxCommits;
    /// Data node recovery throttling (DNRT) settings applied to every node.
    private Map<String, String> dataNodeRecoveryThrottlingSettings;

    /// Draws all the randomized settings once per cluster, in a fixed order, and applies the same values to every node.
    ///
    /// The nodes resolve their settings on parallel worker threads, and each new thread gets its own random stream, seeded
    /// identically from the test seed. Drawing everything at once, on whichever thread comes first, keeps the values reproducible
    /// from the seed. Initializing the settings separately could draw them from different threads' streams.
    private synchronized void initRandomizedSettings(Map<String, String> settings) {
        if (initialized == false) {
            final Random random = RandomizedContext.current().getRandom();
            initialized = true;
            uploadMaxCommits = random.nextBoolean() ? random.nextInt(1, 10) : 100;
            dataNodeRecoveryThrottlingSettings = randomDataNodeRecoveryThrottlingSettings(random);
        }
        settings.put("stateless.upload.max_commits", String.valueOf(uploadMaxCommits));
        settings.putAll(dataNodeRecoveryThrottlingSettings);
    }

    /// How recoveries are throttled in the test cluster.
    ///
    /// - `MASTER`: master only, with its pre-DNRT default limits.
    /// - `DNRT`: DNRT with random limits, the master does not throttle.
    private enum RecoveryThrottlingMode {
        MASTER,
        DNRT
    }

    private static Map<String, String> randomDataNodeRecoveryThrottlingSettings(Random random) {
        final RecoveryThrottlingMode mode = RandomPicks.randomFrom(random, RecoveryThrottlingMode.values());
        final Map<String, String> settings = dataNodeRecoveryThrottlingSettings(mode, random);
        LOGGER.info("Using data node recovery throttling mode [{}] with settings {}", mode, settings);
        return settings;
    }

    private static Map<String, String> dataNodeRecoveryThrottlingSettings(RecoveryThrottlingMode mode, Random random) {
        final Map<String, String> settings = new HashMap<>();
        switch (mode) {
            case MASTER -> {
                // Keep the pre-DNRT serverless default master-side throttling and leave DNRT unbounded
                settings.put("cluster.routing.allocation.node_concurrent_recoveries", "32");
                settings.put("cluster.routing.allocation.node_initial_primaries_recoveries", "6");
                settings.put("cluster.routing.allocation.min_heap_required_for_concurrent_primary_recoveries", "2560mb");
                settings.put("cluster.routing.allocation.concurrent_primary_recoveries_per_heap_gb", "1.5");
                settings.put("cluster.routing.allocation.cluster_concurrent_rebalance", "2");
                settings.putAll(UNBOUNDED_DATA_NODE_RECOVERY_SETTINGS);
            }
            case DNRT -> {
                settings.putAll(UNTHROTTLED_MASTER_RECOVERY_SETTINGS);
                settings.put("indices.recovery.max_concurrent_incoming_recoveries", String.valueOf(random.nextInt(1, 17)));
                settings.put("indices.recovery.max_non_critical_incoming_recoveries_proportion", random.nextInt(50, 101) + "%");
                settings.put("indices.recovery.max_concurrent_outgoing_recoveries", String.valueOf(random.nextInt(1, 17)));
                settings.put("indices.recovery.enable_direct_cancellations", String.valueOf(random.nextBoolean()));
            }
        }
        settings.put("indices.recovery.gates.enabled", "false");
        return Map.copyOf(settings);
    }
}
