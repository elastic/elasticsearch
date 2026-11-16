/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.compute.spill.spi.SpillStore;
import org.elasticsearch.compute.spill.spi.SpillStoreProvider;
import org.elasticsearch.compute.spill.spi.SpillStoreServices;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.spill.disk.DiskSpillStore;

import java.util.List;
import java.util.Map;

/**
 * A test-only store provider named {@code cranky}: the real disk store, wrapped in one that fails, delays or
 * short-changes calls at a rate the test sets through dynamic settings. Inert at rate zero and without a delay, so the
 * clean tests of a suite run against the disk store through it and only the cranky tests turn the knobs up.
 * <p>
 * The ES|QL plugin finds providers through the service file of the test resources and creates an instance of its own,
 * so the stores are reached by node id through {@link #storeOf}, not through the plugin instance the node holds.
 */
public class CrankySpillStorePlugin extends Plugin implements SpillStoreProvider {
    public static final String NAME = "cranky";

    private static final Map<String, CrankySpillStore> STORES = ConcurrentCollections.newConcurrentMap();

    /** The share of calls that misbehave, per kind of misbehaviour; zero is inert. */
    public static final Setting<Double> FAILURE_RATE = Setting.doubleSetting(
        "esql.spill.cranky.failure_rate",
        0.0,
        0.0,
        1.0,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    /** The seed of each node's source of randomness, combined with the node's id; set it so a failure reproduces. */
    public static final Setting<Long> SEED = Setting.longSetting(
        "esql.spill.cranky.seed",
        0L,
        Long.MIN_VALUE,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    /** The most a completion is held back; zero completes as the disk store does. */
    public static final Setting<TimeValue> MAX_DELAY = Setting.timeSetting(
        "esql.spill.cranky.max_delay",
        TimeValue.ZERO,
        TimeValue.ZERO,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(FAILURE_RATE, SEED, MAX_DELAY);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public SpillStore create(SpillStoreServices services) {
        CrankySpillStore[] holder = new CrankySpillStore[1];
        holder[0] = new CrankySpillStore(DiskSpillStore.create(services), services, () -> STORES.remove(services.nodeId(), holder[0]));
        STORES.put(services.nodeId(), holder[0]);
        return holder[0];
    }

    /**
     * The store of the node with that id, for a test's counters; null before the ES|QL plugin created it or after
     * the node shut it down.
     */
    public static CrankySpillStore storeOf(String nodeId) {
        return STORES.get(nodeId);
    }
}
