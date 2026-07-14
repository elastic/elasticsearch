/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.engine;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.indices.breaker.BreakerSettings;

/**
 * Name and settings for the stateless {@code reader_heap} circuit breaker. Tracks heap reserved by the stateless
 * search engine for live-docs FixedBitSets, in-memory postings bytes and per-segment baseline. When the limit
 * is exceeded the engine defers the refresh and re-evaluates on the next cycle.
 */
public final class StatelessReaderHeapBreaker {

    public static final String NAME = "stateless_reader_heap";

    /**
     * Limit on the reader heap the stateless search engine will reserve. {@code -1} disables enforcement; any
     * positive value (absolute bytes or a heap percentage) causes refreshes that would exceed it to be deferred.
     */
    public static final Setting<ByteSizeValue> LIMIT_SETTING = Setting.memorySizeSetting(
        "stateless.search.reader_heap_breaker.limit",
        "-1",
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private StatelessReaderHeapBreaker() {}

    public static BreakerSettings breakerSettings(Settings settings) {
        return new BreakerSettings(
            NAME,
            LIMIT_SETTING.get(settings).getBytes(),
            1.0,
            CircuitBreaker.Type.MEMORY,
            CircuitBreaker.Durability.TRANSIENT
        );
    }
}
