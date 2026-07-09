/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;

/**
 * Holds reindex-related dynamic cluster settings
 */
public final class ReindexSettings {

    /**
     * Keep-alive for point-in-time search contexts used during reindexing.
     * When scroll-based search is used, the scroll timeout comes from the search request.
     * If the scroll timeout is set but pit is used, the scroll timeout is ignored in favor of this.
     * Minimum {@code 1ms}; default {@code 5m}; no upper bound.
     */
    public static final Setting<TimeValue> REINDEX_PIT_KEEP_ALIVE_SETTING = Setting.timeSetting(
        "cluster.reindex.pit.keep_alive",
        TimeValue.timeValueMinutes(15),
        TimeValue.timeValueMillis(1),
        Property.Dynamic,
        Property.NodeScope
    );

    /**
     * How many bytes must accumulate in a {@code BulkRequest} before reindex / update-by-query / delete-by-query
     * consult the REQUEST circuit breaker for the in-flight reservation. Setting this very high (e.g. {@code 1pb})
     * effectively disables the per-batch breaker check, which acts as a runtime escape hatch if the accounting
     * logic ever misbehaves in production.
     */
    public static final Setting<ByteSizeValue> REINDEX_MEMORY_ACCOUNTING_THRESHOLD_SETTING = Setting.byteSizeSetting(
        "cluster.reindex.memory_accounting_threshold",
        ByteSizeValue.of(1, ByteSizeUnit.MB),
        ByteSizeValue.of(1, ByteSizeUnit.MB),
        ByteSizeValue.ofBytes(Long.MAX_VALUE),
        Property.Dynamic,
        Property.NodeScope
    );

    /**
     * Absolute cap on the total bytes of {@code _source} (plus per-hit overhead) that a single search response can contribute
     * during reindex / update-by-query / delete-by-query. The effective per-search cap is the lesser of this value and
     * {@code breakerLimit × cluster.reindex.search_response_breaker_fraction}. Defaults to 100 MB (the same as the
     * {@code HeapBufferedAsyncResponseConsumer} remote-fetch cap) so normal searches are unaffected.
     */
    public static final Setting<ByteSizeValue> REINDEX_SEARCH_RESPONSE_MAX_BYTES_SETTING = Setting.byteSizeSetting(
        "cluster.reindex.search_response_max_bytes",
        ByteSizeValue.of(100, ByteSizeUnit.MB),
        ByteSizeValue.of(1, ByteSizeUnit.KB),
        ByteSizeValue.ofBytes(Long.MAX_VALUE),
        Property.Dynamic,
        Property.NodeScope
    );

    /**
     * Fraction of the REQUEST circuit breaker limit to use as the per-search byte cap for reindex / update-by-query /
     * delete-by-query. Combined with {@link #REINDEX_SEARCH_RESPONSE_MAX_BYTES_SETTING}: the effective cap is
     * {@code min(maxBytes, breakerLimit × fraction)}. Default is 1% — keeps the search response well inside the
     * budget that the bulk request also draws from on the same breaker.
     */
    public static final Setting<Double> REINDEX_SEARCH_RESPONSE_BREAKER_FRACTION_SETTING = Setting.doubleSetting(
        "cluster.reindex.search_response_breaker_fraction",
        0.01,
        0.0001,
        1.0,
        Property.Dynamic,
        Property.NodeScope
    );

    private volatile TimeValue pitKeepAlive;
    private volatile long memoryAccountingThresholdInBytes;
    private volatile long searchResponseMaxBytes;
    private volatile double searchResponseBreakerFraction;

    /**
     * {@link ClusterSettings#initializeAndWatch} keeps the value of the settings updated
     */
    public ReindexSettings() {
        // For nodes that do not load ReindexPlugin, the TransportEnrichReindexAction constructor, and some tests,
        // still inject ReindexSettings for cross-module actions.
        // This uses the static default and skips dynamic updates.
        this.pitKeepAlive = REINDEX_PIT_KEEP_ALIVE_SETTING.get(Settings.EMPTY);
        this.memoryAccountingThresholdInBytes = REINDEX_MEMORY_ACCOUNTING_THRESHOLD_SETTING.get(Settings.EMPTY).getBytes();
        this.searchResponseMaxBytes = REINDEX_SEARCH_RESPONSE_MAX_BYTES_SETTING.get(Settings.EMPTY).getBytes();
        this.searchResponseBreakerFraction = REINDEX_SEARCH_RESPONSE_BREAKER_FRACTION_SETTING.get(Settings.EMPTY);
    }

    /**
     * {@link ClusterSettings#initializeAndWatch} keeps the value of the settings updated
     */
    public ReindexSettings(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(REINDEX_PIT_KEEP_ALIVE_SETTING, this::setPitKeepAlive);
        clusterSettings.initializeAndWatch(REINDEX_MEMORY_ACCOUNTING_THRESHOLD_SETTING, this::setMemoryAccountingThreshold);
        clusterSettings.initializeAndWatch(REINDEX_SEARCH_RESPONSE_MAX_BYTES_SETTING, v -> this.searchResponseMaxBytes = v.getBytes());
        clusterSettings.initializeAndWatch(REINDEX_SEARCH_RESPONSE_BREAKER_FRACTION_SETTING, v -> this.searchResponseBreakerFraction = v);
    }

    /**
     * Keep-alive for point-in-time contexts during reindex when PIT-based pagination is used.
     */
    public TimeValue pitKeepAlive() {
        return pitKeepAlive;
    }

    private void setPitKeepAlive(TimeValue pitKeepAlive) {
        this.pitKeepAlive = pitKeepAlive;
    }

    /**
     * Byte threshold at which buildBulk consults the REQUEST circuit breaker during a batch.
     */
    public long getMemoryAccountingThresholdInBytes() {
        return memoryAccountingThresholdInBytes;
    }

    private void setMemoryAccountingThreshold(ByteSizeValue memoryAccountingThreshold) {
        this.memoryAccountingThresholdInBytes = memoryAccountingThreshold.getBytes();
    }

    /**
     * Absolute ceiling on the per-search response byte cap.
     */
    public long getSearchResponseMaxBytes() {
        return searchResponseMaxBytes;
    }

    /**
     * Fraction of the REQUEST breaker limit used to derive the per-search response byte cap.
     */
    public double getSearchResponseBreakerFraction() {
        return searchResponseBreakerFraction;
    }
}
