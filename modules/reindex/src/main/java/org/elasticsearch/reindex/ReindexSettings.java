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

/**
 * Holds reindex-related dynamic cluster settings
 */
public final class ReindexSettings {

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

    private volatile long memoryAccountingThresholdInBytes;

    /**
     * {@link ClusterSettings#initializeAndWatch} keeps the value of the settings updated
     */
    public ReindexSettings() {
        // For nodes that do not load ReindexPlugin, the TransportEnrichReindexAction constructor, and some tests,
        // still inject ReindexSettings for cross-module actions.
        // This uses the static default and skips dynamic updates.
        this.memoryAccountingThresholdInBytes = REINDEX_MEMORY_ACCOUNTING_THRESHOLD_SETTING.get(Settings.EMPTY).getBytes();
    }

    /**
     * {@link ClusterSettings#initializeAndWatch} keeps the value of the settings updated
     */
    public ReindexSettings(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(REINDEX_MEMORY_ACCOUNTING_THRESHOLD_SETTING, this::setMemoryAccountingThreshold);
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
}
