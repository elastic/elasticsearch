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

    private volatile TimeValue pitKeepAlive;

    /**
     * {@link ClusterSettings#initializeAndWatch} keeps the value of the settings updated
     */
    public ReindexSettings() {
        // For nodes that do not load ReindexPlugin, the TransportEnrichReindexAction constructor, and some tests,
        // still inject ReindexSettings for cross-module actions.
        // This uses the static default and skips dynamic updates.
        this.pitKeepAlive = REINDEX_PIT_KEEP_ALIVE_SETTING.get(Settings.EMPTY);
    }

    /**
     * {@link ClusterSettings#initializeAndWatch} keeps the value of the settings updated
     */
    public ReindexSettings(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(REINDEX_PIT_KEEP_ALIVE_SETTING, this::setPitKeepAlive);
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
}
