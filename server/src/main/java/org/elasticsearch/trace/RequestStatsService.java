/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.trace;

import org.elasticsearch.action.admin.cluster.stats.RequestStats;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

public class RequestStatsService {

    public enum RequestKind {
        READ,
        WRITE
    }

    public static final Setting<Boolean> REQUEST_STATS_ENABLED = Setting.boolSetting(
        "requests.stats.enabled",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile boolean enabled = REQUEST_STATS_ENABLED.get(Settings.EMPTY);

    private final EnumMap<RequestStats.Category, RequestTypeAccumulator> accumulators = new EnumMap<>(RequestStats.Category.class);

    private RequestStatsService() {
        accumulators.put(RequestStats.Category.TOTAL, new RequestTypeAccumulator());
        accumulators.put(RequestStats.Category.READ, new RequestTypeAccumulator());
        accumulators.put(RequestStats.Category.WRITE, new RequestTypeAccumulator());
    }

    private static final class InstanceHolder {
        private static final RequestStatsService INSTANCE = new RequestStatsService();
    }

    public static RequestStatsService getInstance() {
        return InstanceHolder.INSTANCE;
    }

    public void start(Settings settings, ClusterService clusterService) {
        this.enabled = REQUEST_STATS_ENABLED.get(settings);

        ClusterSettings clusterSettings = clusterService.getClusterSettings();
        clusterSettings.addSettingsUpdateConsumer(REQUEST_STATS_ENABLED, v -> enabled = v);
    }

    public void recordSuccess(RequestKind kind) {
        if (enabled == false) {
            return;
        }
        record(kind, true);
    }

    public void recordFailure(RequestKind kind) {
        if (enabled == false) {
            return;
        }
        record(kind, false);
    }

    private void record(RequestKind kind, boolean success) {
        RequestTypeAccumulator total = accumulators.get(RequestStats.Category.TOTAL);
        total.record(success);
        if (kind == RequestKind.READ) {
            accumulators.get(RequestStats.Category.READ).record(success);
        } else {
            accumulators.get(RequestStats.Category.WRITE).record(success);
        }
    }

    public RequestStats snapshot() {
        if (enabled == false) {
            return RequestStats.empty();
        }
        EnumMap<RequestStats.Category, RequestStats.TypeStats> map = new EnumMap<>(RequestStats.Category.class);
        for (Map.Entry<RequestStats.Category, RequestTypeAccumulator> entry : accumulators.entrySet()) {
            map.put(entry.getKey(), entry.getValue().snapshot());
        }
        return new RequestStats(map);
    }

    public boolean isEnabled() {
        return enabled;
    }

    private static final class RequestTypeAccumulator {
        private final LongAdder success = new LongAdder();
        private final LongAdder failed = new LongAdder();
        private final LongAdder total = new LongAdder();

        void record(boolean isSuccess) {
            total.increment();
            if (isSuccess) {
                success.increment();
            } else {
                failed.increment();
            }
        }

        RequestStats.TypeStats snapshot() {
            return new RequestStats.TypeStats(success.sum(), failed.sum(), total.sum());
        }
    }
}
