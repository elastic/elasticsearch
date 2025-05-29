/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.ClientHelper.LOGS_PATTERN_USAGE_ORIGIN;
import static org.elasticsearch.xpack.logsdb.LogsdbIndexModeSettingsProvider.LOGS_PATTERN;

/**
 * A component that checks in the background whether there are data streams that match <code>log-*-*</code> pattern and if so records this
 * as persistent setting in cluster state. If <code>logs-*-*</code> data stream usage has been found then this component will no longer
 * run in the background.
 * <p>
 * After {@link #onMaster()} is invoked, the first check is scheduled to run after 1 minute. If no <code>logs-*-*</code> data streams are
 * found, then the next check runs after 2 minutes. The schedule time will double if no data streams with <code>logs-*-*</code> pattern
 * are found up until the maximum configured period in the {@link #USAGE_CHECK_MAX_PERIOD} setting (defaults to 24 hours).
 * <p>
 * If during a check one or more <code>logs-*-*</code> data streams are found, then the {@link #LOGSDB_PRIOR_LOGS_USAGE} setting gets set
 * as persistent cluster setting and this component will not schedule new checks. The mentioned setting is visible in persistent settings
 * of cluster state and a signal that upon upgrading to 9.x logsdb will not be enabled by default for data streams matching the
 * <code>logs-*-*</code> pattern. It isn't recommended to manually set the {@link #LOGSDB_PRIOR_LOGS_USAGE} setting.
 */
final class LogsPatternUsageService implements LocalNodeMasterListener {

    private static final Logger LOGGER = LogManager.getLogger(LogsPatternUsageService.class);
    private static final TimeValue USAGE_CHECK_MINIMUM = TimeValue.timeValueSeconds(30);
    static final Setting<TimeValue> USAGE_CHECK_MAX_PERIOD = Setting.timeSetting(
        "logsdb.usage_check.max_period",
        new TimeValue(24, TimeUnit.HOURS),
        Setting.Property.NodeScope
    );
    static final Setting<Boolean> LOGSDB_PRIOR_LOGS_USAGE = Setting.boolSetting(
        "logsdb.prior_logs_usage",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final Client client;
    private final Settings nodeSettings;
    private final ThreadPool threadPool;
    private final Supplier<Metadata> metadataSupplier;

    // Initializing to 30s, so first time will run with a delay of 60s:
    volatile TimeValue nextWaitTime = USAGE_CHECK_MINIMUM;
    volatile boolean isMaster;
    volatile boolean hasPriorLogsUsage;
    volatile Scheduler.Cancellable cancellable;

    LogsPatternUsageService(Client client, Settings nodeSettings, ThreadPool threadPool, Supplier<Metadata> metadataSupplier) {
        this.client = new OriginSettingClient(client, LOGS_PATTERN_USAGE_ORIGIN);
        this.nodeSettings = nodeSettings;
        this.threadPool = threadPool;
        this.metadataSupplier = metadataSupplier;
    }

    @Override
    public void onMaster() {
        if (cancellable == null || cancellable.isCancelled()) {
            isMaster = true;
            nextWaitTime = USAGE_CHECK_MINIMUM;
            scheduleNext();
        }
    }

    @Override
    public void offMaster() {
        isMaster = false;
        if (cancellable != null && cancellable.isCancelled() == false) {
            cancellable.cancel();
            cancellable = null;
        }
    }

    void scheduleNext() {
        TimeValue maxWaitTime = USAGE_CHECK_MAX_PERIOD.get(nodeSettings);
        nextWaitTime = TimeValue.timeValueMillis(Math.min(nextWaitTime.millis() * 2, maxWaitTime.millis()));
        scheduleNext(nextWaitTime);
    }

    void scheduleNext(TimeValue waitTime) {
        if (isMaster && hasPriorLogsUsage == false) {
            try {
                cancellable = threadPool.schedule(this::check, waitTime, threadPool.generic());
            } catch (EsRejectedExecutionException e) {
                if (e.isExecutorShutdown()) {
                    LOGGER.debug("Failed to check; Shutting down", e);
                } else {
                    throw e;
                }
            }
        } else {
            LOGGER.debug("Skipping check, because [{}]/[{}]", isMaster, hasPriorLogsUsage);
        }
    }

    void check() {
        LOGGER.debug("Starting logs-*-* usage check");
        if (isMaster) {
            var metadata = metadataSupplier.get();
            if (LOGSDB_PRIOR_LOGS_USAGE.exists(metadata.persistentSettings())) {
                LOGGER.debug("Using persistent logs-*-* usage check");
                hasPriorLogsUsage = true;
                return;
            }

            if (hasLogsUsage(metadata)) {
                updateSetting();
            } else {
                LOGGER.debug("No usage found; Skipping check");
                scheduleNext();
            }
        } else {
            LOGGER.debug("No longer master; Skipping check");
        }
    }

    static boolean hasLogsUsage(Metadata metadata) {
        for (var dataStream : metadata.dataStreams().values()) {
            if (Regex.simpleMatch(LOGS_PATTERN, dataStream.getName())) {
                return true;
            }
        }
        return false;
    }

    void updateSetting() {
        var settingsToUpdate = Settings.builder().put(LOGSDB_PRIOR_LOGS_USAGE.getKey(), true).build();
        var request = new ClusterUpdateSettingsRequest(TimeValue.ONE_MINUTE, TimeValue.ONE_MINUTE);
        request.persistentSettings(settingsToUpdate);
        client.execute(ClusterUpdateSettingsAction.INSTANCE, request, ActionListener.wrap(resp -> {
            if (resp.isAcknowledged() && LOGSDB_PRIOR_LOGS_USAGE.exists(resp.getPersistentSettings())) {
                hasPriorLogsUsage = true;
                cancellable = null;
            } else {
                LOGGER.debug(() -> "unexpected response [" + LOGSDB_PRIOR_LOGS_USAGE.getKey() + "], retrying...");
                scheduleNext(TimeValue.ONE_MINUTE);
            }
        }, e -> {
            LOGGER.warn(() -> "Failed to update [" + LOGSDB_PRIOR_LOGS_USAGE.getKey() + "], retrying...", e);
            scheduleNext(TimeValue.ONE_MINUTE);
        }));
    }
}
