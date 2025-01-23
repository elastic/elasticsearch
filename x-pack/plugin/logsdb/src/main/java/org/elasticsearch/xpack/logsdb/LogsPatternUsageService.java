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

/**
 * A component that check in the background whether there are data streams that match log-*-* pattern and if so records this as persistent
 * setting in cluster state. If logs-*-* data stream usage has been found then this component will no longer in the background.
 */
final class LogsPatternUsageService implements LocalNodeMasterListener {

    private static final String LOGS_PATTERN = "logs-*-*";
    private static final Logger LOGGER = LogManager.getLogger(LogsPatternUsageService.class);
    static final Setting<TimeValue> USAGE_CHECK_PERIOD = Setting.timeSetting(
        "logsdb.usage_check.period",
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

    volatile boolean isMaster;
    volatile boolean hasPriorLogsUsage;
    volatile Scheduler.Cancellable cancellable;

    LogsPatternUsageService(Client client, Settings nodeSettings, ThreadPool threadPool, Supplier<Metadata> metadataSupplier) {
        this.client = client;
        this.nodeSettings = nodeSettings;
        this.threadPool = threadPool;
        this.metadataSupplier = metadataSupplier;
    }

    @Override
    public void onMaster() {
        if (cancellable == null || cancellable.isCancelled()) {
            isMaster = true;
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
        TimeValue waitTime = USAGE_CHECK_PERIOD.get(nodeSettings);
        scheduleNext(waitTime);
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
                scheduleNext(TimeValue.ONE_MINUTE);
            }
        }, e -> {
            LOGGER.debug(() -> "Failed to update [" + LOGSDB_PRIOR_LOGS_USAGE.getKey() + "]", e);
            scheduleNext(TimeValue.ONE_MINUTE);
        }));
    }
}
