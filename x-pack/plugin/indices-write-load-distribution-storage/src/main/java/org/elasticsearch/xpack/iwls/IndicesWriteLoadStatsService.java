/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.iwls;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.iwls.IndicesWriteLoadDistributionStoragePlugin.WRITE_LOAD_COLLECTOR_THREAD_POOL;

public class IndicesWriteLoadStatsService extends AbstractLifecycleComponent {
    public static final Setting<Boolean> ENABLED_SETTING = Setting.boolSetting(
        "indices.write_load.collect.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<TimeValue> SAMPLING_FREQUENCY_SETTING = Setting.timeSetting(
        "indices.write_load.collect.sampling_frequency",
        TimeValue.timeValueSeconds(1),
        TimeValue.timeValueMillis(500),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<TimeValue> STORE_FREQUENCY_SETTING = Setting.timeSetting(
        "indices.write_load.store.frequency",
        TimeValue.timeValueMinutes(1),
        TimeValue.timeValueSeconds(2),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final Logger logger = LogManager.getLogger(IndicesWriteLoadStatsService.class);

    private final IndicesWriteLoadStatsCollector indexShardWriteLoadStatsCollector;
    private final IndicesWriteLoadStore indicesWriteLoadStore;
    private final ThreadPool threadPool;
    private final AtomicBoolean started = new AtomicBoolean();

    private volatile TimeValue samplingFrequency;
    private volatile TimeValue storeFrequency;
    private volatile boolean enabled;

    private volatile Scheduler.Cancellable scheduledSampling;
    private volatile Scheduler.Cancellable scheduledStore;

    public static IndicesWriteLoadStatsService create(
        IndicesWriteLoadStatsCollector indicesWriteLoadStatsCollector,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        Settings settings
    ) {
        final IndicesWriteLoadStore indicesWriteLoadStore = new IndicesWriteLoadStore(
            threadPool,
            client,
            clusterService.getClusterSettings(),
            settings
        );
        return new IndicesWriteLoadStatsService(
            indicesWriteLoadStatsCollector,
            indicesWriteLoadStore,
            threadPool,
            clusterService.getClusterSettings(),
            settings
        );
    }

    IndicesWriteLoadStatsService(
        IndicesWriteLoadStatsCollector collector,
        IndicesWriteLoadStore indicesWriteLoadStore,
        ThreadPool threadPool,
        ClusterSettings clusterSettings,
        Settings settings
    ) {
        this.indexShardWriteLoadStatsCollector = collector;
        this.indicesWriteLoadStore = indicesWriteLoadStore;
        this.threadPool = threadPool;
        this.samplingFrequency = SAMPLING_FREQUENCY_SETTING.get(settings);
        this.storeFrequency = STORE_FREQUENCY_SETTING.get(settings);
        this.enabled = ENABLED_SETTING.get(settings);

        clusterSettings.addSettingsUpdateConsumer(SAMPLING_FREQUENCY_SETTING, this::setSamplingFrequency);
        clusterSettings.addSettingsUpdateConsumer(STORE_FREQUENCY_SETTING, this::setStoreFrequency);
        clusterSettings.addSettingsUpdateConsumer(ENABLED_SETTING, this::setEnabled);
    }

    @Override
    protected void doStart() {
        if (started.compareAndSet(false, true)) {
            maybeScheduleTasks();
        }
    }

    @Override
    protected void doStop() {
        if (started.compareAndSet(true, false)) {
            enabled = false;
            maybeCancelTasks();
        }
    }

    @Override
    protected void doClose() throws IOException {
        indicesWriteLoadStore.close();
    }

    private void setSamplingFrequency(TimeValue samplingFrequency) {
        this.samplingFrequency = samplingFrequency;
    }

    private void setStoreFrequency(TimeValue storeFrequency) {
        this.storeFrequency = storeFrequency;
    }

    private void setEnabled(boolean enabled) {
        this.enabled = enabled;
        if (enabled) {
            maybeScheduleTasks();
        } else {
            maybeCancelTasks();
        }
    }

    private void collectWriteLoadSamples() {
        if (enabled == false) {
            return;
        }

        try {
            indexShardWriteLoadStatsCollector.collectWriteLoadStats();
        } catch (Exception e) {
            logger.warn("Unable to collect write load stats", e);
        }

        maybeScheduleSampling();
    }

    private void storeWriteLoadDistributions() {
        if (enabled == false) {
            return;
        }

        try {
            final var writeLoadDistributions = indexShardWriteLoadStatsCollector.getWriteLoadDistributionAndReset();
            indicesWriteLoadStore.putAsync(writeLoadDistributions);
        } catch (Exception e) {
            logger.warn("Unable to store shard write load distributions", e);
        }

        maybeScheduleStore();
    }

    private void maybeScheduleTasks() {
        maybeScheduleSampling();
        maybeScheduleStore();
    }

    private void maybeCancelTasks() {
        if (scheduledSampling != null) {
            scheduledSampling.cancel();
        }

        if (scheduledStore != null) {
            scheduledStore.cancel();
        }
    }

    private void maybeScheduleSampling() {
        if (enabled == false) {
            return;
        }

        scheduledSampling = threadPool.schedule(this::collectWriteLoadSamples, samplingFrequency, WRITE_LOAD_COLLECTOR_THREAD_POOL);
    }

    private void maybeScheduleStore() {
        if (enabled == false) {
            return;
        }

        scheduledStore = threadPool.schedule(this::storeWriteLoadDistributions, storeFrequency, WRITE_LOAD_COLLECTOR_THREAD_POOL);
    }
}
