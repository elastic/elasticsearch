/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.monitor.fs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.monitor.NodeHealthService;
import org.elasticsearch.monitor.StatusInfo;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static org.elasticsearch.monitor.StatusInfo.Status.HEALTHY;
import static org.elasticsearch.monitor.StatusInfo.Status.UNHEALTHY;

/**
 * Runs periodically and attempts to create a temp file to see if the filesystem is writable. If not then it marks the path as unhealthy.
 */
public class FsHealthService extends AbstractLifecycleComponent implements NodeHealthService {

    private static final Logger logger = LogManager.getLogger(FsHealthService.class);

    private static final StatusInfo HEALTHY_DISABLED = new StatusInfo(HEALTHY, "health check disabled");
    private static final StatusInfo UNHEALTHY_BROKEN_NODE_LOCK = new StatusInfo(UNHEALTHY, "health check failed due to broken node lock");
    private static final StatusInfo HEALTHY_SUCCESS = new StatusInfo(HEALTHY, "health check passed");

    private final ThreadPool threadPool;
    private volatile boolean enabled;
    private volatile boolean brokenLock;
    private final TimeValue refreshInterval;
    private volatile TimeValue slowPathLoggingThreshold;
    private final NodeEnvironment nodeEnv;
    private final LongSupplier currentTimeMillisSupplier;
    private Scheduler.Cancellable scheduledFuture; // accesses all synchronized on AbstractLifecycleComponent#lifecycle

    @Nullable
    private volatile Set<Path> unhealthyPaths;

    public static final Setting<Boolean> ENABLED_SETTING = Setting.boolSetting(
        "monitor.fs.health.enabled",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    public static final Setting<TimeValue> REFRESH_INTERVAL_SETTING = Setting.timeSetting(
        "monitor.fs.health.refresh_interval",
        TimeValue.timeValueSeconds(120),
        TimeValue.timeValueMillis(1),
        Setting.Property.NodeScope
    );
    public static final Setting<TimeValue> SLOW_PATH_LOGGING_THRESHOLD_SETTING = Setting.timeSetting(
        "monitor.fs.health.slow_path_logging_threshold",
        TimeValue.timeValueSeconds(5),
        TimeValue.timeValueMillis(1),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public FsHealthService(Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool, NodeEnvironment nodeEnv) {
        this.threadPool = threadPool;
        this.enabled = ENABLED_SETTING.get(settings);
        this.refreshInterval = REFRESH_INTERVAL_SETTING.get(settings);
        this.slowPathLoggingThreshold = SLOW_PATH_LOGGING_THRESHOLD_SETTING.get(settings);
        this.currentTimeMillisSupplier = threadPool::relativeTimeInMillis;
        this.nodeEnv = nodeEnv;
        clusterSettings.addSettingsUpdateConsumer(SLOW_PATH_LOGGING_THRESHOLD_SETTING, this::setSlowPathLoggingThreshold);
        clusterSettings.addSettingsUpdateConsumer(ENABLED_SETTING, this::setEnabled);
    }

    @Override
    protected void doStart() {
        scheduledFuture = threadPool.scheduleWithFixedDelay(new FsHealthMonitor(), refreshInterval, ThreadPool.Names.GENERIC);
    }

    @Override
    protected void doStop() {
        scheduledFuture.cancel();
    }

    @Override
    protected void doClose() {}

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public void setSlowPathLoggingThreshold(TimeValue slowPathLoggingThreshold) {
        this.slowPathLoggingThreshold = slowPathLoggingThreshold;
    }

    @Override
    public StatusInfo getHealth() {
        if (enabled == false) {
            return HEALTHY_DISABLED;
        }

        if (brokenLock) {
            return UNHEALTHY_BROKEN_NODE_LOCK;
        }

        var unhealthyPaths = this.unhealthyPaths; // single volatile read
        if (unhealthyPaths != null) {
            assert unhealthyPaths.isEmpty() == false;
            return new StatusInfo(
                UNHEALTHY,
                "health check failed on [" + unhealthyPaths.stream().map(Path::toString).collect(Collectors.joining(",")) + "]"
            );
        }

        return HEALTHY_SUCCESS;
    }

    class FsHealthMonitor extends AbstractRunnable {

        // Exposed for testing
        static final String TEMP_FILE_NAME = ".es_temp_file";

        private final byte[] bytesToWrite = UUIDs.randomBase64UUID().getBytes(StandardCharsets.UTF_8);

        @Override
        public void onFailure(Exception e) {
            logger.error("health check failed", e);
        }

        @Override
        public void onRejection(Exception e) {
            if (e instanceof EsRejectedExecutionException esre && esre.isExecutorShutdown()) {
                logger.debug("health check skipped (executor shut down)", e);
            } else {
                onFailure(e);
                assert false : e;
            }
        }

        @Override
        public void doRun() {
            if (enabled) {
                monitorFSHealth();
                logger.debug("health check completed");
            }
        }

        private void monitorFSHealth() {
            Set<Path> currentUnhealthyPaths = null;
            final Path[] paths;
            try {
                paths = nodeEnv.nodeDataPaths();
            } catch (IllegalStateException e) {
                logger.error("health check failed", e);
                brokenLock = true;
                return;
            }

            for (Path path : paths) {
                final long executionStartTime = currentTimeMillisSupplier.getAsLong();
                try {
                    if (Files.exists(path)) {
                        final Path tempDataPath = path.resolve(TEMP_FILE_NAME);
                        Files.deleteIfExists(tempDataPath);
                        try (OutputStream os = Files.newOutputStream(tempDataPath, StandardOpenOption.CREATE_NEW)) {
                            os.write(bytesToWrite);
                            IOUtils.fsync(tempDataPath, false);
                        }
                        Files.delete(tempDataPath);
                        final long elapsedTime = currentTimeMillisSupplier.getAsLong() - executionStartTime;
                        if (elapsedTime > slowPathLoggingThreshold.millis()) {
                            logger.warn(
                                "health check of [{}] took [{}ms] which is above the warn threshold of [{}]",
                                path,
                                elapsedTime,
                                slowPathLoggingThreshold
                            );
                        }
                    }
                } catch (Exception ex) {
                    logger.error(() -> "health check of [" + path + "] failed", ex);
                    if (currentUnhealthyPaths == null) {
                        currentUnhealthyPaths = Sets.newHashSetWithExpectedSize(1);
                    }
                    currentUnhealthyPaths.add(path);
                }
            }
            unhealthyPaths = currentUnhealthyPaths;
            brokenLock = false;
        }
    }
}
