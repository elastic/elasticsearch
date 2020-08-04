/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.monitor.fs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.core.internal.io.IOUtils;
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
import java.util.HashSet;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static org.elasticsearch.monitor.StatusInfo.Status.HEALTHY;
import static org.elasticsearch.monitor.StatusInfo.Status.UNHEALTHY;

/**
 * Runs periodically and attempts to create a temp file to see if the filesystem is writable. If not then it marks the
 * path as unhealthy.
 */
public class FsHealthService extends AbstractLifecycleComponent implements NodeHealthService {

    private static final Logger logger = LogManager.getLogger(FsHealthService.class);
    private final ThreadPool threadPool;
    private volatile boolean enabled;
    private final TimeValue refreshInterval;
    private volatile TimeValue slowPathLoggingThreshold;
    private final NodeEnvironment nodeEnv;
    private final LongSupplier currentTimeMillisSupplier;
    private volatile Scheduler.Cancellable scheduledFuture;

    @Nullable
    private volatile Set<Path> unhealthyPaths;

    public static final Setting<Boolean> ENABLED_SETTING =
        Setting.boolSetting("monitor.fs.health.enabled", true, Setting.Property.NodeScope, Setting.Property.Dynamic);
    public static final Setting<TimeValue> REFRESH_INTERVAL_SETTING =
        Setting.timeSetting("monitor.fs.health.refresh_interval", TimeValue.timeValueSeconds(120), TimeValue.timeValueMillis(1),
            Setting.Property.NodeScope);
    public static final Setting<TimeValue> SLOW_PATH_LOGGING_THRESHOLD_SETTING =
        Setting.timeSetting("monitor.fs.health.slow_path_logging_threshold", TimeValue.timeValueSeconds(5), TimeValue.timeValueMillis(1),
            Setting.Property.NodeScope, Setting.Property.Dynamic);


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
        scheduledFuture = threadPool.scheduleWithFixedDelay(new FsHealthMonitor(), refreshInterval,
                ThreadPool.Names.GENERIC);
    }

    @Override
    protected void doStop() {
        scheduledFuture.cancel();
    }

    @Override
    protected void doClose() {
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public void setSlowPathLoggingThreshold(TimeValue slowPathLoggingThreshold) {
        this.slowPathLoggingThreshold = slowPathLoggingThreshold;
    }

    @Override
    public StatusInfo getHealth() {
        StatusInfo statusInfo;
        Set<Path> unhealthyPaths = this.unhealthyPaths;
        if (enabled == false) {
            statusInfo = new StatusInfo(HEALTHY, "health check disabled");
        } else if (unhealthyPaths == null) {
            statusInfo = new StatusInfo(HEALTHY, "health check passed");
        } else {
            String info = "health check failed on [" + unhealthyPaths.stream()
                .map(k -> k.toString()).collect(Collectors.joining(",")) + "]";
            statusInfo = new StatusInfo(UNHEALTHY, info);
        }
        return statusInfo;
    }

     class FsHealthMonitor implements Runnable {

        static final String TEMP_FILE_NAME = ".es_temp_file";
        private byte[] byteToWrite;

        FsHealthMonitor(){
            this.byteToWrite = UUIDs.randomBase64UUID().getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public void run() {
            try {
                if (enabled) {
                    monitorFSHealth();
                    logger.debug("health check succeeded");
                }
            } catch (Exception e) {
                logger.error("health check failed", e);
            }
        }

        private void monitorFSHealth() {
            Set<Path> currentUnhealthyPaths = null;
            for (Path path : nodeEnv.nodeDataPaths()) {
                long executionStartTime = currentTimeMillisSupplier.getAsLong();
                try {
                    if (Files.exists(path)) {
                        Path tempDataPath = path.resolve(TEMP_FILE_NAME);
                        Files.deleteIfExists(tempDataPath);
                        try (OutputStream os = Files.newOutputStream(tempDataPath, StandardOpenOption.CREATE_NEW)) {
                            os.write(byteToWrite);
                            IOUtils.fsync(tempDataPath, false);
                        }
                        Files.delete(tempDataPath);
                        final long elapsedTime = currentTimeMillisSupplier.getAsLong() - executionStartTime;
                        if (elapsedTime > slowPathLoggingThreshold.millis()) {
                            logger.warn("health check of [{}] took [{}ms] which is above the warn threshold of [{}]",
                                path, elapsedTime, slowPathLoggingThreshold);
                        }
                    }
                } catch (Exception ex) {
                    logger.error(new ParameterizedMessage("health check of [{}] failed", path), ex);
                    if (currentUnhealthyPaths == null) {
                        currentUnhealthyPaths = new HashSet<>(1);
                    }
                    currentUnhealthyPaths.add(path);
                }
            }
            unhealthyPaths = currentUnhealthyPaths;
        }
    }
}

