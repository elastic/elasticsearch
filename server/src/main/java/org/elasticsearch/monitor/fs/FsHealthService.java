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
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.monitor.NodeHealthService;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * Runs periodically and attempts to create a temp file to see if the filesystem is writable. If not then it marks the
 * path as unhealthy.
 */
public class FsHealthService extends AbstractLifecycleComponent implements NodeHealthService {

    private static final Logger logger = LogManager.getLogger(FsHealthService.class);

    private final ThreadPool threadPool;
    private volatile boolean enabled;
    private volatile TimeValue refreshInterval;
    private volatile TimeValue healthCheckTimeoutInterval;
    private final NodeEnvironment nodeEnv;
    private final LongSupplier currentTimeMillisSupplier;
    private Map<Path, Status> pathHealthStats;
    private volatile Scheduler.Cancellable scheduledFuture;
    private final AtomicLong lastRunTimeMillis = new AtomicLong();


    public static final Setting<Boolean> ENABLED_SETTING =
        Setting.boolSetting("monitor.fs.health.enabled", true, Setting.Property.NodeScope, Setting.Property.Dynamic);
    public static final Setting<TimeValue> REFRESH_INTERVAL_SETTING =
        Setting.timeSetting("monitor.fs.health.refresh_interval", TimeValue.timeValueSeconds(1), TimeValue.timeValueMillis(10),
            Setting.Property.NodeScope);
    public static final Setting<TimeValue> HEALTHY_TIMEOUT_SETTING =
        Setting.timeSetting("monitor.fs.health.healthy_timeout", TimeValue.timeValueSeconds(1), TimeValue.timeValueMillis(1),
            Setting.Property.NodeScope, Setting.Property.Dynamic);

    public FsHealthService(Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool, NodeEnvironment nodeEnv) {
        this.threadPool = threadPool;
        this.enabled = ENABLED_SETTING.get(settings);
        this.refreshInterval = REFRESH_INTERVAL_SETTING.get(settings);
        this.healthCheckTimeoutInterval = HEALTHY_TIMEOUT_SETTING.get(settings);
        this.currentTimeMillisSupplier = threadPool::relativeTimeInMillis;
        this.nodeEnv = nodeEnv;
        this.pathHealthStats = new HashMap<>(1);
        clusterSettings.addSettingsUpdateConsumer(HEALTHY_TIMEOUT_SETTING, this::setHealthCheckTimeoutInterval);
        clusterSettings.addSettingsUpdateConsumer(ENABLED_SETTING, this::setEnabled);
    }

    @Override
    protected void doStart() {
        scheduledFuture = threadPool.scheduleWithFixedDelay(FsHealthMonitor::new, refreshInterval,
                ThreadPool.Names.GENERIC);
    }


    @Override
    protected void doStop() {
        scheduledFuture.cancel();
    }

    @Override
    protected void doClose() throws IOException {

    }

    public void setEnabled(boolean enabled) { this.enabled = enabled; }


    public void setHealthCheckTimeoutInterval(TimeValue healthCheckTimeoutInterval) {
        this.healthCheckTimeoutInterval = healthCheckTimeoutInterval;
    }

    @Override
    public Status getHealth() {
        if (enabled == false) {
            return Status.UNKNOWN;
        }
        else if (currentTimeMillisSupplier.getAsLong() - lastRunTimeMillis.get() > refreshInterval.millis() + healthCheckTimeoutInterval.millis()) {
            return Status.UNHEALTHY;
        }
        return pathHealthStats.entrySet().stream().anyMatch(map -> map.getValue() == Status.UNHEALTHY) ? Status.UNHEALTHY
            : Status.HEALTHY;
    }

     class FsHealthMonitor implements Runnable {

        static final String TEMP_FILE_NAME = ".es_temp_file";
        private byte[] byteToWrite;
        private AtomicBoolean checkInProgress;

        FsHealthMonitor(){
            this.byteToWrite = new byte[20];
            this.checkInProgress = new AtomicBoolean();
        }
        @Override
        public void run() {
            try {
                if (enabled) {
                    monitorFSHealth();
                    logger.debug("Monitor for disk health ran successfully {}", pathHealthStats);
                }
            } catch (Exception e) {
                logger.error("Exception monitoring disk health", e);
            }
        }

        private void monitorFSHealth() {
            if (checkInProgress.compareAndSet(false, true) == false) {
                logger.warn("Skipping Monitor for disk health as a check is already in progress");
                return;
            }
            for (Path path : nodeEnv.nodeDataPaths()) {
                try {
                    if (Files.exists(path)) {
                        Path tempDataPath = path.resolve(TEMP_FILE_NAME);
                        Files.deleteIfExists(tempDataPath);
                        try (OutputStream os = Files.newOutputStream(tempDataPath, StandardOpenOption.CREATE_NEW)) {
                            new Random().nextBytes(byteToWrite);
                            os.write(byteToWrite);
                            IOUtils.fsync(tempDataPath, false);
                            pathHealthStats.put(path, Status.HEALTHY);
                        }
                        Files.delete(tempDataPath);
                    }
                } catch (Exception ex) {
                    logger.error("Failed to perform writes on path {} due to {}", path, ex);
                    pathHealthStats.put(path,  Status.UNHEALTHY);
                }
            }
            lastRunTimeMillis.getAndUpdate(l -> Math.max(l, currentTimeMillisSupplier.getAsLong()));
            final boolean checkFinished = checkInProgress.compareAndSet(true, false);
            assert checkFinished;
        }
    }

}

