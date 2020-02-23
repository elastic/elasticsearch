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
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * Runs periodically and attempts to create a temp file to see if the filesystem is writable. If not then it marks the
 * path as unhealthy.
 */
public class FsHealthService extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(FsHealthService.class);

    private final ThreadPool threadPool;
    private volatile boolean enabled;
    private volatile TimeValue refreshInterval;
    private volatile TimeValue healthcheckTimeoutInterval;
    private final NodeEnvironment nodeEnv;
    private final LongSupplier currentTimeMillisSupplier;
    private AtomicLong lastSuccessfulRunTimeMillis = new AtomicLong(Long.MIN_VALUE);
    private Map<Path, Status> pathHealthStats;
    private volatile Scheduler.Cancellable scheduledFuture;

    enum Status { HEALTHY, UNHEALTHY, UNKNOWN }

    public static final Setting<Boolean> ENABLED_SETTING =
        Setting.boolSetting("monitor.fs.health.enabled", true, Setting.Property.NodeScope, Setting.Property.Dynamic);
    public static final Setting<TimeValue> REFRESH_INTERVAL_SETTING =
        Setting.timeSetting("monitor.fs.health.refresh_interval", TimeValue.timeValueSeconds(5), TimeValue.timeValueSeconds(1),
            Setting.Property.NodeScope, Setting.Property.Dynamic);
    public static final Setting<TimeValue> HEALTHCHECK_TIMEOUT_SETTING =
        Setting.timeSetting("monitor.fs.health.unhealthy_timeout", TimeValue.timeValueMinutes(5), TimeValue.timeValueMinutes(2),
            Setting.Property.NodeScope, Setting.Property.Dynamic);


    FsHealthService(Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool, NodeEnvironment nodeEnv,
                    LongSupplier currentTimeMillisSupplier) {
        this.threadPool = threadPool;
        this.enabled = ENABLED_SETTING.get(settings);
        this.refreshInterval = REFRESH_INTERVAL_SETTING.get(settings);
        this.healthcheckTimeoutInterval = HEALTHCHECK_TIMEOUT_SETTING.get(settings);
        this.currentTimeMillisSupplier = currentTimeMillisSupplier;
        this.nodeEnv = nodeEnv;
        this.pathHealthStats = new HashMap<>();
        clusterSettings.addSettingsUpdateConsumer(REFRESH_INTERVAL_SETTING, this::setRefreshInterval);
        clusterSettings.addSettingsUpdateConsumer(HEALTHCHECK_TIMEOUT_SETTING, this::setHealthcheckTimeoutInterval);
        clusterSettings.addSettingsUpdateConsumer(ENABLED_SETTING, this::setEnabled);
    }

    @Override
    protected void doStart() {
        //TODO check if this needs to be a part of a dedicated threadpool
        scheduledFuture = threadPool.scheduleWithFixedDelay(new FsHealthMonitor(), refreshInterval, ThreadPool.Names.SAME);
    }


    @Override
    protected void doStop() {
        scheduledFuture.cancel();
    }

    @Override
    protected void doClose() throws IOException {

    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public void setRefreshInterval(TimeValue refreshInterval) {
        this.refreshInterval = refreshInterval;
    }

    public void setHealthcheckTimeoutInterval(TimeValue healthcheckTimeoutInterval) {
        this.healthcheckTimeoutInterval = healthcheckTimeoutInterval;
    }

    public Boolean isWritable(Path path){
        if (!enabled){
            return null;
        }
        Status status = pathHealthStats.getOrDefault(path, Status.UNKNOWN);
        if (status == Status.UNHEALTHY)
            return Boolean.FALSE;
        else if (lastSuccessfulRunTimeMillis.get() < currentTimeMillisSupplier.getAsLong() - healthcheckTimeoutInterval.getMillis()){
            return Boolean.FALSE;
        }
        return Boolean.TRUE;
    }

    private class FsHealthMonitor implements Runnable {

        private static final String TEMP_FILE_NAME = ".es_temp_file";

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

        private void monitorFSHealth(){

            Map<Path, Status> pathHealthStats = new HashMap<>();
            try {
                for (Path path : nodeEnv.nodeDataPaths()) {
                    try {
                        if (Files.exists(path)) {
                            Path resolve = path.resolve(TEMP_FILE_NAME);
                            // delete any lingering file from a previous failure
                            Files.deleteIfExists(resolve);
                            Files.createFile(resolve);
                            Files.delete(resolve);
                            pathHealthStats.put(path, Status.HEALTHY);
                        }
                    }catch(IOException ex){
                        logger.error("Failed to perform writes on path {} due to {}", path, ex);
                        pathHealthStats.put(path, Status.UNHEALTHY);
                    } catch(Exception ex){
                        logger.error("Failed to perform writes on path {} due to {}", path, ex);
                        pathHealthStats.put(path, Status.UNKNOWN);
                    }
                }
                lastSuccessfulRunTimeMillis.getAndUpdate(l -> Math.max(l, currentTimeMillisSupplier.getAsLong()));
            }catch (Exception e){
                logger.error("Failed to list node paths ", e);
            }
            finalizeAndUpdate(pathHealthStats);
        }

        private void finalizeAndUpdate(Map<Path, Status> pathHealthMap) {
            pathHealthStats = pathHealthMap;
        }

    }

}

