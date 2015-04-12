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

package org.elasticsearch.monitor.jvm;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.concurrent.ScheduledFuture;

import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;
import static org.elasticsearch.monitor.jvm.JvmStats.GarbageCollector;
import static org.elasticsearch.monitor.jvm.JvmStats.jvmStats;

/**
 *
 */
public class JvmMonitorService extends AbstractLifecycleComponent<JvmMonitorService> {

    private final ThreadPool threadPool;
    private final boolean enabled;
    private final TimeValue interval;
    private final ImmutableMap<String, GcThreshold> gcThresholds;

    private volatile ScheduledFuture scheduledFuture;

    static class GcThreshold {
        public final String name;
        public final long warnThreshold;
        public final long infoThreshold;
        public final long debugThreshold;

        GcThreshold(String name, long warnThreshold, long infoThreshold, long debugThreshold) {
            this.name = name;
            this.warnThreshold = warnThreshold;
            this.infoThreshold = infoThreshold;
            this.debugThreshold = debugThreshold;
        }

        @Override
        public String toString() {
            return "GcThreshold{" +
                    "name='" + name + '\'' +
                    ", warnThreshold=" + warnThreshold +
                    ", infoThreshold=" + infoThreshold +
                    ", debugThreshold=" + debugThreshold +
                    '}';
        }
    }

    @Inject
    public JvmMonitorService(Settings settings, ThreadPool threadPool) {
        super(settings);
        this.threadPool = threadPool;

        this.enabled = this.settings.getAsBoolean("monitor.jvm.enabled", true);
        this.interval = this.settings.getAsTime("monitor.jvm.interval", timeValueSeconds(1));

        MapBuilder<String, GcThreshold> gcThresholds = MapBuilder.newMapBuilder();
        Map<String, Settings> gcThresholdGroups = this.settings.getGroups("monitor.jvm.gc");
        for (Map.Entry<String, Settings> entry : gcThresholdGroups.entrySet()) {
            String name = entry.getKey();
            TimeValue warn = entry.getValue().getAsTime("warn", null);
            TimeValue info = entry.getValue().getAsTime("info", null);
            TimeValue debug = entry.getValue().getAsTime("debug", null);
            if (warn == null || info == null || debug == null) {
                logger.warn("ignoring gc_threshold for [{}], missing warn/info/debug values", name);
            } else {
                gcThresholds.put(name, new GcThreshold(name, warn.millis(), info.millis(), debug.millis()));
            }
        }
        if (!gcThresholds.containsKey(GcNames.YOUNG)) {
            gcThresholds.put(GcNames.YOUNG, new GcThreshold(GcNames.YOUNG, 1000, 700, 400));
        }
        if (!gcThresholds.containsKey(GcNames.OLD)) {
            gcThresholds.put(GcNames.OLD, new GcThreshold(GcNames.OLD, 10000, 5000, 2000));
        }
        if (!gcThresholds.containsKey("default")) {
            gcThresholds.put("default", new GcThreshold("default", 10000, 5000, 2000));
        }

        this.gcThresholds = gcThresholds.immutableMap();

        logger.debug("enabled [{}], interval [{}], gc_threshold [{}]", enabled, interval, this.gcThresholds);
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        if (!enabled) {
            return;
        }
        scheduledFuture = threadPool.scheduleWithFixedDelay(new JvmMonitor(), interval);
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        if (!enabled) {
            return;
        }
        FutureUtils.cancel(scheduledFuture);
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }

    private class JvmMonitor implements Runnable {

        private JvmStats lastJvmStats = jvmStats();
        private long seq = 0;

        public JvmMonitor() {
        }

        @Override
        public void run() {
            try {
                monitorLongGc();
            } catch (Throwable t) {
                logger.debug("failed to monitor", t);
            }
        }

        private synchronized void monitorLongGc() {
            seq++;
            JvmStats currentJvmStats = jvmStats();

            for (int i = 0; i < currentJvmStats.getGc().getCollectors().length; i++) {
                GarbageCollector gc = currentJvmStats.getGc().getCollectors()[i];
                GarbageCollector prevGc = lastJvmStats.gc.collectors[i];

                // no collection has happened
                long collections = gc.collectionCount - prevGc.collectionCount;
                if (collections == 0) {
                    continue;
                }
                long collectionTime = gc.collectionTime - prevGc.collectionTime;
                if (collectionTime == 0) {
                    continue;
                }

                GcThreshold gcThreshold = gcThresholds.get(gc.getName());
                if (gcThreshold == null) {
                    gcThreshold = gcThresholds.get("default");
                }

                long avgCollectionTime = collectionTime / collections;

                if (avgCollectionTime > gcThreshold.warnThreshold) {
                    logger.warn("[gc][{}][{}][{}] duration [{}], collections [{}]/[{}], total [{}]/[{}], memory [{}]->[{}]/[{}], all_pools {}",
                            gc.getName(), seq, gc.getCollectionCount(), TimeValue.timeValueMillis(collectionTime), collections, TimeValue.timeValueMillis(currentJvmStats.getTimestamp() - lastJvmStats.getTimestamp()), TimeValue.timeValueMillis(collectionTime), gc.getCollectionTime(), lastJvmStats.getMem().getHeapUsed(), currentJvmStats.getMem().getHeapUsed(), JvmInfo.jvmInfo().getMem().getHeapMax(), buildPools(lastJvmStats, currentJvmStats));
                } else if (avgCollectionTime > gcThreshold.infoThreshold) {
                    logger.info("[gc][{}][{}][{}] duration [{}], collections [{}]/[{}], total [{}]/[{}], memory [{}]->[{}]/[{}], all_pools {}",
                            gc.getName(), seq, gc.getCollectionCount(), TimeValue.timeValueMillis(collectionTime), collections, TimeValue.timeValueMillis(currentJvmStats.getTimestamp() - lastJvmStats.getTimestamp()), TimeValue.timeValueMillis(collectionTime), gc.getCollectionTime(), lastJvmStats.getMem().getHeapUsed(), currentJvmStats.getMem().getHeapUsed(), JvmInfo.jvmInfo().getMem().getHeapMax(), buildPools(lastJvmStats, currentJvmStats));
                } else if (avgCollectionTime > gcThreshold.debugThreshold && logger.isDebugEnabled()) {
                    logger.debug("[gc][{}][{}][{}] duration [{}], collections [{}]/[{}], total [{}]/[{}], memory [{}]->[{}]/[{}], all_pools {}",
                            gc.getName(), seq, gc.getCollectionCount(), TimeValue.timeValueMillis(collectionTime), collections, TimeValue.timeValueMillis(currentJvmStats.getTimestamp() - lastJvmStats.getTimestamp()), TimeValue.timeValueMillis(collectionTime), gc.getCollectionTime(), lastJvmStats.getMem().getHeapUsed(), currentJvmStats.getMem().getHeapUsed(), JvmInfo.jvmInfo().getMem().getHeapMax(), buildPools(lastJvmStats, currentJvmStats));
                }
            }
            lastJvmStats = currentJvmStats;
        }

        private String buildPools(JvmStats prev, JvmStats current) {
            StringBuilder sb = new StringBuilder();
            for (JvmStats.MemoryPool currentPool : current.getMem()) {
                JvmStats.MemoryPool prevPool = null;
                for (JvmStats.MemoryPool pool : prev.getMem()) {
                    if (pool.getName().equals(currentPool.getName())) {
                        prevPool = pool;
                        break;
                    }
                }
                sb.append("{[").append(currentPool.getName())
                        .append("] [").append(prevPool == null ? "?" : prevPool.getUsed()).append("]->[").append(currentPool.getUsed()).append("]/[").append(currentPool.getMax()).append("]}");
            }
            return sb.toString();
        }
    }
}
