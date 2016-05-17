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

import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.monitor.jvm.JvmStats.GarbageCollector;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.unmodifiableMap;

public class JvmGcMonitorService extends AbstractLifecycleComponent<JvmGcMonitorService> {

    private final ThreadPool threadPool;
    private final boolean enabled;
    private final TimeValue interval;
    private final Map<String, GcThreshold> gcThresholds;

    private volatile ScheduledFuture scheduledFuture;

    public final static Setting<Boolean> ENABLED_SETTING =
        Setting.boolSetting("monitor.jvm.gc.enabled", true, Property.NodeScope);
    public final static Setting<TimeValue> REFRESH_INTERVAL_SETTING =
        Setting.timeSetting("monitor.jvm.gc.refresh_interval", TimeValue.timeValueSeconds(1), TimeValue.timeValueSeconds(1),
            Property.NodeScope);

    private static String GC_COLLECTOR_PREFIX = "monitor.jvm.gc.collector.";
    public final static Setting<Settings> GC_SETTING = Setting.groupSetting(GC_COLLECTOR_PREFIX, Property.NodeScope);

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

    public JvmGcMonitorService(Settings settings, ThreadPool threadPool) {
        super(settings);
        this.threadPool = threadPool;

        this.enabled = ENABLED_SETTING.get(settings);
        this.interval = REFRESH_INTERVAL_SETTING.get(settings);

        Map<String, GcThreshold> gcThresholds = new HashMap<>();
        Map<String, Settings> gcThresholdGroups = GC_SETTING.get(settings).getAsGroups();
        for (Map.Entry<String, Settings> entry : gcThresholdGroups.entrySet()) {
            String name = entry.getKey();
            TimeValue warn = getValidThreshold(entry.getValue(), entry.getKey(), "warn");
            TimeValue info = getValidThreshold(entry.getValue(), entry.getKey(), "info");
            TimeValue debug = getValidThreshold(entry.getValue(), entry.getKey(), "debug");
            gcThresholds.put(name, new GcThreshold(name, warn.millis(), info.millis(), debug.millis()));
        }
        gcThresholds.putIfAbsent(GcNames.YOUNG, new GcThreshold(GcNames.YOUNG, 1000, 700, 400));
        gcThresholds.putIfAbsent(GcNames.OLD, new GcThreshold(GcNames.OLD, 10000, 5000, 2000));
        gcThresholds.putIfAbsent("default", new GcThreshold("default", 10000, 5000, 2000));
        this.gcThresholds = unmodifiableMap(gcThresholds);

        logger.debug("enabled [{}], interval [{}], gc_threshold [{}]", enabled, interval, this.gcThresholds);
    }

    private static TimeValue getValidThreshold(Settings settings, String key, String level) {
        TimeValue threshold = settings.getAsTime(level, null);
        if (threshold == null) {
            throw new IllegalArgumentException("missing gc_threshold for [" + getThresholdName(key, level) + "]");
        }
        if (threshold.nanos() <= 0) {
            throw new IllegalArgumentException("invalid gc_threshold [" + threshold + "] for [" + getThresholdName(key, level) + "]");
        }
        return threshold;
    }

    private static String getThresholdName(String key, String level) {
        return GC_COLLECTOR_PREFIX + key + "." + level;
    }

    private static final String LOG_MESSAGE =
        "[gc][{}][{}][{}] duration [{}], collections [{}]/[{}], total [{}]/[{}], memory [{}]->[{}]/[{}], all_pools {}";

    @Override
    protected void doStart() {
        if (!enabled) {
            return;
        }
        scheduledFuture = threadPool.scheduleWithFixedDelay(new JvmMonitor(gcThresholds) {
            @Override
            void onMonitorFailure(Throwable t) {
                logger.debug("failed to monitor", t);
            }

            @Override
            void onSlowGc(
                final Threshold threshold,
                final String name,
                final long seq,
                final TimeValue elapsed,
                final long totalGcCollectionCount,
                final long currentGcCollectionCount,
                final TimeValue totalGcCollectionTime, final TimeValue currentGcCollectionTime,
                final ByteSizeValue lastHeapUsed,
                final ByteSizeValue currentHeapUsed,
                final ByteSizeValue maxHeapUsed,
                final String pools) {
                logSlowGc(
                    logger,
                    threshold,
                    name,
                    seq,
                    elapsed,
                    totalGcCollectionCount,
                    currentGcCollectionCount,
                    totalGcCollectionTime,
                    currentGcCollectionTime,
                    lastHeapUsed,
                    currentHeapUsed,
                    maxHeapUsed,
                    pools);
            }
        }, interval);
    }

    static void logSlowGc(
        final ESLogger logger,
        final JvmMonitor.Threshold threshold,
        final String name,
        final long seq,
        final TimeValue elapsed,
        final long totalGcCollectionCount,
        final long currentGcCollectionCount,
        final TimeValue totalGcCollectionTime,
        final TimeValue currentGcCollectionTime,
        final ByteSizeValue lastHeapUsed,
        final ByteSizeValue currentHeapUsed,
        final ByteSizeValue maxHeapUsed,
        final String pools) {
        switch (threshold) {
            case WARN:
                if (logger.isWarnEnabled()) {
                    logger.warn(
                        LOG_MESSAGE,
                        name,
                        seq,
                        totalGcCollectionCount,
                        currentGcCollectionTime,
                        currentGcCollectionCount,
                        elapsed,
                        currentGcCollectionTime,
                        totalGcCollectionTime,
                        lastHeapUsed,
                        currentHeapUsed,
                        maxHeapUsed,
                        pools);
                }
                break;
            case INFO:
                if (logger.isInfoEnabled()) {
                    logger.info(
                        LOG_MESSAGE,
                        name,
                        seq,
                        totalGcCollectionCount,
                        currentGcCollectionTime,
                        currentGcCollectionCount,
                        elapsed,
                        currentGcCollectionTime,
                        totalGcCollectionTime,
                        lastHeapUsed,
                        currentHeapUsed,
                        maxHeapUsed,
                        pools);
                }
                break;
            case DEBUG:
                if (logger.isDebugEnabled()) {
                    logger.debug(
                        LOG_MESSAGE,
                        name,
                        seq,
                        totalGcCollectionCount,
                        currentGcCollectionTime,
                        currentGcCollectionCount,
                        elapsed,
                        currentGcCollectionTime,
                        totalGcCollectionTime,
                        lastHeapUsed,
                        currentHeapUsed,
                        maxHeapUsed,
                        pools);
                }
                break;
        }
    }

    @Override
    protected void doStop() {
        if (!enabled) {
            return;
        }
        FutureUtils.cancel(scheduledFuture);
    }

    @Override
    protected void doClose() {
    }

    static abstract class JvmMonitor implements Runnable {

        enum Threshold { DEBUG, INFO, WARN }

        private long lastTime = now();
        private JvmStats lastJvmStats = jvmStats();
        private long seq = 0;
        private final Map<String, GcThreshold> gcThresholds;

        public JvmMonitor(Map<String, GcThreshold> gcThresholds) {
            this.gcThresholds = Objects.requireNonNull(gcThresholds);
        }

        @Override
        public void run() {
            try {
                monitorLongGc();
            } catch (Throwable t) {
                onMonitorFailure(t);
            }
        }

        abstract void onMonitorFailure(Throwable t);

        synchronized void monitorLongGc() {
            seq++;
            final long currentTime = now();
            JvmStats currentJvmStats = jvmStats();

            for (int i = 0; i < currentJvmStats.getGc().getCollectors().length; i++) {
                GarbageCollector gc = currentJvmStats.getGc().getCollectors()[i];
                GarbageCollector prevGc = lastJvmStats.getGc().getCollectors()[i];

                // no collection has happened
                long collections = gc.getCollectionCount() - prevGc.getCollectionCount();
                if (collections == 0) {
                    continue;
                }
                long collectionTime = gc.getCollectionTime().millis() - prevGc.getCollectionTime().millis();
                if (collectionTime == 0) {
                    continue;
                }

                GcThreshold gcThreshold = gcThresholds.get(gc.getName());
                if (gcThreshold == null) {
                    gcThreshold = gcThresholds.get("default");
                }

                long avgCollectionTime = collectionTime / collections;

                Threshold threshold = null;
                if (avgCollectionTime > gcThreshold.warnThreshold) {
                    threshold = Threshold.WARN;
                } else if (avgCollectionTime > gcThreshold.infoThreshold) {
                    threshold = Threshold.INFO;
                } else if (avgCollectionTime > gcThreshold.debugThreshold) {
                    threshold = Threshold.DEBUG;
                }
                if (threshold != null) {
                    onSlowGc(
                        threshold,
                        gc.getName(),
                        seq,
                        TimeValue.timeValueMillis(TimeUnit.NANOSECONDS.toMillis(currentTime) - TimeUnit.NANOSECONDS.toMillis(lastTime)),
                        gc.getCollectionCount(),
                        collections,
                        gc.getCollectionTime(),
                        TimeValue.timeValueMillis(collectionTime),
                        lastJvmStats.getMem().getHeapUsed(),
                        currentJvmStats.getMem().getHeapUsed(),
                        JvmInfo.jvmInfo().getMem().getHeapMax(),
                        buildPools(lastJvmStats, currentJvmStats));
                }
            }
            lastTime = currentTime;
            lastJvmStats = currentJvmStats;
        }

        JvmStats jvmStats() {
            return JvmStats.jvmStats();
        }

        long now() {
            return System.nanoTime();
        }

        abstract void onSlowGc(
            final Threshold threshold,
            final String name,
            final long seq,
            final TimeValue elapsed,
            final long totalGcCollectionCount,
            final long currentGcCollectionCount,
            final TimeValue totalGcCollectionTime,
            final TimeValue currentGcCollectionTime,
            final ByteSizeValue lastHeapUsed,
            final ByteSizeValue currentHeapUsed,
            final ByteSizeValue maxHeapUsed,
            final String pools);

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
                sb.append("{[")
                    .append(currentPool.getName())
                    .append("] [")
                    .append(prevPool == null ? "?" : prevPool.getUsed())
                    .append("]->[")
                    .append(currentPool.getUsed())
                    .append("]/[")
                    .append(currentPool.getMax())
                    .append("]}");
            }
            return sb.toString();
        }
    }

}
