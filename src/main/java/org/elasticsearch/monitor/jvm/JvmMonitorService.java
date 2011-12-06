/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.collect.ImmutableSet;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.monitor.dump.DumpGenerator;
import org.elasticsearch.monitor.dump.DumpMonitorService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;

import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;
import static org.elasticsearch.monitor.dump.summary.SummaryDumpContributor.SUMMARY;
import static org.elasticsearch.monitor.dump.thread.ThreadDumpContributor.THREAD_DUMP;
import static org.elasticsearch.monitor.jvm.DeadlockAnalyzer.deadlockAnalyzer;
import static org.elasticsearch.monitor.jvm.JvmStats.GarbageCollector;
import static org.elasticsearch.monitor.jvm.JvmStats.jvmStats;

/**
 *
 */
public class JvmMonitorService extends AbstractLifecycleComponent<JvmMonitorService> {

    private final ThreadPool threadPool;

    private final DumpMonitorService dumpMonitorService;

    private final boolean enabled;

    private final TimeValue interval;

    private final TimeValue gcThreshold;

    private volatile ScheduledFuture scheduledFuture;

    @Inject
    public JvmMonitorService(Settings settings, ThreadPool threadPool, DumpMonitorService dumpMonitorService) {
        super(settings);
        this.threadPool = threadPool;
        this.dumpMonitorService = dumpMonitorService;

        this.enabled = componentSettings.getAsBoolean("enabled", JvmStats.isLastGcEnabled());
        this.interval = componentSettings.getAsTime("interval", timeValueSeconds(1));
        this.gcThreshold = componentSettings.getAsTime("gc_threshold", timeValueMillis(5000));

        logger.debug("enabled [{}], last_gc_enabled [{}], interval [{}], gc_threshold [{}]", enabled, JvmStats.isLastGcEnabled(), interval, gcThreshold);
    }

    @Override
    protected void doStart() throws ElasticSearchException {
        if (!enabled) {
            return;
        }
        scheduledFuture = threadPool.scheduleWithFixedDelay(new JvmMonitor(), interval);
    }

    @Override
    protected void doStop() throws ElasticSearchException {
        if (!enabled) {
            return;
        }
        scheduledFuture.cancel(true);
    }

    @Override
    protected void doClose() throws ElasticSearchException {
    }

    private class JvmMonitor implements Runnable {

        private JvmStats lastJvmStats = jvmStats();

        private final Set<DeadlockAnalyzer.Deadlock> lastSeenDeadlocks = new HashSet<DeadlockAnalyzer.Deadlock>();

        public JvmMonitor() {
        }

        @Override
        public void run() {
//            monitorDeadlock();
            monitorLongGc();
        }

        private void monitorLongGc() {
            JvmStats currentJvmStats = jvmStats();

            for (int i = 0; i < currentJvmStats.gc().collectors().length; i++) {
                GarbageCollector gc = currentJvmStats.gc().collectors()[i];
                if (gc.lastGc() != null && lastJvmStats.gc.collectors()[i].lastGc() != null) {
                    GarbageCollector.LastGc lastGc = gc.lastGc();
                    if (lastGc.startTime == lastJvmStats.gc.collectors()[i].lastGc().startTime()) {
                        // we already handled this one...
                        continue;
                    }
                    // Ignore any duration > 1hr; getLastGcInfo occasionally returns total crap
                    if (lastGc.duration().hoursFrac() > 1) {
                        continue;
                    }
                    if (lastGc.duration().millis() > gcThreshold.millis()) {
                        logger.info("[gc][{}][{}] took [{}]/[{}], reclaimed [{}], leaving [{}] used, max [{}]", gc.name(), gc.getCollectionCount(), lastGc.duration(), gc.getCollectionTime(), lastGc.reclaimed(), lastGc.afterUsed(), lastGc.max());
                    } else if (logger.isDebugEnabled()) {
                        logger.debug("[gc][{}][{}] took [{}]/[{}], reclaimed [{}], leaving [{}] used, max [{}]", gc.name(), gc.getCollectionCount(), lastGc.duration(), gc.getCollectionTime(), lastGc.reclaimed(), lastGc.afterUsed(), lastGc.max());
                    }
                } else {
                    long collectionTime = gc.collectionTime().millis() - lastJvmStats.gc().collectors()[i].collectionTime().millis();
                    if (collectionTime > gcThreshold.millis()) {
                        logger.info("[gc][{}] collection occurred, took [{}]", gc.name(), new TimeValue(collectionTime));
                    }
                }
            }
            lastJvmStats = currentJvmStats;
        }

        private void monitorDeadlock() {
            DeadlockAnalyzer.Deadlock[] deadlocks = deadlockAnalyzer().findDeadlocks();
            if (deadlocks != null && deadlocks.length > 0) {
                ImmutableSet<DeadlockAnalyzer.Deadlock> asSet = new ImmutableSet.Builder<DeadlockAnalyzer.Deadlock>().add(deadlocks).build();
                if (!asSet.equals(lastSeenDeadlocks)) {
                    DumpGenerator.Result genResult = dumpMonitorService.generateDump("deadlock", null, SUMMARY, THREAD_DUMP);
                    StringBuilder sb = new StringBuilder("Detected Deadlock(s)");
                    for (DeadlockAnalyzer.Deadlock deadlock : asSet) {
                        sb.append("\n   ----> ").append(deadlock);
                    }
                    sb.append("\nDump generated [").append(genResult.location()).append("]");
                    logger.error(sb.toString());
                    lastSeenDeadlocks.clear();
                    lastSeenDeadlocks.addAll(asSet);
                }
            } else {
                lastSeenDeadlocks.clear();
            }
        }
    }
}
