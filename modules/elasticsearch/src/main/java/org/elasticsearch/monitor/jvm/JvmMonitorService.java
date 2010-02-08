/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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
import com.google.inject.Inject;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.monitor.dump.DumpGenerator;
import org.elasticsearch.monitor.dump.DumpMonitorService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.util.TimeValue;
import org.elasticsearch.util.component.AbstractComponent;
import org.elasticsearch.util.component.Lifecycle;
import org.elasticsearch.util.component.LifecycleComponent;
import org.elasticsearch.util.settings.Settings;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;

import static org.elasticsearch.monitor.dump.summary.SummaryDumpContributor.*;
import static org.elasticsearch.monitor.dump.thread.ThreadDumpContributor.*;
import static org.elasticsearch.monitor.jvm.DeadlockAnalyzer.*;
import static org.elasticsearch.monitor.jvm.JvmStats.*;
import static org.elasticsearch.util.TimeValue.*;

/**
 * @author kimchy (Shay Banon)
 */
public class JvmMonitorService extends AbstractComponent implements LifecycleComponent<JvmMonitorService> {

    private final Lifecycle lifecycle = new Lifecycle();

    private final ThreadPool threadPool;

    private final DumpMonitorService dumpMonitorService;

    private final boolean enabled;

    private final TimeValue interval;

    private final TimeValue gcCollectionWarning;

    private volatile ScheduledFuture scheduledFuture;

    @Inject public JvmMonitorService(Settings settings, ThreadPool threadPool, DumpMonitorService dumpMonitorService) {
        super(settings);
        this.threadPool = threadPool;
        this.dumpMonitorService = dumpMonitorService;

        this.enabled = componentSettings.getAsBoolean("enabled", true);
        this.interval = componentSettings.getAsTime("interval", timeValueSeconds(10));
        this.gcCollectionWarning = componentSettings.getAsTime("gcCollectionWarning", timeValueSeconds(10));
    }

    @Override public Lifecycle.State lifecycleState() {
        return lifecycle.state();
    }

    @Override public JvmMonitorService start() throws ElasticSearchException {
        if (!lifecycle.moveToStarted()) {
            return this;
        }
        if (!enabled) {
            return this;
        }
        scheduledFuture = threadPool.scheduleWithFixedDelay(new JvmMonitor(), interval);
        return this;
    }

    @Override public JvmMonitorService stop() throws ElasticSearchException {
        if (!lifecycle.moveToStopped()) {
            return this;
        }
        if (!enabled) {
            return this;
        }
        scheduledFuture.cancel(true);
        return this;
    }

    @Override public void close() {
        if (lifecycle.started()) {
            stop();
        }
        if (!lifecycle.moveToClosed()) {
            return;
        }
    }

    private class JvmMonitor implements Runnable {

        private JvmStats lastJvmStats = jvmStats();

        private final Set<DeadlockAnalyzer.Deadlock> lastSeenDeadlocks = new HashSet<DeadlockAnalyzer.Deadlock>();

        public JvmMonitor() {
        }

        @Override public void run() {
            monitorDeadlock();
            monitorLongGc();
        }

        private void monitorLongGc() {
            JvmStats currentJvmStats = jvmStats();
            long collectionTime = currentJvmStats.gcCollectionTime().millis() - lastJvmStats.gcCollectionTime().millis();
            if (collectionTime > gcCollectionWarning.millis()) {
                logger.warn("Long GC collection occurred, took [" + new TimeValue(collectionTime) + "], breached threshold [" + gcCollectionWarning + "]");
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
