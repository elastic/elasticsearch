/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.repositories;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

public final class RepositoriesStatsCollector extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(RepositoriesStatsCollector.class);

    public static final Setting<TimeValue> INTERVAL =
        Setting.timeSetting("repositories.monitoring.export.interval", TimeValue.MINUS_ONE, TimeValue.MINUS_ONE,
            Setting.Property.Dynamic, Setting.Property.NodeScope);

    private final StatsCollector statsCollector = new StatsCollector();

    private final Supplier<Map<String, RepositoryStats>> statsProvider;
    private final ThreadPool threadPool;
    private final Consumer<Map<String, RepositoryStats>> exporter;
    private final ClusterSettings clusterSettings;
    private TimeValue interval;
    private Scheduler.Cancellable scheduledTask;

    public RepositoriesStatsCollector(final Settings settings,
                                      final ClusterSettings clusterSettings,
                                      final Supplier<Map<String, RepositoryStats>> statsSupplier,
                                      final ThreadPool threadPool) {
        this(settings, clusterSettings, statsSupplier, threadPool, RepositoriesStatsCollector::exportMetrics);
    }

    public RepositoriesStatsCollector(final Settings settings,
                                      final ClusterSettings clusterSettings,
                                      final Supplier<Map<String, RepositoryStats>> statsSupplier,
                                      final ThreadPool threadPool,
                                      final Consumer<Map<String, RepositoryStats>> exporter) {
        this.statsProvider = statsSupplier;
        this.threadPool = threadPool;
        this.exporter = exporter;
        this.interval = INTERVAL.get(settings);
        this.clusterSettings = clusterSettings;
    }

    private static void exportMetrics(Map<String, RepositoryStats> repositoriesStats) {
        logger.info("Repositories stats {}", repositoriesStats);
    }

    @Override
    protected synchronized void doStart() {
        scheduleStatsCollection();
        clusterSettings.addSettingsUpdateConsumer(INTERVAL, this::setInterval);
    }

    @Override
    protected synchronized void doStop() {
        cancelScheduledCollection();
    }

    @Override
    protected void doClose() {}

    private void scheduleStatsCollection() {
        cancelScheduledCollection();

        if (isEnabled()) {
            scheduledTask = threadPool.scheduleWithFixedDelay(statsCollector, interval, ThreadPool.Names.GENERIC);
        }
    }

    private void cancelScheduledCollection() {
        if (scheduledTask != null) {
            try {
                scheduledTask.cancel();
            } finally {
                scheduledTask = null;
            }
        }
    }

    synchronized void setInterval(TimeValue newInterval) {
        if (lifecycle.started() == false)
            return;

        interval = newInterval;
        scheduleStatsCollection();
    }

    private boolean isEnabled() {
        return interval.equals(TimeValue.MINUS_ONE) == false;
    }

    private class StatsCollector extends AbstractRunnable {
        @Override
        public void onFailure(Exception e) {
            logger.warn("Unable to export repositories metrics", e);
        }

        @Override
        protected void doRun() throws Exception {
            exporter.accept(statsProvider.get());
        }
    }
}
