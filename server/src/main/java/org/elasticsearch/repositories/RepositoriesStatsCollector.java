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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
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

    public static final TimeValue MIN_INTERVAL = TimeValue.timeValueSeconds(1L);

    public static final Setting<TimeValue> INTERVAL =
        Setting.timeSetting("repositories.monitoring.export.interval", TimeValue.timeValueMinutes(1L), MIN_INTERVAL,
            Setting.Property.Dynamic, Setting.Property.NodeScope);

    public static final Setting<Boolean> ENABLED =
        Setting.boolSetting("repositories.monitoring.export.enabled", true,
            Setting.Property.Dynamic, Setting.Property.NodeScope);

    private final StatsCollector statsCollector = new StatsCollector();

    private final Supplier<Map<String, RepositoryStats>> statsProvider;
    private final ThreadPool threadPool;
    private final Consumer<Map<String, RepositoryStats>> exporter;
    private final ClusterService clusterService;
    private volatile TimeValue interval;
    private volatile boolean enabled;
    private volatile Scheduler.Cancellable scheduler;

    public RepositoriesStatsCollector(final Settings settings,
                                      final ClusterService clusterService,
                                      final Supplier<Map<String, RepositoryStats>> statsSupplier,
                                      final ThreadPool threadPool) {
        this(settings, clusterService, statsSupplier, threadPool, RepositoriesStatsCollector::exportMetrics);
    }

    public RepositoriesStatsCollector(final Settings settings,
                                      final ClusterService clusterService,
                                      final Supplier<Map<String, RepositoryStats>> statsSupplier,
                                      final ThreadPool threadPool,
                                      final Consumer<Map<String, RepositoryStats>> exporter) {
        this.statsProvider = statsSupplier;
        this.threadPool = threadPool;
        this.exporter = exporter;
        this.interval = INTERVAL.get(settings);
        this.enabled = ENABLED.get(settings);
        this.clusterService = clusterService;
    }

    private static void exportMetrics(Map<String, RepositoryStats> repositoriesStats) {
        logger.info("Repositories stats {}", repositoriesStats);
    }

    private String threadPoolName() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected void doStart() {
        scheduleStatsCollection();
        clusterService.getClusterSettings().addSettingsUpdateConsumer(ENABLED, this::setEnabled);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(INTERVAL, this::setInterval);
    }

    @Override
    protected void doStop() {
        if (scheduler != null) {
            cancelScheduledCollection();
        }
    }

    @Override
    protected void doClose() {}

    private void scheduleStatsCollection() {
        if (scheduler != null) {
            cancelScheduledCollection();
        }

        if (isEnabled()) {
            scheduler = threadPool.scheduleWithFixedDelay(statsCollector, getInterval(), threadPoolName());
        }
    }

    private void cancelScheduledCollection() {
        if (scheduler != null) {
            try {
                scheduler.cancel();
            } finally {
                scheduler = null;
            }
        }
    }

    void setEnabled(boolean enabled) {
        this.enabled = enabled;
        scheduleStatsCollection();
    }

    void setInterval(TimeValue interval) {
        this.interval = interval;
        scheduleStatsCollection();
    }

    private boolean isEnabled() {
        return enabled;
    }

    private TimeValue getInterval() {
        return interval;
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
