/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent;

import com.google.common.collect.ImmutableSet;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.marvel.agent.collector.Collector;
import org.elasticsearch.marvel.agent.exporter.Exporter;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;
import org.elasticsearch.marvel.license.LicenseService;
import org.elasticsearch.node.settings.NodeSettingsService;

import java.util.Collection;
import java.util.Set;

public class AgentService extends AbstractLifecycleComponent<AgentService> implements NodeSettingsService.Listener {

    private static final String SETTINGS_BASE = "marvel.agent.";

    public static final String SETTINGS_INTERVAL = SETTINGS_BASE + "interval";
    public static final String SETTINGS_ENABLED = SETTINGS_BASE + "enabled";

    public static final String SETTINGS_STATS_TIMEOUT = SETTINGS_BASE + "stats.timeout";

    private volatile ExportingWorker exportingWorker;

    private volatile Thread workerThread;
    private volatile long samplingInterval;

    private volatile TimeValue clusterStatsTimeout;

    private final Collection<Collector> collectors;
    private final Collection<Exporter> exporters;

    @Inject
    public AgentService(Settings settings, NodeSettingsService nodeSettingsService,
                        LicenseService licenseService,
                        Set<Collector> collectors, Set<Exporter> exporters) {
        super(settings);
        this.samplingInterval = settings.getAsTime(SETTINGS_INTERVAL, TimeValue.timeValueSeconds(10)).millis();

        TimeValue statsTimeout = settings.getAsTime(SETTINGS_STATS_TIMEOUT, TimeValue.timeValueMinutes(10));

        if (settings.getAsBoolean(SETTINGS_ENABLED, true)) {
            this.collectors = ImmutableSet.copyOf(collectors);
            this.exporters = ImmutableSet.copyOf(exporters);
        } else {
            this.collectors = ImmutableSet.of();
            this.exporters = ImmutableSet.of();
            logger.info("collecting disabled by settings");
        }

        nodeSettingsService.addListener(this);
        logger.trace("marvel is running in [{}] mode", licenseService.mode());
    }

    protected void applyIntervalSettings() {
        if (samplingInterval <= 0) {
            logger.info("data sampling is disabled due to interval settings [{}]", samplingInterval);
            if (workerThread != null) {

                // notify  worker to stop on its leisure, not to disturb an exporting operation
                exportingWorker.closed = true;

                exportingWorker = null;
                workerThread = null;
            }
        } else if (workerThread == null || !workerThread.isAlive()) {

            exportingWorker = new ExportingWorker();
            workerThread = new Thread(exportingWorker, EsExecutors.threadName(settings, "marvel.exporters"));
            workerThread.setDaemon(true);
            workerThread.start();

        }
    }

    @Override
    protected void doStart() {
        if (exporters.size() == 0) {
            return;
        }

        for (Collector collector : collectors) {
            collector.start();
        }

        for (Exporter exporter : exporters) {
            exporter.start();
        }

        applyIntervalSettings();
    }

    @Override
    protected void doStop() {
        if (exporters.size() == 0) {
            return;
        }
        if (workerThread != null && workerThread.isAlive()) {
            exportingWorker.closed = true;
            workerThread.interrupt();
            try {
                workerThread.join(60000);
            } catch (InterruptedException e) {
                // we don't care...
            }

        }

        for (Collector collector : collectors) {
            collector.stop();
        }

        for (Exporter exporter : exporters) {
            exporter.stop();
        }
    }

    @Override
    protected void doClose() {
        for (Collector collector : collectors) {
            collector.close();
        }

        for (Exporter exporter : exporters) {
            exporter.close();
        }
    }

    // used for testing
    public Collection<Exporter> getExporters() {
        return exporters;
    }

    @Override
    public void onRefreshSettings(Settings settings) {
        TimeValue newSamplingInterval = settings.getAsTime(SETTINGS_INTERVAL, null);
        if (newSamplingInterval != null && newSamplingInterval.millis() != samplingInterval) {
            logger.info("sampling interval updated to [{}]", newSamplingInterval);
            samplingInterval = newSamplingInterval.millis();
            applyIntervalSettings();
        }
    }

    class ExportingWorker implements Runnable {

        volatile boolean closed = false;

        @Override
        public void run() {
            while (!closed) {
                // sleep first to allow node to complete initialization before collecting the first start
                try {
                    Thread.sleep(samplingInterval);
                    if (closed) {
                        continue;
                    }

                    for (Collector collector : collectors) {
                        logger.trace("collecting {}", collector.name());
                        Collection<MarvelDoc> results = collector.collect();

                        if (results != null && !results.isEmpty()) {
                            for (Exporter exporter : exporters) {
                                exporter.export(results);
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Throwable t) {
                    logger.error("Background thread had an uncaught exception:", t);
                }
            }
            logger.debug("worker shutdown");
        }
    }
}
