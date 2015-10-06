/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.marvel.agent.collector.Collector;
import org.elasticsearch.marvel.agent.collector.cluster.ClusterInfoCollector;
import org.elasticsearch.marvel.agent.exporter.ExportBulk;
import org.elasticsearch.marvel.agent.exporter.Exporter;
import org.elasticsearch.marvel.agent.exporter.Exporters;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.node.settings.NodeSettingsService;

import java.util.*;

public class AgentService extends AbstractLifecycleComponent<AgentService> implements NodeSettingsService.Listener {

    private volatile ExportingWorker exportingWorker;

    private volatile Thread workerThread;
    private volatile long samplingInterval;

    private final MarvelSettings marvelSettings;

    private final Collection<Collector> collectors;
    private final Exporters exporters;

    @Inject
    public AgentService(Settings settings, NodeSettingsService nodeSettingsService,
                        MarvelSettings marvelSettings, Set<Collector> collectors, Exporters exporters) {
        super(settings);
        this.marvelSettings = marvelSettings;
        this.samplingInterval = marvelSettings.interval().millis();

        this.collectors = Collections.unmodifiableSet(filterCollectors(collectors, marvelSettings.collectors()));
        this.exporters = exporters;

        nodeSettingsService.addListener(this);
    }

    protected Set<Collector> filterCollectors(Set<Collector> collectors, String[] filters) {
        if (CollectionUtils.isEmpty(filters)) {
            return collectors;
        }

        Set<Collector> list = new HashSet<>();
        for (Collector collector : collectors) {
            if (Regex.simpleMatch(filters, collector.name().toLowerCase(Locale.ROOT))) {
                list.add(collector);
            } else if (collector instanceof ClusterInfoCollector) {
                list.add(collector);
            }
        }
        return list;
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

    public void stopCollection() {
        if (exportingWorker != null) {
            exportingWorker.collecting = false;
        }
    }

    public void startCollection() {
        if (exportingWorker != null) {
            exportingWorker.collecting = true;
        }
    }

    @Override
    protected void doStart() {
        for (Collector collector : collectors) {
            collector.start();
        }
        exporters.start();
        applyIntervalSettings();
    }

    @Override
    protected void doStop() {
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

        exporters.stop();
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

    @Override
    public void onRefreshSettings(Settings settings) {
        TimeValue newSamplingInterval = settings.getAsTime(MarvelSettings.INTERVAL, null);
        if (newSamplingInterval != null && newSamplingInterval.millis() != samplingInterval) {
            logger.info("sampling interval updated to [{}]", newSamplingInterval);
            samplingInterval = newSamplingInterval.millis();
            applyIntervalSettings();
        }
    }

    class ExportingWorker implements Runnable {

        volatile boolean closed = false;
        volatile boolean collecting = true;

        @Override
        public void run() {
            while (!closed) {
                // sleep first to allow node to complete initialization before collecting the first start
                try {
                    Thread.sleep(samplingInterval);

                    if (closed) {
                        continue;
                    }

                    ExportBulk bulk = exporters.openBulk();
                    if (bulk == null) { // exporters are either not ready or faulty
                        continue;
                    }
                    try {
                        if (logger.isTraceEnabled()) {
                            logger.trace("collecting data - collectors [{}]", Strings.collectionToCommaDelimitedString(collectors));
                        }
                        for (Collector collector : collectors) {
                            if (collecting) {
                                Collection<MarvelDoc> docs = collector.collect();
                                if (docs != null) {
                                    logger.trace("bulk [{}] - adding [{}] collected docs from [{}] collector", bulk, docs.size(), collector.name());
                                    bulk.add(docs);
                                } else {
                                    logger.trace("bulk [{}] - skipping collected docs from [{}] collector", bulk, collector.name());
                                }
                            }
                            if (closed) {
                                // Stop collecting if the worker is marked as closed
                                break;
                            }
                        }
                    } finally {
                        bulk.close(!closed && collecting);
                    }

                } catch (InterruptedException e) {
                    logger.trace("interrupted");
                    Thread.currentThread().interrupt();
                } catch (Throwable t) {
                    logger.error("background thread had an uncaught exception", t);
                }
            }
            logger.debug("worker shutdown");
        }
    }
}
