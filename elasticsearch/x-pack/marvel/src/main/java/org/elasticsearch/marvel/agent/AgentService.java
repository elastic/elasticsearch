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
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.marvel.agent.collector.Collector;
import org.elasticsearch.marvel.agent.collector.cluster.ClusterStatsCollector;
import org.elasticsearch.marvel.agent.exporter.ExportBulk;
import org.elasticsearch.marvel.agent.exporter.Exporter;
import org.elasticsearch.marvel.agent.exporter.Exporters;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;
import org.elasticsearch.marvel.MarvelSettings;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * The {@code AgentService} is a service that does the work of publishing the details to the monitoring cluster.
 * <p>
 * If this service is stopped, then the attached, monitored node is not going to publish its details to the monitoring cluster. Given
 * service life cycles, the intended way to temporarily stop the publishing is using the start and stop collection methods.
 *
 * @see #stopCollection()
 * @see #startCollection()
 */
public class AgentService extends AbstractLifecycleComponent<AgentService> {

    private volatile ExportingWorker exportingWorker;

    private volatile Thread workerThread;
    private volatile long samplingInterval;
    private final Collection<Collector> collectors;
    private final String[] settingsCollectors;
    private final Exporters exporters;

    @Inject
    public AgentService(Settings settings, ClusterSettings clusterSettings, Set<Collector> collectors, Exporters exporters) {
        super(settings);
        samplingInterval = MarvelSettings.INTERVAL.get(settings).millis();
        settingsCollectors = MarvelSettings.COLLECTORS.get(settings).toArray(new String[0]);
        clusterSettings.addSettingsUpdateConsumer(MarvelSettings.INTERVAL, this::setInterval);
        this.collectors = Collections.unmodifiableSet(filterCollectors(collectors, settingsCollectors));
        this.exporters = exporters;
    }

    private void setInterval(TimeValue interval) {
        this.samplingInterval = interval.millis();
        applyIntervalSettings();
    }

    protected Set<Collector> filterCollectors(Set<Collector> collectors, String[] filters) {
        if (CollectionUtils.isEmpty(filters)) {
            return collectors;
        }

        Set<Collector> list = new HashSet<>();
        for (Collector collector : collectors) {
            if (Regex.simpleMatch(filters, collector.name().toLowerCase(Locale.ROOT))) {
                list.add(collector);
            } else if (collector instanceof ClusterStatsCollector) {
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
            workerThread = new Thread(exportingWorker, EsExecutors.threadName(settings, "monitoring.exporters"));
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

    public TimeValue getSamplingInterval() {
        return new TimeValue(samplingInterval, TimeUnit.MILLISECONDS);
    }

    public String[] collectors() {
        return settingsCollectors;
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
                                    logger.trace("bulk [{}] - adding [{}] collected docs from [{}] collector", bulk, docs.size(),
                                            collector.name());
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
