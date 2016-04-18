/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.marvel.MonitoringSettings;
import org.elasticsearch.marvel.agent.collector.Collector;
import org.elasticsearch.marvel.agent.collector.cluster.ClusterStatsCollector;
import org.elasticsearch.marvel.agent.exporter.ExportException;
import org.elasticsearch.marvel.agent.exporter.Exporter;
import org.elasticsearch.marvel.agent.exporter.Exporters;
import org.elasticsearch.marvel.agent.exporter.MonitoringDoc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

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
    private volatile long samplingIntervalMillis;
    private final Collection<Collector> collectors;
    private final String[] settingsCollectors;
    private final Exporters exporters;

    @Inject
    public AgentService(Settings settings, ClusterSettings clusterSettings, Set<Collector> collectors, Exporters exporters) {
        super(settings);
        this.samplingIntervalMillis = MonitoringSettings.INTERVAL.get(settings).millis();
        this.settingsCollectors = MonitoringSettings.COLLECTORS.get(settings).toArray(new String[0]);
        this.collectors = Collections.unmodifiableSet(filterCollectors(collectors, settingsCollectors));
        this.exporters = exporters;

        clusterSettings.addSettingsUpdateConsumer(MonitoringSettings.INTERVAL, this::setInterval);
    }

    private void setInterval(TimeValue interval) {
        this.samplingIntervalMillis = interval.millis();
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
        if (samplingIntervalMillis <= 0) {
            logger.info("data sampling is disabled due to interval settings [{}]", samplingIntervalMillis);
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

    /** stop collection and exporting. this method blocks until all background activity is guaranteed to be stopped */
    public void stopCollection() {
        final ExportingWorker worker = this.exportingWorker;
        if (worker != null) {
            worker.stopCollecting();
        }
    }

    public void startCollection() {
        final ExportingWorker worker = this.exportingWorker;
        if (worker != null) {
            worker.collecting = true;
        }
    }

    @Override
    protected void doStart() {
        logger.info("monitoring service started");

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
            try {
                exporter.close();
            } catch (Exception e) {
                logger.error("failed to close exporter [{}]", e, exporter.name());
            }
        }
    }

    public TimeValue getSamplingInterval() {
        return TimeValue.timeValueMillis(samplingIntervalMillis);
    }

    public String[] collectors() {
        return settingsCollectors;
    }

    class ExportingWorker implements Runnable {

        volatile boolean closed = false;
        volatile boolean collecting = true;

        final ReleasableLock collectionLock = new ReleasableLock(new ReentrantLock(false));

        @Override
        public void run() {
            while (!closed) {
                // sleep first to allow node to complete initialization before collecting the first start
                try {
                    Thread.sleep(samplingIntervalMillis);

                    if (closed) {
                        continue;
                    }

                    try (Releasable ignore = collectionLock.acquire()) {

                        Collection<MonitoringDoc> docs = collect();

                        if ((docs.isEmpty() == false) && (closed == false)) {
                            exporters.export(docs);
                        }
                    }

                } catch (ExportException e) {
                    logger.error("exception when exporting documents", e);
                } catch (InterruptedException e) {
                    logger.trace("interrupted");
                    Thread.currentThread().interrupt();
                } catch (Throwable t) {
                    logger.error("background thread had an uncaught exception", t);
                }
            }
            logger.debug("worker shutdown");
        }

        /** stop collection and exporting. this method will be block until background collection is actually stopped */
        public void stopCollecting() {
            collecting = false;
            collectionLock.acquire().close();
        }

        private Collection<MonitoringDoc> collect() {
            if (logger.isTraceEnabled()) {
                logger.trace("collecting data - collectors [{}]", Strings.collectionToCommaDelimitedString(collectors));
            }

            Collection<MonitoringDoc> docs = new ArrayList<>();
            for (Collector collector : collectors) {
                if (collecting) {
                    Collection<MonitoringDoc> result = collector.collect();
                    if (result != null) {
                        logger.trace("adding [{}] collected docs from [{}] collector", result.size(), collector.name());
                        docs.addAll(result);
                    } else {
                        logger.trace("skipping collected docs from [{}] collector", collector.name());
                    }
                }
                if (closed) {
                    // Stop collecting if the worker is marked as closed
                    break;
                }
            }
            return docs;
        }
    }
}
