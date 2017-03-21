/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.monitoring.collector.Collector;
import org.elasticsearch.xpack.monitoring.exporter.Exporter;
import org.elasticsearch.xpack.monitoring.exporter.Exporters;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The {@code MonitoringService} is a service that does the work of publishing the details to the monitoring cluster.
 * <p>
 * If this service is stopped, then the attached, monitored node is not going to publish its details to the monitoring cluster. Given
 * service life cycles, the intended way to temporarily stop the publishing is using the start and stop methods.
 */
public class MonitoringService extends AbstractLifecycleComponent {

    /** State of the monitoring service, either started or stopped **/
    private final AtomicBoolean started = new AtomicBoolean(false);

    /** Task in charge of collecting and exporting monitoring data **/
    private final MonitoringExecution monitor = new MonitoringExecution();

    private final ThreadPool threadPool;
    private final Set<Collector> collectors;
    private final Exporters exporters;

    private volatile TimeValue interval;
    private volatile ThreadPool.Cancellable scheduler;

    MonitoringService(Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool,
                      Set<Collector> collectors, Exporters exporters) {
        super(settings);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.collectors = Objects.requireNonNull(collectors);
        this.exporters = Objects.requireNonNull(exporters);
        this.interval = MonitoringSettings.INTERVAL.get(settings);
        clusterSettings.addSettingsUpdateConsumer(MonitoringSettings.INTERVAL, this::setInterval);
    }

    void setInterval(TimeValue interval) {
        this.interval = interval;
        scheduleExecution();
    }

    public TimeValue getInterval() {
        return interval;
    }

    boolean isMonitoringActive() {
        return isStarted()
                && interval != null
                && interval.millis() >= MonitoringSettings.MIN_INTERVAL.millis();
    }

    private String threadPoolName() {
        return ThreadPool.Names.GENERIC;
    }

    boolean isStarted() {
        return started.get();
    }

    @Override
    protected void doStart() {
        if (started.compareAndSet(false, true)) {
            try {
                logger.debug("monitoring service is starting");
                scheduleExecution();
                logger.debug("monitoring service started");
            } catch (Exception e) {
                logger.error((Supplier<?>) () -> new ParameterizedMessage("failed to start monitoring service"), e);
                started.set(false);
                throw e;
            }
        }
    }

    @Override
    protected void doStop() {
        if (started.getAndSet(false)) {
            logger.debug("monitoring service is stopping");
            cancelExecution();
            logger.debug("monitoring service stopped");
        }
    }

    @Override
    protected void doClose() {
        logger.debug("monitoring service is closing");
        monitor.close();

        for (Exporter exporter : exporters) {
            try {
                exporter.close();
            } catch (Exception e) {
                logger.error((Supplier<?>) () -> new ParameterizedMessage("failed to close exporter [{}]", exporter.name()), e);
            }
        }
        logger.debug("monitoring service closed");
    }

    void scheduleExecution() {
        if (scheduler != null) {
            cancelExecution();
        }
        if (isMonitoringActive()) {
            scheduler = threadPool.scheduleWithFixedDelay(monitor, interval, threadPoolName());
        }
    }

    void cancelExecution() {
        if (scheduler != null) {
            try {
                scheduler.cancel();
            } finally {
                scheduler = null;
            }
        }
    }

    /**
     * {@link MonitoringExecution} is a scheduled {@link Runnable} that periodically checks if monitoring
     * data can be collected and exported. It runs at a given interval corresponding to the monitoring
     * sampling interval. It first checks if monitoring is still enabled (because it might have changed
     * since the last time the task was scheduled: interval set to -1 or the monitoring service is stopped).
     * Since collecting and exporting data can take time, it uses a semaphore to track the current execution.
     */
    class MonitoringExecution extends AbstractRunnable implements Closeable {

        /**
         * Binary semaphore used to wait for monitoring execution to terminate before closing or stopping
         * the monitoring service. A semaphore is preferred over a ReentrantLock because the lock is
         * obtained by a thread and released by another thread.
         **/
        private final Semaphore semaphore = new Semaphore(1);

        @Override
        public void doRun() {
            if (isMonitoringActive() == false) {
                logger.debug("monitoring execution is skipped");
                return;
            }

            if (semaphore.tryAcquire() == false) {
                logger.debug("monitoring execution is skipped until previous execution terminated");
                return;
            }

            threadPool.executor(threadPoolName()).submit(new AbstractRunnable() {
                @Override
                protected void doRun() throws Exception {
                    Collection<MonitoringDoc> results = new ArrayList<>();
                    for (Collector collector : collectors) {
                        if (isStarted() == false) {
                            // Do not collect more data if the the monitoring service is stopping
                            // otherwise some collectors might just fail.
                            return;
                        }

                        try {
                            Collection<MonitoringDoc> result = collector.collect();
                            if (result != null) {
                                results.addAll(result);
                            }
                        } catch (Exception e) {
                            logger.warn((Supplier<?>) () ->
                                    new ParameterizedMessage("monitoring collector [{}] failed to collect data", collector.name()), e);
                        }
                    }
                    if (isMonitoringActive()) {
                        exporters.export(results, ActionListener.wrap(r -> semaphore.release(), this::onFailure));
                    } else {
                        semaphore.release();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    logger.warn("monitoring execution failed", e);
                    semaphore.release();
                }

                @Override
                public void onRejection(Exception e) {
                    logger.warn("monitoring execution has been rejected", e);
                    semaphore.release();
                }
            });
        }

        @Override
        public void onFailure(Exception e) {
            logger.warn("monitoring execution failed", e);
        }

        @Override
        public void close() {
            try {
                // Block until the lock can be acquired or 10s. The timed try acquire is necessary as there may be a failure that causes
                // the semaphore to not get released and then the node will hang forever on shutdown
                if (semaphore.tryAcquire(10L, TimeUnit.SECONDS) == false) {
                    logger.warn("monitoring execution did not complete after waiting for 10s");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
