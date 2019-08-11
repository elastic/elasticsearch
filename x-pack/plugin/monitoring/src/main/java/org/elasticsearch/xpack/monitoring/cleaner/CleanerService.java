/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.cleaner;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractLifecycleRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.monitoring.MonitoringField;

import java.time.Clock;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * {@code CleanerService} takes care of deleting old monitoring indices.
 */
public class CleanerService extends AbstractLifecycleComponent {
    private static final Logger logger = LogManager.getLogger(CleanerService.class);

    private final XPackLicenseState licenseState;
    private final ThreadPool threadPool;
    private final ExecutionScheduler executionScheduler;
    private final List<Listener> listeners = new CopyOnWriteArrayList<>();
    private final IndicesCleaner runnable;

    private volatile TimeValue globalRetention;

    CleanerService(Settings settings, ClusterSettings clusterSettings, XPackLicenseState licenseState, ThreadPool threadPool,
                   ExecutionScheduler executionScheduler) {
        this.licenseState = licenseState;
        this.threadPool = threadPool;
        this.executionScheduler = executionScheduler;
        this.globalRetention = MonitoringField.HISTORY_DURATION.get(settings);
        this.runnable = new IndicesCleaner();

        // the validation is performed by the setting's object itself
        clusterSettings.addSettingsUpdateConsumer(MonitoringField.HISTORY_DURATION, this::setGlobalRetention);
    }

    public CleanerService(Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool, XPackLicenseState licenseState) {
        this(settings, clusterSettings, licenseState, threadPool, new DefaultExecutionScheduler());
    }

    @Override
    protected void doStart() {
        logger.debug("starting cleaning service");
        threadPool.schedule(runnable, executionScheduler.nextExecutionDelay(ZonedDateTime.now(Clock.systemDefaultZone())),
            executorName());
        logger.debug("cleaning service started");
    }

    @Override
    protected void doStop() {
        logger.debug("stopping cleaning service");
        listeners.clear();
        logger.debug("cleaning service stopped");
    }

    @Override
    protected void doClose() {
        logger.debug("closing cleaning service");
        runnable.cancel();
        logger.debug("cleaning service closed");
    }

    private String executorName() {
        return ThreadPool.Names.GENERIC;
    }

    /**
     * Get the retention that can be used.
     * <p>
     * This will ignore the global retention if the license does not allow retention updates.
     *
     * @return Never {@code null}
     * @see XPackLicenseState#isUpdateRetentionAllowed()
     */
    public TimeValue getRetention() {
        // we only care about their value if they are allowed to set it
        if (licenseState.isUpdateRetentionAllowed() && globalRetention != null) {
            return globalRetention;
        }
        else {
            return MonitoringField.HISTORY_DURATION.getDefault(Settings.EMPTY);
        }
    }

    /**
     * Set the global retention. This is expected to be used by the cluster settings to dynamically control the global retention time.
     * <p>
     * Even if the current license prevents retention updates, it will accept the change so that they do not need to re-set it if they
     * upgrade their license (they can always unset it).
     *
     * @param globalRetention The global retention to use dynamically.
     */
    public void setGlobalRetention(TimeValue globalRetention) {
        // notify the user that their setting will be ignored until they get the right license
        if (licenseState.isUpdateRetentionAllowed() == false) {
            logger.warn("[{}] setting will be ignored until an appropriate license is applied", MonitoringField.HISTORY_DURATION.getKey());
        }

        this.globalRetention = globalRetention;
    }

    /**
     * Add a {@code listener} that is executed by the internal {@code IndicesCleaner} given the {@link #getRetention() retention} time.
     *
     * @param listener A listener used to control retention
     */
    public void add(Listener listener) {
        listeners.add(listener);
    }

    /**
     * Remove a {@code listener}.
     *
     * @param listener A listener used to control retention
     * @see #add(Listener)
     */
    public void remove(Listener listener) {
        listeners.remove(listener);
    }

    /**
     * Listener that get called when indices must be cleaned
     */
    public interface Listener {

        /**
         * This method is called on listeners so that they can
         * clean indices.
         *
         * @param retention global retention value, it can be overridden at exporter level
         */
        void onCleanUpIndices(TimeValue retention);
    }

    /**
     * {@code IndicesCleaner} runs and reschedules itself in order to automatically clean (delete) indices that are outside of the
     * {@link #getRetention() retention} period.
     */
    class IndicesCleaner extends AbstractLifecycleRunnable {

        private volatile Scheduler.Cancellable cancellable;

        /**
         * Enable automatic logging and stopping of the runnable based on the {@link #lifecycle}.
         */
        IndicesCleaner() {
            super(lifecycle, logger);
        }

        @Override
        protected void doRunInLifecycle() throws Exception {
            if (licenseState.isMonitoringAllowed() == false) {
                logger.debug("cleaning service is disabled due to invalid license");
                return;
            }

            // fetch the retention, which is depends on a bunch of rules
            TimeValue retention = getRetention();

            logger.trace("cleaning up indices with retention [{}]", retention);

            // Note: listeners are free to override the retention
            for (Listener listener : listeners) {
                try {
                    listener.onCleanUpIndices(retention);
                } catch (Exception e) {
                    logger.error("listener failed to clean indices", e);
                }
            }

            logger.trace("done cleaning up indices");
        }

        /**
         * Reschedule the cleaner if the service is not stopped.
         */
        @Override
        protected void onAfterInLifecycle() {
            ZonedDateTime start = ZonedDateTime.now(Clock.systemUTC());
            TimeValue delay = executionScheduler.nextExecutionDelay(start);

            logger.debug("scheduling next execution in [{}] seconds", delay.seconds());

            try {
                cancellable = threadPool.schedule(this, delay, executorName());
            } catch (EsRejectedExecutionException e) {
                if (e.isExecutorShutdown()) {
                    logger.debug("couldn't schedule new execution of the cleaner, executor is shutting down", e);
                } else {
                    throw e;
                }
            }
        }

        @Override
        public void onFailure(Exception e) {
            logger.error("failed to clean indices", e);
        }

        /**
         * Cancel/stop the cleaning service.
         * <p>
         * This will kill any scheduled {@link #cancellable} from running. It's possible that this will be executed concurrently with the
         * {@link #onAfter() rescheduling code}, at which point it will be stopped during the next execution <em>if</em> the service is
         * stopped.
         */
        public void cancel() {
            if (cancellable != null && cancellable.isCancelled() == false) {
                cancellable.cancel();
            }
        }
    }

    interface ExecutionScheduler {

        /**
         * Calculates the delay in millis between "now" and the next execution.
         *
         * @param now the current time
         * @return the delay in millis
         */
        TimeValue nextExecutionDelay(ZonedDateTime now);
    }

    /**
     * Schedule task so that it will be executed everyday at the next 01:00 AM.
     */
    static class DefaultExecutionScheduler implements ExecutionScheduler {

        @Override
        public TimeValue nextExecutionDelay(ZonedDateTime now) {
            // Runs at 01:00 AM today or the next day if it's too late
            ZonedDateTime next = now.toLocalDate()
                .atStartOfDay(now.getZone())
                .plusHours(1);
            // if it's not after now, then it needs to be the next day!
            if (next.isAfter(now) == false) {
                next = next.plusDays(1);
            }
            return TimeValue.timeValueMillis(Duration.between(now, next).toMillis());
        }
    }
}
