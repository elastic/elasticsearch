/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.cleaner;

import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.marvel.MarvelSettings;
import org.elasticsearch.marvel.license.MarvelLicensee;
import org.elasticsearch.threadpool.ThreadPool;
import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;

/**
 * CleanerService takes care of deleting old monitoring indices.
 */
public class CleanerService extends AbstractLifecycleComponent<CleanerService> {


    private final MarvelLicensee licensee;
    private final ThreadPool threadPool;
    private final ExecutionScheduler executionScheduler;
    private final List<Listener> listeners = new CopyOnWriteArrayList<>();

    private volatile IndicesCleaner runnable;
    private volatile TimeValue retention;

    CleanerService(Settings settings, ClusterSettings clusterSettings, MarvelLicensee licensee, ThreadPool threadPool,
                   ExecutionScheduler executionScheduler) {
        super(settings);
        this.licensee = licensee;
        this.threadPool = threadPool;
        this.executionScheduler = executionScheduler;
        clusterSettings.addSettingsUpdateConsumer(MarvelSettings.HISTORY_DURATION, this::setRetention, this::validateRetention);
    }

    @Inject
    public CleanerService(Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool, MarvelLicensee licensee) {
        this(settings, clusterSettings, licensee,threadPool, new DefaultExecutionScheduler());
    }

    @Override
    protected void doStart() {
        logger.debug("starting cleaning service");
        this.runnable = new IndicesCleaner();
        threadPool.schedule(executionScheduler.nextExecutionDelay(new DateTime(ISOChronology.getInstance())), executorName(), runnable);
        logger.debug("cleaning service started");
    }

    @Override
    protected void doStop() {
        logger.debug("stopping cleaning service");
        listeners.clear();
    }

    @Override
    protected void doClose() {
        logger.debug("closing cleaning service");
        if (runnable != null) {
            runnable.cancel();
        }
        logger.debug("cleaning service closed");
    }

    private String executorName() {
        return ThreadPool.Names.GENERIC;
    }

    TimeValue getRetention() {
        return retention;
    }

    public void setRetention(TimeValue retention) {
        validateRetention(retention);
        this.retention = retention;
    }

    public void validateRetention(TimeValue retention) {
        if (retention == null) {
            throw new IllegalArgumentException("history duration setting cannot be null");
        }
        if ((retention.getMillis() <= 0) && (retention.getMillis() != -1)) {
            throw new IllegalArgumentException("invalid history duration setting value");
        }
        if (!licensee.allowUpdateRetention()) {
            throw new IllegalArgumentException("license does not allow the history duration setting to be updated to value ["
                    + retention + "]");
        }
    }

    public void add(Listener listener) {
        listeners.add(listener);
    }

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

    class IndicesCleaner extends AbstractRunnable {

        private volatile ScheduledFuture<?> future;

        @Override
        protected void doRun() throws Exception {
            if (lifecycle.stoppedOrClosed()) {
                logger.trace("cleaning service is stopping, exiting");
                return;
            }
            if (!licensee.cleaningEnabled()) {
                logger.debug("cleaning service is disabled due to invalid license");
                return;
            }

            TimeValue globalRetention = retention;
            if (globalRetention == null) {
                try {
                    globalRetention = MarvelSettings.HISTORY_DURATION.get(settings);
                    validateRetention(globalRetention);
                } catch (IllegalArgumentException e) {
                    globalRetention = MarvelSettings.HISTORY_DURATION.get(Settings.EMPTY);
                }
            }

            DateTime start = new DateTime(ISOChronology.getInstance());
            if (globalRetention.millis() > 0) {
                logger.trace("cleaning up indices with retention [{}]", globalRetention);

                for (Listener listener : listeners) {
                    try {
                        listener.onCleanUpIndices(globalRetention);
                    } catch (Throwable t) {
                        logger.error("listener failed to clean indices", t);
                    }
                }
            }

            if (!lifecycle.stoppedOrClosed()) {
                TimeValue delay = executionScheduler.nextExecutionDelay(start);
                logger.debug("scheduling next execution in [{}] seconds", delay.seconds());
                future = threadPool.schedule(delay, executorName(), this);
            }
        }

        @Override
        public void onFailure(Throwable t) {
            logger.error("failed to clean indices", t);
        }

        public void cancel() {
            FutureUtils.cancel(future);
        }
    }

    interface ExecutionScheduler {

        /**
         * Calculates the delay in millis between "now" and
         * the next execution.
         *
         * @param now the current time
         * @return the delay in millis
         */
        TimeValue nextExecutionDelay(DateTime now);
    }

    /**
     * Schedule task so that it will be executed everyday at 01:00 AM
     */
    static class DefaultExecutionScheduler implements ExecutionScheduler {

        @Override
        public TimeValue nextExecutionDelay(DateTime now) {
            // Runs at 01:00 AM today or the next day if it's too late
            DateTime next = now.withTimeAtStartOfDay().plusHours(1);
            if (next.isBefore(now) || next.equals(now)) {
                next = next.plusDays(1);
            }
            return TimeValue.timeValueMillis(next.getMillis() - now.getMillis());
        }
    }
}
