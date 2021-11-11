/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.scheduler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.FutureUtils;

import java.time.Clock;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class SchedulerEngine {

    public static class Job {
        private final String id;
        private final Schedule schedule;

        public Job(String id, Schedule schedule) {
            this.id = id;
            this.schedule = schedule;
        }

        public String getId() {
            return id;
        }

        public Schedule getSchedule() {
            return schedule;
        }
    }

    public static class Event {
        private final String jobName;
        private final long triggeredTime;
        private final long scheduledTime;

        public Event(String jobName, long triggeredTime, long scheduledTime) {
            this.jobName = jobName;
            this.triggeredTime = triggeredTime;
            this.scheduledTime = scheduledTime;
        }

        public String getJobName() {
            return jobName;
        }

        public long getTriggeredTime() {
            return triggeredTime;
        }

        public long getScheduledTime() {
            return scheduledTime;
        }

        @Override
        public String toString() {
            return "Event[jobName=" + jobName + "," + "triggeredTime=" + triggeredTime + "," + "scheduledTime=" + scheduledTime + "]";
        }
    }

    public interface Listener {
        void triggered(Event event);
    }

    public interface Schedule {

        /**
         * Returns the next scheduled time after the given time, according to this schedule. If the given schedule
         * cannot resolve the next scheduled time, then {@code -1} is returned. It really depends on the type of
         * schedule to determine when {@code -1} is returned. Some schedules (e.g. IntervalSchedule) will never return
         * {@code -1} as they can always compute the next scheduled time. {@code Cron} based schedules are good example
         * of schedules that may return {@code -1}, for example, when the schedule only points to times that are all
         * before the given time (in which case, there is no next scheduled time for the given time).
         *
         * Example:
         *
         *      cron    0 0 0 * 1 ? 2013        (only points to days in January 2013)
         *
         *      time    2015-01-01 12:00:00     (this time is in 2015)
         *
         */
        long nextScheduledTimeAfter(long startTime, long now);
    }

    private final Map<String, ActiveSchedule> schedules = ConcurrentCollections.newConcurrentMap();
    private final Clock clock;
    private final ScheduledExecutorService scheduler;
    private final Logger logger;
    private final List<Listener> listeners = new CopyOnWriteArrayList<>();

    public SchedulerEngine(final Settings settings, final Clock clock) {
        this(settings, clock, LogManager.getLogger(SchedulerEngine.class));
    }

    SchedulerEngine(final Settings settings, final Clock clock, final Logger logger) {
        this.clock = Objects.requireNonNull(clock, "clock");
        this.scheduler = Executors.newScheduledThreadPool(
            1,
            EsExecutors.daemonThreadFactory(Objects.requireNonNull(settings, "settings"), "trigger_engine_scheduler")
        );
        this.logger = Objects.requireNonNull(logger, "logger");
    }

    public void register(Listener listener) {
        listeners.add(listener);
    }

    public void unregister(Listener listener) {
        listeners.remove(listener);
    }

    public void start(Collection<Job> jobs) {
        jobs.forEach(this::add);
    }

    public void stop() {
        scheduler.shutdownNow();
        try {
            final boolean terminated = scheduler.awaitTermination(5L, TimeUnit.SECONDS);
            if (terminated == false) {
                logger.warn("scheduler engine was not terminated after waiting 5s");
            }
        } catch (InterruptedException e) {
            logger.warn("interrupted while waiting for scheduler engine termination");
            Thread.currentThread().interrupt();
        }
    }

    public Set<String> scheduledJobIds() {
        return Collections.unmodifiableSet(new HashSet<>(schedules.keySet()));
    }

    public void add(Job job) {
        ActiveSchedule schedule = new ActiveSchedule(job.getId(), job.getSchedule(), clock.millis());
        schedules.compute(schedule.name, (name, previousSchedule) -> {
            if (previousSchedule != null) {
                previousSchedule.cancel();
            }
            logger.debug(() -> new ParameterizedMessage("added job [{}]", job.getId()));
            return schedule;
        });
    }

    public boolean remove(String jobId) {
        ActiveSchedule removedSchedule = schedules.remove(jobId);
        if (removedSchedule != null) {
            logger.debug(() -> new ParameterizedMessage("removed job [{}]", jobId));
            removedSchedule.cancel();
        }
        return removedSchedule != null;
    }

    /**
     * @return The number of currently active/triggered jobs
     */
    public int jobCount() {
        return schedules.size();
    }

    protected void notifyListeners(final String name, final long triggeredTime, final long scheduledTime) {
        final Event event = new Event(name, triggeredTime, scheduledTime);
        for (final Listener listener : listeners) {
            try {
                listener.triggered(event);
            } catch (final Exception e) {
                // do not allow exceptions to escape this method; we should continue to notify listeners and schedule the next run
                logger.warn(new ParameterizedMessage("listener failed while handling triggered event [{}]", name), e);
            }
        }
    }

    // for testing
    ActiveSchedule getSchedule(String jobId) {
        return schedules.get(jobId);
    }

    class ActiveSchedule implements Runnable {

        private final String name;
        private final Schedule schedule;
        private final long startTime;

        private ScheduledFuture<?> future;
        private long scheduledTime = -1;

        ActiveSchedule(String name, Schedule schedule, long startTime) {
            this.name = name;
            this.schedule = schedule;
            this.startTime = startTime;
            this.scheduleNextRun(startTime);
        }

        @Override
        public void run() {
            final long triggeredTime = clock.millis();
            try {
                logger.debug(() -> new ParameterizedMessage("job [{}] triggered with triggeredTime=[{}]", name, triggeredTime));
                notifyListeners(name, triggeredTime, scheduledTime);
            } catch (final Throwable t) {
                /*
                 * Allowing the throwable to escape here will lead to be it being caught in FutureTask#run and set as the outcome of this
                 * task; however, we never inspect the outcomes of these scheduled tasks and so allowing the throwable to escape
                 * unhandled here could lead to us losing fatal errors. Instead, we rely on ExceptionsHelper#maybeDieOnAnotherThread to
                 * appropriately dispatch any error to the uncaught exception handler. We should never see an exception here as these do
                 * not escape from SchedulerEngine#notifyListeners.
                 */
                ExceptionsHelper.maybeDieOnAnotherThread(t);
                throw t;
            }
            scheduleNextRun(triggeredTime);
        }

        private void scheduleNextRun(long triggeredTime) {
            this.scheduledTime = computeNextScheduledTime(triggeredTime);
            if (scheduledTime != -1) {
                long delay = Math.max(0, scheduledTime - clock.millis());
                try {
                    synchronized (this) {
                        if (future == null || future.isCancelled() == false) {
                            logger.debug(
                                () -> new ParameterizedMessage(
                                    "schedule job [{}] with scheduleTime=[{}] and delay=[{}]",
                                    name,
                                    scheduledTime,
                                    delay
                                )
                            );
                            future = scheduler.schedule(this, delay, TimeUnit.MILLISECONDS);
                        }
                    }
                } catch (RejectedExecutionException e) {
                    // ignoring rejections if the scheduler has been shut down already
                    if (scheduler.isShutdown() == false) {
                        throw e;
                    }
                }
            }
        }

        // for testing
        long getScheduledTime() {
            return scheduledTime;
        }

        long computeNextScheduledTime(long triggeredTime) {
            // multiple time sources + multiple cpus + ntp + VMs means you can't trust time ever!
            // scheduling happens far enough in advance in most cases that time can drift and we
            // may execute at some point before the scheduled time. There can also be time differences
            // between the CPU cores and/or the clock used by the threadpool and that used by this class
            // for scheduling. Regardless, we shouldn't reschedule to execute again until after the
            // scheduled time.
            final long scheduleAfterTime;
            if (scheduledTime != -1 && triggeredTime < scheduledTime) {
                scheduleAfterTime = scheduledTime;
            } else {
                scheduleAfterTime = triggeredTime;
            }

            return schedule.nextScheduledTimeAfter(startTime, scheduleAfterTime);
        }

        public synchronized void cancel() {
            FutureUtils.cancel(future);
        }
    }
}
