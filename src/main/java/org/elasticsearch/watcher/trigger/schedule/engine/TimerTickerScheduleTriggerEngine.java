/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.trigger.schedule.engine;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.XRejectedExecutionHandler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.support.ThreadPoolSettingsBuilder;
import org.elasticsearch.watcher.support.clock.Clock;
import org.elasticsearch.watcher.trigger.schedule.*;

import java.util.Collection;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionHandler;

/**
 *
 */
public class TimerTickerScheduleTriggerEngine extends ScheduleTriggerEngine {

    public static final String THREAD_POOL_NAME = "watcher_scheduler";

    public static Settings additionalSettings(Settings nodeSettings) {
        Settings settings = nodeSettings.getAsSettings("threadpool." + THREAD_POOL_NAME);
        if (!settings.names().isEmpty()) {
            // scheduler TP is already configured in the node settings
            // no need for additional settings
            return ImmutableSettings.EMPTY;
        }
        int availableProcessors = EsExecutors.boundedNumberOfProcessors(settings);
        return new ThreadPoolSettingsBuilder.Fixed(THREAD_POOL_NAME)
                .size(availableProcessors)
                .queueSize(1000)
                .build();
    }

    private final Clock clock;

    private volatile Map<String, ActiveSchedule> schedules;
    private Timer timer;
    private TimerTask ticker;
    private EsThreadPoolExecutor executor;

    @Inject
    public TimerTickerScheduleTriggerEngine(Settings settings, Clock clock, ScheduleRegistry scheduleRegistry, ThreadPool threadPool) {
        super(settings, scheduleRegistry);
        this.schedules = new ConcurrentHashMap<>();
        this.clock = clock;
        this.executor = (EsThreadPoolExecutor) threadPool.executor(THREAD_POOL_NAME);
    }

    @Override
    public void start(Collection<Job> jobs) {
        long starTime = clock.millis();
        Map<String, ActiveSchedule> schedules = new ConcurrentHashMap<>();
        for (Job job : jobs) {
            if (job.trigger() instanceof ScheduleTrigger) {
                ScheduleTrigger trigger = (ScheduleTrigger) job.trigger();
                schedules.put(job.name(), new ActiveSchedule(job.name(), trigger.schedule(), starTime));
            }
        }
        this.schedules = schedules;
        this.ticker = new TimerTask() {
            @Override
            public void run() {
                checkJobs();
            }
        };
        this.timer = new Timer("ticker-schedule-trigger-engine", true);
        this.timer.scheduleAtFixedRate(ticker, clock.millis() % 1000 , 10);
    }

    @Override
    public void stop() {
        ticker.cancel();
        timer.cancel();
        executor.getQueue().clear();
    }

    @Override
    public void add(Job job) {
        assert job.trigger() instanceof ScheduleTrigger;
        ScheduleTrigger trigger = (ScheduleTrigger) job.trigger();
        schedules.put(job.name(), new ActiveSchedule(job.name(), trigger.schedule(), clock.millis()));
    }

    @Override
    public boolean remove(String jobName) {
        return schedules.remove(jobName) != null;
    }

    void checkJobs() {
        long triggeredTime = clock.millis();
        for (ActiveSchedule schedule : schedules.values()) {
            long scheduledTime = schedule.check(triggeredTime);
            if (scheduledTime > 0) {
                notifyListeners(schedule.name, triggeredTime, scheduledTime);
            }
        }
    }

    protected void notifyListeners(String name, long triggeredTime, long scheduledTime) {
        final ScheduleTriggerEvent event = new ScheduleTriggerEvent(new DateTime(triggeredTime), new DateTime(scheduledTime));
        for (Listener listener : listeners) {
            try {
                executor.execute(new ListenerRunnable(listener, name, event));
            } catch (EsRejectedExecutionException e) {
                if (logger.isDebugEnabled()) {
                    RejectedExecutionHandler rejectedExecutionHandler = executor.getRejectedExecutionHandler();
                    long rejected = -1;
                    if (rejectedExecutionHandler instanceof XRejectedExecutionHandler) {
                        rejected = ((XRejectedExecutionHandler) rejectedExecutionHandler).rejected();
                    }
                    int queueCapacity = executor.getQueue().size();
                    logger.debug("can't execute trigger on the [" + THREAD_POOL_NAME + "] thread pool, rejected tasks [" + rejected + "] queue capacity [" + queueCapacity +"]");
                }
            }
        }
    }

    static class ActiveSchedule {

        private final String name;
        private final Schedule schedule;
        private final long startTime;

        private volatile long scheduledTime;

        public ActiveSchedule(String name, Schedule schedule, long startTime) {
            this.name = name;
            this.schedule = schedule;
            this.startTime = startTime;
            // we don't want the schedule to trigger on the start time itself, so we compute
            // the next scheduled time by simply computing the schedule time on the startTime + 1
            this.scheduledTime = schedule.nextScheduledTimeAfter(startTime, startTime + 1);
        }

        /**
         * Checks whether the given time is the same or after the scheduled time of this schedule. If so, the scheduled time is
         * returned a new scheduled time is computed and set. Otherwise (the given time is before the scheduled time), {@code -1}
         * is returned.
         */
        public long check(long time) {
            if (time < scheduledTime) {
                return -1;
            }
            long prevScheduledTime = scheduledTime == 0 ? time : scheduledTime;
            scheduledTime = schedule.nextScheduledTimeAfter(startTime, scheduledTime);
            return prevScheduledTime;
        }
    }

    static class ListenerRunnable implements Runnable {

        private final Listener listener;
        private final String jobName;
        private final ScheduleTriggerEvent event;

        public ListenerRunnable(Listener listener, String jobName, ScheduleTriggerEvent event) {
            this.listener = listener;
            this.jobName = jobName;
            this.event = event;
        }

        @Override
        public void run() {
            listener.triggered(jobName, event);
        }
    }

}
