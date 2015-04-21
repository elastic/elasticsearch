/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.trigger.schedule.engine;

import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.XRejectedExecutionHandler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.support.clock.Clock;
import org.elasticsearch.watcher.trigger.TriggerEvent;
import org.elasticsearch.watcher.trigger.schedule.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionHandler;

/**
 *
 */
public class TickerScheduleTriggerEngine extends ScheduleTriggerEngine {

    private final Clock clock;

    private final TimeValue tickInterval;
    private volatile Map<String, ActiveSchedule> schedules;
    private Ticker ticker;
    private EsThreadPoolExecutor executor;

    @Inject
    public TickerScheduleTriggerEngine(Settings settings, ScheduleRegistry scheduleRegistry, Clock clock, ThreadPool threadPool) {
        super(settings, scheduleRegistry);
        this.tickInterval = settings.getAsTime("watcher.trigger.schedule.ticker.tick_interval", TimeValue.timeValueMillis(500));
        this.schedules = new ConcurrentHashMap<>();
        this.clock = clock;
        this.executor = (EsThreadPoolExecutor) threadPool.executor(ScheduleModule.THREAD_POOL_NAME);
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
        this.ticker = new Ticker();
    }

    @Override
    public void stop() {
        ticker.close();
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
        List<TriggerEvent> events = new ArrayList<>();
        for (ActiveSchedule schedule : schedules.values()) {
            long scheduledTime = schedule.check(triggeredTime);
            if (scheduledTime > 0) {
                logger.trace("triggered job [{}] at [{}] (scheduled time was [{}])", schedule.name, new DateTime(triggeredTime), new DateTime(scheduledTime));
                events.add(new ScheduleTriggerEvent(schedule.name, new DateTime(triggeredTime), new DateTime(scheduledTime)));
                if (events.size() >= 1000) {
                    notifyListeners(ImmutableList.copyOf(events));
                    events.clear();
                }
            }
        }
        if (events.size() > 0) {
            notifyListeners(events);
        }
    }

    protected void notifyListeners(List<TriggerEvent> events) {
        for (Listener listener : listeners) {
            try {
                executor.execute(new ListenerRunnable(listener, events));
            } catch (EsRejectedExecutionException e) {
                if (logger.isDebugEnabled()) {
                    RejectedExecutionHandler rejectedExecutionHandler = executor.getRejectedExecutionHandler();
                    long rejected = -1;
                    if (rejectedExecutionHandler instanceof XRejectedExecutionHandler) {
                        rejected = ((XRejectedExecutionHandler) rejectedExecutionHandler).rejected();
                    }
                    int queueCapacity = executor.getQueue().size();
                    logger.debug("can't execute trigger on the [" + ScheduleModule.THREAD_POOL_NAME + "] thread pool, rejected tasks [" + rejected + "] queue capacity [" + queueCapacity +"]");
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
            this.scheduledTime = schedule.nextScheduledTimeAfter(startTime, startTime);
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
            scheduledTime = schedule.nextScheduledTimeAfter(startTime, time);
            return prevScheduledTime;
        }
    }

    static class ListenerRunnable implements Runnable {

        private final Listener listener;
        private final List<TriggerEvent> events;

        public ListenerRunnable(Listener listener, List<TriggerEvent> events) {
            this.listener = listener;
            this.events = events;
        }

        @Override
        public void run() {
            listener.triggered(events);
        }
    }

    class Ticker extends Thread {

        private volatile boolean active = true;
        private final CountDownLatch closeLatch = new CountDownLatch(1);

        public Ticker() {
            super("ticker-schedule-trigger-engine");
            setDaemon(true);
            start();
        }

        @Override
        public void run() {

            // calibrate with round clock
            while (clock.millis() % 1000 > 15) {
            }
            while (active) {
                logger.trace("checking jobs [{}]", clock.now());
                checkJobs();
                try {
                    sleep(tickInterval.millis());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            closeLatch.countDown();
        }

        public void close() {
            active = false;
            try {
                closeLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

}
