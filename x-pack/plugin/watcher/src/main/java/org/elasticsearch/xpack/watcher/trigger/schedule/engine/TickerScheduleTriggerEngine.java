/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.trigger.schedule.engine;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.xpack.core.watcher.trigger.TriggerEvent;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.trigger.schedule.Schedule;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleRegistry;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTrigger;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTriggerEngine;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.joda.time.DateTime;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.common.settings.Setting.positiveTimeSetting;
import static org.joda.time.DateTimeZone.UTC;

public class TickerScheduleTriggerEngine extends ScheduleTriggerEngine {

    public static final Setting<TimeValue> TICKER_INTERVAL_SETTING =
        positiveTimeSetting("xpack.watcher.trigger.schedule.ticker.tick_interval", TimeValue.timeValueMillis(500), Property.NodeScope);

    private final TimeValue tickInterval;
    private volatile Map<String, ActiveSchedule> schedules;
    private Ticker ticker;

    public TickerScheduleTriggerEngine(Settings settings, ScheduleRegistry scheduleRegistry, Clock clock) {
        super(settings, scheduleRegistry, clock);
        this.tickInterval = TICKER_INTERVAL_SETTING.get(settings);
        this.schedules = new ConcurrentHashMap<>();
        this.ticker = new Ticker(Node.NODE_DATA_SETTING.get(settings));
    }

    @Override
    public synchronized void start(Collection<Watch> jobs) {
        long startTime = clock.millis();
        Map<String, ActiveSchedule> schedules = new ConcurrentHashMap<>();
        for (Watch job : jobs) {
            if (job.trigger() instanceof ScheduleTrigger) {
                ScheduleTrigger trigger = (ScheduleTrigger) job.trigger();
                schedules.put(job.id(), new ActiveSchedule(job.id(), trigger.getSchedule(), startTime));
            }
        }
        this.schedules = schedules;
    }

    @Override
    public void stop() {
        schedules.clear();
        ticker.close();
    }

    @Override
    public synchronized void pauseExecution() {
        schedules.clear();
    }

    @Override
    public void add(Watch watch) {
        assert watch.trigger() instanceof ScheduleTrigger;
        ScheduleTrigger trigger = (ScheduleTrigger) watch.trigger();
        schedules.put(watch.id(), new ActiveSchedule(watch.id(), trigger.getSchedule(), clock.millis()));
    }

    @Override
    public boolean remove(String jobId) {
        return schedules.remove(jobId) != null;
    }

    void checkJobs() {
        long triggeredTime = clock.millis();
        List<TriggerEvent> events = new ArrayList<>();
        for (ActiveSchedule schedule : schedules.values()) {
            long scheduledTime = schedule.check(triggeredTime);
            if (scheduledTime > 0) {
                logger.debug("triggered job [{}] at [{}] (scheduled time was [{}])", schedule.name,
                    new DateTime(triggeredTime, UTC), new DateTime(scheduledTime, UTC));
                events.add(new ScheduleTriggerEvent(schedule.name, new DateTime(triggeredTime, UTC),
                    new DateTime(scheduledTime, UTC)));
                if (events.size() >= 1000) {
                    notifyListeners(events);
                    events.clear();
                }
            }
        }
        if (events.isEmpty() == false) {
            notifyListeners(events);
        }
    }

    protected void notifyListeners(List<TriggerEvent> events) {
        consumers.forEach(consumer -> consumer.accept(events));
    }

    static class ActiveSchedule {

        private final String name;
        private final Schedule schedule;
        private final long startTime;

        private volatile long scheduledTime;

        ActiveSchedule(String name, Schedule schedule, long startTime) {
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

    class Ticker extends Thread {

        private volatile boolean active = true;
        private final CountDownLatch closeLatch = new CountDownLatch(1);
        private boolean isDataNode;

        Ticker(boolean isDataNode) {
            super("ticker-schedule-trigger-engine");
            this.isDataNode = isDataNode;
            setDaemon(true);
            if (isDataNode) {
                start();
            }
        }

        @Override
        public void run() {
            while (active) {
                logger.trace("checking jobs [{}]", new DateTime(clock.millis(), UTC));
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
            if (isDataNode) {
                logger.trace("stopping ticker thread");
                active = false;
                try {
                    closeLatch.await();
                } catch (InterruptedException e) {
                    logger.warn("caught an interrupted exception when waiting while closing ticker thread", e);
                    Thread.currentThread().interrupt();
                }
                logger.trace("ticker thread stopped");
            }
        }
    }
}
