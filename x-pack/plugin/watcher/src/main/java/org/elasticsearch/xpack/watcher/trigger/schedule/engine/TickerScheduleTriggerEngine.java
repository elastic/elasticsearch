/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.trigger.schedule.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.watcher.support.WatcherDateTimeUtils;
import org.elasticsearch.xpack.core.watcher.trigger.TriggerEvent;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.core.watcher.watch.WatchStatus;
import org.elasticsearch.xpack.watcher.trigger.schedule.IntervalSchedule;
import org.elasticsearch.xpack.watcher.trigger.schedule.Schedule;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleRegistry;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTrigger;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTriggerEngine;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTriggerEvent;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.common.settings.Setting.positiveTimeSetting;

public class TickerScheduleTriggerEngine extends ScheduleTriggerEngine {

    public static final Setting<TimeValue> TICKER_INTERVAL_SETTING = positiveTimeSetting(
        "xpack.watcher.trigger.schedule.ticker.tick_interval",
        TimeValue.timeValueMillis(500),
        Property.NodeScope
    );

    private static final Logger logger = LogManager.getLogger(TickerScheduleTriggerEngine.class);

    private final TimeValue tickInterval;
    private final Map<String, ActiveSchedule> schedules = new ConcurrentHashMap<>();
    private final Map<String, ActiveSchedule> recentlyAddedSchedules = new HashMap<>(); // used only inside synchronized blocks
    private final Ticker ticker;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    public TickerScheduleTriggerEngine(Settings settings, ScheduleRegistry scheduleRegistry, Clock clock) {
        super(scheduleRegistry, clock);
        this.tickInterval = TICKER_INTERVAL_SETTING.get(settings);
        this.ticker = new Ticker(DiscoveryNode.canContainData(settings));
    }

    @Override
    public synchronized void start(Collection<Watch> jobs) {
        long startTime = clock.millis();
        logger.info("Starting watcher engine at {}", WatcherDateTimeUtils.dateTimeFormatter.formatMillis(startTime));
        schedules.clear();
        for (Watch job : jobs) {
            if (job.trigger() instanceof ScheduleTrigger trigger) {
                schedules.put(job.id(), createSchedule(job, trigger, startTime));
            }
        }
        schedules.putAll(recentlyAddedSchedules);
        recentlyAddedSchedules.clear();
        isRunning.set(true);
    }

    private ActiveSchedule createSchedule(Watch job, ScheduleTrigger trigger, long currentTimeInMillis) {
        return new ActiveSchedule(
            job.id(),
            trigger.getSchedule(),
            trigger.getSchedule() instanceof IntervalSchedule ? calculateLastStartTime(job) : currentTimeInMillis
        );
    }

    @Override
    public void stop() {
        logger.info("Stopping watcher engine");
        isRunning.set(false);
        schedules.clear();
        ticker.close();
    }

    @Override
    public void pauseExecution() {
        logger.info("Pausing watcher engine");
        isRunning.set(false);
        schedules.clear();
    }

    @Override
    public void add(Watch watch) {
        logger.trace("Adding watch [{}] to engine (engine is running: {})", watch.id(), isRunning.get());
        assert watch.trigger() instanceof ScheduleTrigger;
        ScheduleTrigger trigger = (ScheduleTrigger) watch.trigger();
        ActiveSchedule currentSchedule = schedules.get(watch.id());
        // only update the schedules data structure if the scheduled trigger really has changed, otherwise the time would be reset again
        // resulting in later executions, as the time would only count after a watch has been stored, as this code is triggered by the
        // watcher indexing listener
        // this also means that updating an existing watch would not retrigger the schedule time, if it remains the same schedule
        if (currentSchedule == null || currentSchedule.schedule.equals(trigger.getSchedule()) == false) {
            ActiveSchedule schedule = createSchedule(watch, trigger, clock.millis());
            synchronized (this) {
                if (isRunning.get() == false) {
                    recentlyAddedSchedules.put(watch.id(), schedule);
                } else {
                    schedules.put(watch.id(), schedule);
                }
            }
        }
    }

    /**
     * Attempts to calculate the epoch millis of the last time the watch was checked, If the watch has never been checked, the timestamp of
     * the last state change is used. If the watch has never been checked and has never been in an active state, the current time is used.
     * @param job the watch to calculate the last start time for
     * @return the epoch millis of the last time the watch was checked or now
     */
    private long calculateLastStartTime(Watch job) {
        var lastChecked = Optional.ofNullable(job)
            .map(Watch::status)
            .map(WatchStatus::lastChecked)
            .map(ZonedDateTime::toInstant)
            .map(Instant::toEpochMilli);

        return lastChecked.orElseGet(
            () -> Optional.ofNullable(job)
                .map(Watch::status)
                .map(WatchStatus::state)
                .map(WatchStatus.State::getTimestamp)
                .map(ZonedDateTime::toInstant)
                .map(Instant::toEpochMilli)
                .orElse(clock.millis())
        );
    }

    @Override
    public boolean remove(String jobId) {
        logger.debug("Removing watch [{}] from engine (engine is running: {})", jobId, isRunning.get());
        return schedules.remove(jobId) != null;
    }

    void checkJobs() {
        if (isRunning.get() == false) {
            logger.debug(
                "Watcher not running because the engine is paused. Currently scheduled watches being skipped: {}",
                schedules.size()
            );
            return;
        }
        long triggeredTime = clock.millis();
        List<TriggerEvent> events = new ArrayList<>();
        for (ActiveSchedule schedule : schedules.values()) {
            if (isRunning.get() == false) {
                logger.debug("Watcher paused while running [{}]", schedule.name);
                break;
            }
            long scheduledTime = schedule.check(triggeredTime);
            if (scheduledTime > 0) {
                ZonedDateTime triggeredDateTime = utcDateTimeAtEpochMillis(triggeredTime);
                ZonedDateTime scheduledDateTime = utcDateTimeAtEpochMillis(scheduledTime);
                logger.debug("triggered job [{}] at [{}] (scheduled time was [{}])", schedule.name, triggeredDateTime, scheduledDateTime);
                events.add(new ScheduleTriggerEvent(schedule.name, triggeredDateTime, scheduledDateTime));
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

    private static ZonedDateTime utcDateTimeAtEpochMillis(long triggeredTime) {
        return Instant.ofEpochMilli(triggeredTime).atZone(ZoneOffset.UTC);
    }

    // visible for testing
    Map<String, ActiveSchedule> getSchedules() {
        return Collections.unmodifiableMap(schedules);
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
            logger.debug(
                "Watcher: activating schedule for watch '{}', first run at {}",
                name,
                WatcherDateTimeUtils.dateTimeFormatter.formatMillis(scheduledTime)
            );
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

        // visible for testing
        Schedule getSchedule() {
            return schedule;
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
                logger.trace("checking jobs [{}]", clock.instant().atZone(ZoneOffset.UTC));
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
