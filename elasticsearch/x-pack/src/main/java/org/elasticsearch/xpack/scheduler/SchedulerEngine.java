/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.scheduler;

import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.xpack.watcher.support.clock.Clock;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableMap;

/**
 *
 */
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

    private volatile Schedules schedules;
    private ScheduledExecutorService scheduler;
    private final Clock clock;
    private List<Listener> listeners = new CopyOnWriteArrayList<>();

    public SchedulerEngine(Clock clock) {
        this.clock = clock;
    }

    public void register(Listener listener) {
        listeners.add(listener);
    }

    public void start(Collection<Job> jobs) {
        this.scheduler = Executors.newScheduledThreadPool(1, EsExecutors.daemonThreadFactory("trigger_engine_scheduler"));
        long starTime = clock.millis();
        List<ActiveSchedule> schedules = jobs.stream()
                .map(job -> new ActiveSchedule(job.getId(), job.getSchedule(), starTime))
                .collect(Collectors.toList());
        this.schedules = new Schedules(schedules);
    }

    public void stop() {
        scheduler.shutdownNow();
        try {
            scheduler.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void add(Job job) {
        ActiveSchedule schedule = new ActiveSchedule(job.getId(), job.getSchedule(), clock.millis());
        schedules = schedules.add(schedule);
    }

    public boolean remove(String jobId) {
        Schedules newSchedules = schedules.remove(jobId);
        if (newSchedules == null) {
            return false;
        }
        schedules = newSchedules;
        return true;
    }

    protected void notifyListeners(String name, long triggeredTime, long scheduledTime) {
        final Event event = new Event(name, triggeredTime, scheduledTime);
        for (Listener listener : listeners) {
            listener.triggered(event);
        }
    }

    class ActiveSchedule implements Runnable {

        private final String name;
        private final Schedule schedule;
        private final long startTime;

        private volatile ScheduledFuture<?> future;
        private volatile long scheduledTime;

        public ActiveSchedule(String name, Schedule schedule, long startTime) {
            this.name = name;
            this.schedule = schedule;
            this.startTime = startTime;
            this.scheduledTime = schedule.nextScheduledTimeAfter(startTime, startTime);
            if (scheduledTime != -1) {
                long delay = Math.max(0, scheduledTime - clock.millis());
                future = scheduler.schedule(this, delay, TimeUnit.MILLISECONDS);
            }
        }

        @Override
        public void run() {
            long triggeredTime = clock.millis();
            notifyListeners(name, triggeredTime, scheduledTime);
            scheduledTime = schedule.nextScheduledTimeAfter(startTime, triggeredTime);
            if (scheduledTime != -1) {
                long delay = Math.max(0, scheduledTime - triggeredTime);
                future = scheduler.schedule(this, delay, TimeUnit.MILLISECONDS);
            }
        }

        public void cancel() {
            FutureUtils.cancel(future);
        }
    }

    static class Schedules {

        private final Map<String, ActiveSchedule> scheduleByName;

        Schedules(Collection<ActiveSchedule> schedules) {
            Map<String, ActiveSchedule> builder = new HashMap<>();
            for (ActiveSchedule schedule : schedules) {
                builder.put(schedule.name, schedule);
            }
            this.scheduleByName = unmodifiableMap(builder);
        }

        public Schedules(Map<String, ActiveSchedule> scheduleByName) {
            this.scheduleByName = scheduleByName;
        }

        public Schedules add(ActiveSchedule schedule) {
            boolean replacing = scheduleByName.containsKey(schedule.name);
            if (!replacing) {
                Map<String, ActiveSchedule> newScheduleByName = new HashMap<>(scheduleByName);
                newScheduleByName.put(schedule.name, schedule);
                return new Schedules(unmodifiableMap(newScheduleByName));
            }
            Map<String, ActiveSchedule> builder = new HashMap<>(scheduleByName.size());
            for (Map.Entry<String, ActiveSchedule> scheduleEntry : scheduleByName.entrySet()) {
                final String existingScheduleName = scheduleEntry.getKey();
                final ActiveSchedule existingSchedule = scheduleEntry.getValue();
                if (existingScheduleName.equals(schedule.name)) {
                    existingSchedule.cancel();
                    builder.put(schedule.name, schedule);
                } else {
                    builder.put(existingScheduleName, existingSchedule);
                }
            }
            return new Schedules(unmodifiableMap(builder));
        }

        public Schedules remove(String name) {
            if (!scheduleByName.containsKey(name)) {
                return null;
            }
            Map<String, ActiveSchedule> builder = new HashMap<>(scheduleByName.size() - 1);
            for (Map.Entry<String, ActiveSchedule> scheduleEntry : scheduleByName.entrySet()) {
                final String existingScheduleName = scheduleEntry.getKey();
                final ActiveSchedule existingSchedule = scheduleEntry.getValue();
                if (existingScheduleName.equals(name)) {
                    existingSchedule.cancel();
                } else {
                    builder.put(existingScheduleName, existingSchedule);
                }
            }
            return new Schedules(unmodifiableMap(builder));
        }
    }
}
