/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.trigger.schedule.engine;

import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
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
import java.util.concurrent.*;

/**
 *
 */
public class SchedulerScheduleTriggerEngine extends ScheduleTriggerEngine {

    private final Clock clock;
    private volatile Schedules schedules;
    private ScheduledExecutorService scheduler;
    private EsThreadPoolExecutor executor;

    @Inject
    public SchedulerScheduleTriggerEngine(Settings settings, ScheduleRegistry scheduleRegistry, Clock clock, ThreadPool threadPool) {
        super(settings, scheduleRegistry);
        this.clock = clock;
        this.executor = (EsThreadPoolExecutor) threadPool.executor(ScheduleModule.THREAD_POOL_NAME);
    }

    @Override
    public void start(Collection<Job> jobs) {
        logger.debug("starting schedule engine...");
        this.scheduler = Executors.newScheduledThreadPool(1, EsExecutors.daemonThreadFactory("trigger_engine_scheduler"));
        long starTime = clock.millis();
        List<ActiveSchedule> schedules = new ArrayList<>();
        for (Job job : jobs) {
            if (job.trigger() instanceof ScheduleTrigger) {
                ScheduleTrigger trigger = (ScheduleTrigger) job.trigger();
                schedules.add(new ActiveSchedule(job.name(), trigger.schedule(), starTime));
            }
        }
        this.schedules = new Schedules(schedules);
        logger.debug("schedule engine started at [{}]", clock.now());
    }

    @Override
    public void stop() {
        logger.debug("stopping schedule engine...");
        scheduler.shutdownNow();
        try {
            scheduler.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        executor.getQueue().clear();
        logger.debug("schedule engine stopped");
    }

    @Override
    public void add(Job job) {
        assert job.trigger() instanceof ScheduleTrigger;
        ScheduleTrigger trigger = (ScheduleTrigger) job.trigger();
        ActiveSchedule schedule = new ActiveSchedule(job.name(), trigger.schedule(), clock.millis());
        schedules = schedules.add(schedule);
    }

    @Override
    public boolean remove(String jobName) {
        Schedules newSchedules = schedules.remove(jobName);
        if (newSchedules == null) {
            return false;
        }
        schedules = newSchedules;
        return true;
    }

    protected void notifyListeners(String name, long triggeredTime, long scheduledTime) {
        logger.trace("triggered job [{}] at [{}] (scheduled time was [{}])", name, new DateTime(triggeredTime), new DateTime(scheduledTime));
        final ScheduleTriggerEvent event = new ScheduleTriggerEvent(name, new DateTime(triggeredTime), new DateTime(scheduledTime));
        for (Listener listener : listeners) {
            try {
                executor.execute(new ListenerRunnable(listener, event));
            } catch (EsRejectedExecutionException e) {
                if (logger.isDebugEnabled()) {
                    RejectedExecutionHandler rejectedExecutionHandler = executor.getRejectedExecutionHandler();
                    long rejected = -1;
                    if (rejectedExecutionHandler instanceof XRejectedExecutionHandler) {
                        rejected = ((XRejectedExecutionHandler) rejectedExecutionHandler).rejected();
                    }
                    int queueCapacity = executor.getQueue().size();
                    logger.debug("can't execute trigger on the [{}] thread pool, rejected tasks [{}] queue capacity [{}]", ScheduleModule.THREAD_POOL_NAME, rejected, queueCapacity);
                }
            }
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
            if (future != null) {
                future.cancel(true);
            }
        }
    }

    static class ListenerRunnable implements Runnable {

        private final Listener listener;
        private final ScheduleTriggerEvent event;

        public ListenerRunnable(Listener listener, ScheduleTriggerEvent event) {
            this.listener = listener;
            this.event = event;
        }

        @Override
        public void run() {
            listener.triggered(ImmutableList.<TriggerEvent>of(event));
        }
    }

    static class Schedules {

        private final ActiveSchedule[] schedules;
        private final ImmutableMap<String, ActiveSchedule> scheduleByName;

        Schedules(Collection<ActiveSchedule> schedules) {
            ImmutableMap.Builder<String, ActiveSchedule> builder = ImmutableMap.builder();
            this.schedules = new ActiveSchedule[schedules.size()];
            int i = 0;
            for (ActiveSchedule schedule : schedules) {
                builder.put(schedule.name, schedule);
                this.schedules[i++] = schedule;
            }
            this.scheduleByName = builder.build();
        }

        public Schedules(ActiveSchedule[] schedules, ImmutableMap<String, ActiveSchedule> scheduleByName) {
            this.schedules = schedules;
            this.scheduleByName = scheduleByName;
        }

        public Schedules add(ActiveSchedule schedule) {
            boolean replacing = scheduleByName.containsKey(schedule.name);
            if (!replacing) {
                ActiveSchedule[] newSchedules = new ActiveSchedule[schedules.length + 1];
                System.arraycopy(schedules, 0, newSchedules, 0, schedules.length);
                newSchedules[schedules.length] = schedule;
                ImmutableMap<String, ActiveSchedule> newScheduleByName = ImmutableMap.<String, ActiveSchedule>builder()
                        .putAll(scheduleByName)
                        .put(schedule.name, schedule)
                        .build();
                return new Schedules(newSchedules, newScheduleByName);
            }
            ActiveSchedule[] newSchedules = new ActiveSchedule[schedules.length];
            ImmutableMap.Builder<String, ActiveSchedule> builder = ImmutableMap.builder();
            for (int i = 0; i < schedules.length; i++) {
                ActiveSchedule sched = schedules[i].name.equals(schedule.name) ? schedule : schedules[i];
                schedules[i] = sched;
                builder.put(sched.name, sched);
            }
            return new Schedules(newSchedules, builder.build());
        }

        public Schedules remove(String name) {
            if (!scheduleByName.containsKey(name)) {
                return null;
            }
            ImmutableMap.Builder<String, ActiveSchedule> builder = ImmutableMap.builder();
            ActiveSchedule[] newSchedules = new ActiveSchedule[schedules.length - 1];
            int i = 0;
            for (ActiveSchedule schedule : schedules) {
                if (!schedule.name.equals(name)) {
                    newSchedules[i++] = schedule;
                    builder.put(schedule.name, schedule);
                } else {
                    schedule.cancel();
                }
            }
            return new Schedules(newSchedules, builder.build());
        }
    }

}
