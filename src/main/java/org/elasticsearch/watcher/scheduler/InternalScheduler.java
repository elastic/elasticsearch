/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.scheduler;

import org.elasticsearch.watcher.WatcherPlugin;
import org.elasticsearch.watcher.WatcherSettingsException;
import org.elasticsearch.watcher.scheduler.schedule.CronnableSchedule;
import org.elasticsearch.watcher.scheduler.schedule.IntervalSchedule;
import org.elasticsearch.watcher.scheduler.schedule.Schedule;
import org.elasticsearch.watcher.support.clock.Clock;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.DateTimeZone;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.threadpool.ThreadPool;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.simpl.SimpleJobFactory;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.elasticsearch.watcher.scheduler.WatcherQuartzJob.jobDetail;

public class InternalScheduler extends AbstractComponent implements Scheduler {

    // Not happy about it, but otherwise we're stuck with Quartz's SimpleThreadPool
    private volatile static ThreadPool threadPool;

    private final Clock clock;
    private final DateTimeZone defaultTimeZone;

    private volatile org.quartz.Scheduler scheduler;
    private List<Listener> listeners;


    @Inject
    public InternalScheduler(Settings settings, ThreadPool threadPool, Clock clock) {
        super(settings);
        InternalScheduler.threadPool = threadPool;
        this.clock = clock;
        this.listeners = new CopyOnWriteArrayList<>();
        String timeZoneStr = componentSettings.get("time_zone", "UTC");
        try {
            this.defaultTimeZone = DateTimeZone.forID(timeZoneStr);
        } catch (IllegalArgumentException iae) {
            throw new WatcherSettingsException("unrecognized time zone setting [" + timeZoneStr + "]", iae);
        }
    }

    @Override
    public synchronized void start(Collection<? extends Job> jobs) {
        try {
            logger.info("Starting scheduler");
            // Can't start a scheduler that has been shutdown, so we need to re-create each time start() is invoked
            Properties properties = new Properties();
            properties.setProperty("org.quartz.threadPool.class", WatcherQuartzThreadPool.class.getName());
            properties.setProperty(StdSchedulerFactory.PROP_SCHED_SKIP_UPDATE_CHECK, "true");
            properties.setProperty(StdSchedulerFactory.PROP_SCHED_INTERRUPT_JOBS_ON_SHUTDOWN, "true");
            properties.setProperty(StdSchedulerFactory.PROP_SCHED_INTERRUPT_JOBS_ON_SHUTDOWN_WITH_WAIT, "true");
            SchedulerFactory schFactory = new StdSchedulerFactory(properties);
            scheduler = schFactory.getScheduler();
            scheduler.setJobFactory(new SimpleJobFactory());
            Map<JobDetail, Set<? extends Trigger>> quartzJobs = new HashMap<>();
            for (Job job : jobs) {
                quartzJobs.put(jobDetail(job.name(), this), createTrigger(job.schedule(), defaultTimeZone, clock));
            }
            scheduler.scheduleJobs(quartzJobs, false);
            scheduler.start();
        } catch (org.quartz.SchedulerException se) {
            logger.error("Failed to start quartz scheduler", se);
        }
    }

    public synchronized void stop() {
        try {
            org.quartz.Scheduler scheduler = this.scheduler;
            if (scheduler != null) {
                logger.info("Stopping scheduler...");
                scheduler.shutdown(true);
                this.scheduler = null;
                logger.info("Stopped scheduler");
            }
        } catch (org.quartz.SchedulerException se){
            logger.error("Failed to stop quartz scheduler", se);
        }
    }

    @Override
    public void addListener(Listener listener) {
        listeners.add(listener);
    }

    void notifyListeners(String name, JobExecutionContext ctx) {
        DateTime scheduledTime = new DateTime(ctx.getScheduledFireTime());
        DateTime fireTime = new DateTime(ctx.getFireTime());
        for (Listener listener : listeners) {
            listener.fire(name, scheduledTime, fireTime);
        }
    }

    /**
     * Schedules the given job
     */
    public void add(Job job) {
        try {
            logger.trace("scheduling [{}] with schedule [{}]", job.name(), job.schedule());
            scheduler.scheduleJob(jobDetail(job.name(), this), createTrigger(job.schedule(), defaultTimeZone, clock), true);
        } catch (org.quartz.SchedulerException se) {
            logger.error("Failed to schedule job",se);
            throw new SchedulerException("Failed to schedule job", se);
        }
    }

    public boolean remove(String jobName) {
        try {
            return scheduler.deleteJob(new JobKey(jobName));
        } catch (org.quartz.SchedulerException se){
            throw new SchedulerException("Failed to remove [" + jobName + "] from the scheduler", se);
        }
    }

    static Set<Trigger> createTrigger(Schedule schedule, DateTimeZone timeZone, Clock clock) {
        HashSet<Trigger> triggers = new HashSet<>();
        if (schedule instanceof CronnableSchedule) {
            for (String cron : ((CronnableSchedule) schedule).crons()) {
                triggers.add(TriggerBuilder.newTrigger()
                        .withSchedule(CronScheduleBuilder.cronSchedule(cron).inTimeZone(timeZone.toTimeZone()))
                        .startAt(clock.now().toDate())
                        .build());
            }
        } else {
            // must be interval schedule
            IntervalSchedule.Interval interval = ((IntervalSchedule) schedule).interval();
            triggers.add(TriggerBuilder.newTrigger().withSchedule(SimpleScheduleBuilder.simpleSchedule()
                    .withIntervalInSeconds((int) interval.seconds())
                    .repeatForever())
                    .startAt(clock.now().toDate())
                    .build());
        }
        return triggers;
    }


    // This Quartz thread pool will always accept. On this thread we will only index a watch record and add it to the work queue
    public static final class WatcherQuartzThreadPool implements org.quartz.spi.ThreadPool {

        private final EsThreadPoolExecutor executor;

        public WatcherQuartzThreadPool() {
            this.executor = (EsThreadPoolExecutor) threadPool.executor(WatcherPlugin.SCHEDULER_THREAD_POOL_NAME);
        }

        @Override
        public boolean runInThread(Runnable runnable) {
            executor.execute(runnable);
            return true;
        }

        @Override
        public int blockForAvailableThreads() {
            return 1;
        }

        @Override
        public void initialize() throws SchedulerConfigException {

        }

        @Override
        public void shutdown(boolean waitForJobsToComplete) {

        }

        @Override
        public int getPoolSize() {
            return 1;
        }

        @Override
        public void setInstanceId(String schedInstId) {
        }

        @Override
        public void setInstanceName(String schedName) {
        }
    }

}
