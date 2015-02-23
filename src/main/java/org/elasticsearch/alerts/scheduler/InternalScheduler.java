/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.scheduler;

import org.elasticsearch.alerts.AlertsPlugin;
import org.elasticsearch.alerts.AlertsSettingsException;
import org.elasticsearch.alerts.scheduler.schedule.CronnableSchedule;
import org.elasticsearch.alerts.scheduler.schedule.IntervalSchedule;
import org.elasticsearch.alerts.scheduler.schedule.Schedule;
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

import static org.elasticsearch.alerts.scheduler.FireAlertQuartzJob.jobDetail;

public class InternalScheduler extends AbstractComponent implements Scheduler {

    // Not happy about it, but otherwise we're stuck with Quartz's SimpleThreadPool
    private volatile static ThreadPool threadPool;

    private volatile org.quartz.Scheduler scheduler;

    private List<Listener> listeners;

    private final DateTimeZone defaultTimeZone;

    @Inject
    public InternalScheduler(Settings settings, ThreadPool threadPool) {
        super(settings);
        this.listeners = new CopyOnWriteArrayList<>();
        InternalScheduler.threadPool = threadPool;
        String timeZoneStr = componentSettings.get("time_zone", "UTC");
        try {
            this.defaultTimeZone = DateTimeZone.forID(timeZoneStr);
        } catch (IllegalArgumentException iae) {
            throw new AlertsSettingsException("unrecognized time zone setting [" + timeZoneStr + "]", iae);
        }
    }

    @Override
    public synchronized void start(Collection<? extends Job> jobs) {
        try {
            logger.info("Starting scheduler");
            // Can't start a scheduler that has been shutdown, so we need to re-create each time start() is invoked
            Properties properties = new Properties();
            properties.setProperty("org.quartz.threadPool.class", AlertQuartzThreadPool.class.getName());
            properties.setProperty(StdSchedulerFactory.PROP_SCHED_SKIP_UPDATE_CHECK, "true");
            properties.setProperty(StdSchedulerFactory.PROP_SCHED_INTERRUPT_JOBS_ON_SHUTDOWN, "true");
            properties.setProperty(StdSchedulerFactory.PROP_SCHED_INTERRUPT_JOBS_ON_SHUTDOWN_WITH_WAIT, "true");
            SchedulerFactory schFactory = new StdSchedulerFactory(properties);
            scheduler = schFactory.getScheduler();
            scheduler.setJobFactory(new SimpleJobFactory());
            Map<JobDetail, Set<? extends Trigger>> quartzJobs = new HashMap<>();
            for (Job alert : jobs) {
                quartzJobs.put(jobDetail(alert.name(), this), createTrigger(alert.schedule(), defaultTimeZone));
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

    void notifyListeners(String alertName, JobExecutionContext ctx) {
        DateTime scheduledTime = new DateTime(ctx.getScheduledFireTime());
        DateTime fireTime = new DateTime(ctx.getFireTime());
        for (Listener listener : listeners) {
            listener.fire(alertName, scheduledTime, fireTime);
        }
    }

    /**
     * Schedules the given alert
     */
    public void add(Job job) {
        try {
            logger.trace("scheduling [{}] with schedule [{}]", job.name(), job.schedule());
            scheduler.scheduleJob(jobDetail(job.name(), this), createTrigger(job.schedule(), defaultTimeZone), true);
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

    static Set<Trigger> createTrigger(Schedule schedule, DateTimeZone timeZone) {
        HashSet<Trigger> triggers = new HashSet<>();
        if (schedule instanceof CronnableSchedule) {
            for (String cron : ((CronnableSchedule) schedule).crons()) {
                triggers.add(TriggerBuilder.newTrigger()
                        .withSchedule(CronScheduleBuilder.cronSchedule(cron).inTimeZone(timeZone.toTimeZone()))
                        .startNow()
                        .build());
            }
        } else {
            // must be interval schedule
            IntervalSchedule.Interval interval = ((IntervalSchedule) schedule).interval();
            triggers.add(TriggerBuilder.newTrigger().withSchedule(SimpleScheduleBuilder.simpleSchedule()
                    .withIntervalInSeconds((int) interval.seconds())
                    .repeatForever())
                    .startNow()
                    .build());
        }
        return triggers;
    }


    // This Quartz thread pool will always accept. On this thread we will only index an alert action and add it to the work queue
    public static final class AlertQuartzThreadPool implements org.quartz.spi.ThreadPool {

        private final EsThreadPoolExecutor executor;

        public AlertQuartzThreadPool() {
            this.executor = (EsThreadPoolExecutor) threadPool.executor(AlertsPlugin.SCHEDULER_THREAD_POOL_NAME);
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
