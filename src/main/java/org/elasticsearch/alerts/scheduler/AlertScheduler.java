/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.scheduler;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.AlertManager;
import org.elasticsearch.alerts.plugin.AlertsPlugin;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.threadpool.ThreadPool;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.simpl.SimpleJobFactory;

import java.util.*;

public class AlertScheduler extends AbstractComponent {

    // Not happy about it, but otherwise we're stuck with Quartz's SimpleThreadPool
    private volatile static ThreadPool threadPool;

    private AlertManager alertManager;
    private volatile Scheduler scheduler;

    @Inject
    public AlertScheduler(Settings settings, ThreadPool threadPool) {
        super(settings);
        AlertScheduler.threadPool = threadPool;
    }

    public void setAlertManager(AlertManager alertManager){
        this.alertManager = alertManager;
    }

    /**
     * Starts the scheduler and schedules the specified alerts before returning.
     *
     * Both the start and stop are synchronized to avoid that scheduler gets stopped while previously stored alerts
     * are being loaded.
     */
    public synchronized void start(Map<String, Alert> alerts) {
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
            Map<JobDetail, Set<? extends Trigger>> jobs = new HashMap<>();
            for (Map.Entry<String, Alert> entry : alerts.entrySet()) {
                jobs.put(createJobDetail(entry.getKey()), createTrigger(entry.getValue().schedule()));
            }
            scheduler.scheduleJobs(jobs, false);
            scheduler.start();
        } catch (SchedulerException se){
            logger.error("Failed to start quartz scheduler", se);
        }
    }

    /**
     * Stops the scheduler.
     */
    public synchronized void stop() {
        try {
            Scheduler scheduler = this.scheduler;
            if (scheduler != null) {
                logger.info("Stopping scheduler...");
                scheduler.shutdown(true);
                this.scheduler = null;
                logger.info("Stopped scheduler");
            }
        } catch (SchedulerException se){
            logger.error("Failed to stop quartz scheduler", se);
        }
    }

    public void executeAlert(String alertName, JobExecutionContext jobExecutionContext){
        DateTime scheduledFireTime = new DateTime(jobExecutionContext.getScheduledFireTime());
        DateTime fireTime = new DateTime(jobExecutionContext.getFireTime());
        alertManager.scheduleAlert(alertName, scheduledFireTime, fireTime);
    }

    public boolean remove(String alertName) {
        try {
            return scheduler.deleteJob(new JobKey(alertName));
        } catch (SchedulerException se){
            throw new ElasticsearchException("Failed to remove [" + alertName + "] from the scheduler", se);
        }
    }

    /**
     * Schedules the alert with the specified name to be fired according to the specified cron expression.
     */
    public void schedule(String alertName, String cronExpression) {
        try {
            logger.trace("Scheduling [{}] with schedule [{}]", alertName, cronExpression);
            scheduler.scheduleJob(createJobDetail(alertName), createTrigger(cronExpression), true);
        } catch (SchedulerException se) {
            logger.error("Failed to schedule job",se);
            throw new ElasticsearchException("Failed to schedule job", se);
        }
    }

    private Set<CronTrigger> createTrigger(String cronExpression) {
        return new HashSet<>(Arrays.asList(
                TriggerBuilder.newTrigger().withSchedule(CronScheduleBuilder.cronSchedule(cronExpression)).build()
        ));
    }

    private JobDetail createJobDetail(String alertName) {
        JobDetail job = JobBuilder.newJob(AlertExecutorJob.class).withIdentity(alertName).build();
        job.getJobDataMap().put("manager", this);
        return job;
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
