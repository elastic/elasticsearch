/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.scheduler;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.AlertManager;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.settings.Settings;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.simpl.SimpleJobFactory;

public class AlertScheduler extends AbstractComponent {

    private volatile Scheduler scheduler;
    private final AlertManager alertManager;

    @Inject
    public AlertScheduler(Settings settings, AlertManager alertManager) {
        super(settings);
        this.alertManager = alertManager;
        alertManager.setAlertScheduler(this);
    }

    public void start() {
        try {
            logger.info("Starting scheduler");
            // Can't start a scheduler that has been shutdown, so we need to re-create each time start() is invoked
            SchedulerFactory schFactory = new StdSchedulerFactory();
            scheduler = schFactory.getScheduler();
            scheduler.setJobFactory(new SimpleJobFactory());
            scheduler.start();
        } catch (SchedulerException se){
            logger.error("Failed to start quartz scheduler", se);
        }
    }

    public void stop() {
        try {
            if (!scheduler.isShutdown()) {
                scheduler.clear();
                scheduler.shutdown(false);
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

    public void clearAlerts() {
        try {
            if (scheduler != null) {
                scheduler.clear();
            }
        } catch (SchedulerException se){
            throw new ElasticsearchException("Failed to clear scheduler", se);
        }
    }

    public void add(String alertName, Alert alert) {
        JobDetail job = JobBuilder.newJob(AlertExecutorJob.class).withIdentity(alertName).build();
        job.getJobDataMap().put("manager", this);
        CronTrigger cronTrigger = TriggerBuilder.newTrigger()
                .withSchedule(CronScheduleBuilder.cronSchedule(alert.schedule()))
                .build();
        try {
            logger.trace("Scheduling [{}] with schedule [{}]", alertName, alert.schedule());
            scheduler.scheduleJob(job, cronTrigger);
        } catch (SchedulerException se) {
            logger.error("Failed to schedule job",se);
        }
    }

}
