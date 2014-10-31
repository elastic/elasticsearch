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

    private final Scheduler scheduler;
    private final AlertManager alertManager;

    @Inject
    public AlertScheduler(Settings settings, AlertManager alertManager) {
        super(settings);
        this.alertManager = alertManager;
        try {
            SchedulerFactory schFactory = new StdSchedulerFactory();
            scheduler = schFactory.getScheduler();
            scheduler.setJobFactory(new SimpleJobFactory());
        } catch (SchedulerException e) {
            throw new ElasticsearchException("Failed to instantiate scheduler", e);
        }
        alertManager.setAlertScheduler(this);
    }

    public void start() {
        try {
            logger.info("Starting scheduler");
            scheduler.start();
        } catch (SchedulerException se){
            logger.error("Failed to start quartz scheduler", se);
        }
    }

    public void stop() {
        try {
            logger.info("Stopping scheduler");
            if (!scheduler.isShutdown()) {
                scheduler.clear();
                scheduler.shutdown(false);
            }
        } catch (SchedulerException se){
            logger.error("Failed to stop quartz scheduler", se);
        }
    }

    public void executeAlert(String alertName, JobExecutionContext jobExecutionContext){
        DateTime scheduledFireTime = new DateTime(jobExecutionContext.getScheduledFireTime());
        DateTime fireTime = new DateTime(jobExecutionContext.getFireTime());
        alertManager.executeAlert(alertName, scheduledFireTime, fireTime);
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
            scheduler.clear();
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
