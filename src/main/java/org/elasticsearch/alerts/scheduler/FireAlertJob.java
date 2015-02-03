/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.scheduler;

import org.quartz.*;

public class FireAlertJob implements Job {

    static final String SCHEDULER_KEY = "scheduler";

    public FireAlertJob() {
    }

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        String alertName = jobExecutionContext.getJobDetail().getKey().getName();
        Scheduler scheduler = (Scheduler) jobExecutionContext.getJobDetail().getJobDataMap().get(SCHEDULER_KEY);
        scheduler.notifyListeners(alertName, jobExecutionContext);
    }

    static JobKey jobKey(String alertName) {
        return new JobKey(alertName);
    }

    static JobDetail jobDetail(String alertName, Scheduler scheduler) {
        JobDetail job = JobBuilder.newJob(FireAlertJob.class).withIdentity(alertName).build();
        job.getJobDataMap().put("scheduler", scheduler);
        return job;
    }
}

