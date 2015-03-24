/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.scheduler;

import org.quartz.*;

public class WatcherQuartzJob implements Job {

    static final String SCHEDULER_KEY = "scheduler";

    public WatcherQuartzJob() {
    }

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        String watchName = jobExecutionContext.getJobDetail().getKey().getName();
        InternalScheduler scheduler = (InternalScheduler) jobExecutionContext.getJobDetail().getJobDataMap().get(SCHEDULER_KEY);
        scheduler.notifyListeners(watchName, jobExecutionContext);
    }

    static JobKey jobKey(String watchName) {
        return new JobKey(watchName);
    }

    static JobDetail jobDetail(String watchName, InternalScheduler scheduler) {
        JobDetail job = JobBuilder.newJob(WatcherQuartzJob.class).withIdentity(watchName).build();
        job.getJobDataMap().put("scheduler", scheduler);
        return job;
    }
}

