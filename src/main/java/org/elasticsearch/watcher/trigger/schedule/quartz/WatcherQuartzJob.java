/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.trigger.schedule.quartz;

import org.quartz.*;

public class WatcherQuartzJob implements Job {

    static final String ENGINE_KEY = "engine";

    public WatcherQuartzJob() {
    }

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        String watchName = jobExecutionContext.getJobDetail().getKey().getName();
        QuartzScheduleTriggerEngine scheduler = (QuartzScheduleTriggerEngine) jobExecutionContext.getJobDetail().getJobDataMap().get(ENGINE_KEY);
        scheduler.notifyListeners(watchName, jobExecutionContext);
    }

    static JobKey jobKey(String watchName) {
        return new JobKey(watchName);
    }

    static JobDetail jobDetail(String watchName, QuartzScheduleTriggerEngine engine) {
        JobDetail job = JobBuilder.newJob(WatcherQuartzJob.class).withIdentity(watchName).build();
        job.getJobDataMap().put(ENGINE_KEY, engine);
        return job;
    }
}

