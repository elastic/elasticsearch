/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup.job;

import org.elasticsearch.xpack.core.scheduler.Cron;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;

public class CronSchedule implements SchedulerEngine.Schedule {
    private final Cron cron;

    CronSchedule(String cronExpression) {
        this.cron = new Cron(cronExpression);
    }

    @Override
    public long nextScheduledTimeAfter(long startTime, long now) {
        assert now >= startTime;
        long nextTime = Long.MAX_VALUE;
        return Math.min(nextTime, cron.getNextValidTimeAfter(now));
    }
}
