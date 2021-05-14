/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.scheduler;

public class CronSchedule implements SchedulerEngine.Schedule {
    private final Cron cron;

    public CronSchedule(String cronExpression) {
        this.cron = new Cron(cronExpression);
    }

    @Override
    public long nextScheduledTimeAfter(long startTime, long now) {
        assert now >= startTime;
        long nextTime = Long.MAX_VALUE;
        return Math.min(nextTime, cron.getNextValidTimeAfter(now));
    }
}
