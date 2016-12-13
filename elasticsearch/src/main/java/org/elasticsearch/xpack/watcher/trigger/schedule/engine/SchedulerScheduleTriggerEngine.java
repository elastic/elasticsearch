/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.trigger.schedule.engine;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleRegistry;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTrigger;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTriggerEngine;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.joda.time.DateTime;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.joda.time.DateTimeZone.UTC;

public class SchedulerScheduleTriggerEngine extends ScheduleTriggerEngine {

    private final SchedulerEngine schedulerEngine;

    public SchedulerScheduleTriggerEngine(Settings settings, ScheduleRegistry scheduleRegistry, Clock clock) {
        super(settings, scheduleRegistry, clock);
        this.schedulerEngine = new SchedulerEngine(clock);
        this.schedulerEngine.register(event ->
                notifyListeners(event.getJobName(), event.getTriggeredTime(), event.getScheduledTime()));
    }

    @Override
    public void start(Collection<Job> jobs) {
        logger.debug("starting schedule engine...");
        final List<SchedulerEngine.Job> schedulerJobs = new ArrayList<>();
        jobs.stream()
                .filter(job -> job.trigger() instanceof ScheduleTrigger)
                .forEach(job -> {
                    ScheduleTrigger trigger = (ScheduleTrigger) job.trigger();
                    schedulerJobs.add(new SchedulerEngine.Job(job.id(), trigger.getSchedule()));
                });
        schedulerEngine.start(schedulerJobs);
        logger.debug("schedule engine started at [{}]", new DateTime(clock.millis(), UTC));
    }

    @Override
    public void stop() {
        logger.debug("stopping schedule engine...");
        schedulerEngine.stop();
        logger.debug("schedule engine stopped");
    }

    @Override
    public void add(Job job) {
        assert job.trigger() instanceof ScheduleTrigger;
        ScheduleTrigger trigger = (ScheduleTrigger) job.trigger();
        schedulerEngine.add(new SchedulerEngine.Job(job.id(), trigger.getSchedule()));
    }

    @Override
    public boolean remove(String jobId) {
        return schedulerEngine.remove(jobId);
    }

    protected void notifyListeners(String name, long triggeredTime, long scheduledTime) {
        logger.trace("triggered job [{}] at [{}] (scheduled time was [{}])", name, new DateTime(triggeredTime, UTC),
                new DateTime(scheduledTime, UTC));
        final ScheduleTriggerEvent event = new ScheduleTriggerEvent(name, new DateTime(triggeredTime, UTC),
                new DateTime(scheduledTime, UTC));
        for (Listener listener : listeners) {
            listener.triggered(Collections.singletonList(event));
        }
    }
}
