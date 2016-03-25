/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.trigger;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.support.clock.Clock;
import org.elasticsearch.watcher.support.clock.ClockMock;
import org.elasticsearch.watcher.trigger.schedule.ScheduleRegistry;
import org.elasticsearch.watcher.trigger.schedule.ScheduleTrigger;
import org.elasticsearch.watcher.trigger.schedule.ScheduleTriggerEngine;
import org.elasticsearch.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A mock scheduler to help with unit testing. Provide {@link ScheduleTriggerEngineMock#trigger} method to manually trigger
 * jobs.
 */
public class ScheduleTriggerEngineMock extends ScheduleTriggerEngine {

    private final ESLogger logger;
    private final ConcurrentMap<String, Job> jobs = new ConcurrentHashMap<>();

    @Inject
    public ScheduleTriggerEngineMock(Settings settings, ScheduleRegistry scheduleRegistry, Clock clock) {
        super(settings, scheduleRegistry, clock);
        this.logger = Loggers.getLogger(ScheduleTriggerEngineMock.class, settings);

    }

    @Override
    public ScheduleTrigger parseTrigger(String context, XContentParser parser) throws IOException {
        return new ScheduleTrigger(scheduleRegistry.parse(context, parser));
    }

    @Override
    public ScheduleTriggerEvent parseTriggerEvent(TriggerService service, String watchId, String context,
                                                  XContentParser parser) throws IOException {
        return ScheduleTriggerEvent.parse(parser, watchId, context, clock);
    }

    @Override
    public void start(Collection<Job> jobs) {
    }

    @Override
    public void stop() {
    }

    @Override
    public void add(Job job) {
        jobs.put(job.id(), job);
    }

    @Override
    public boolean remove(String jobId) {
        return jobs.remove(jobId) != null;
    }

    public void trigger(String jobName) {
        trigger(jobName, 1, null);
    }

    public void trigger(String jobName, int times) {
        trigger(jobName, times, null);
    }

    public void trigger(String jobName, int times, TimeValue interval) {
        for (int i = 0; i < times; i++) {
            DateTime now = clock.now(DateTimeZone.UTC);
            logger.debug("firing [{}] at [{}]", jobName, now);
            ScheduleTriggerEvent event = new ScheduleTriggerEvent(jobName, now, now);
            for (Listener listener : listeners) {
                listener.triggered(Arrays.<TriggerEvent>asList(event));
            }
            if (interval != null)  {
                if (clock instanceof ClockMock) {
                    ((ClockMock) clock).fastForward(interval);
                } else {
                    try {
                        Thread.sleep(interval.millis());
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
    }
}
