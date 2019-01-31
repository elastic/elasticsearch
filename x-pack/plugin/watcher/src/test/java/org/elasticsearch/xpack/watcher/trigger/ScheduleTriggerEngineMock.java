/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.trigger;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.watch.ClockMock;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleRegistry;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTrigger;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTriggerEngine;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.joda.time.DateTime;

import java.io.IOException;
import java.time.Clock;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A mock scheduler to help with unit testing. Provide {@link ScheduleTriggerEngineMock#trigger} method to manually trigger
 * jobCount.
 */
public class ScheduleTriggerEngineMock extends ScheduleTriggerEngine {
    private static final Logger logger = LogManager.getLogger(ScheduleTriggerEngineMock.class);

    private final ConcurrentMap<String, Watch> watches = new ConcurrentHashMap<>();

    public ScheduleTriggerEngineMock(ScheduleRegistry scheduleRegistry, Clock clock) {
        super(scheduleRegistry, clock);
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
    public void start(Collection<Watch> jobs) {
        jobs.forEach(this::add);
    }

    @Override
    public void stop() {
        watches.clear();
    }

    @Override
    public void add(Watch watch) {
        logger.debug("adding watch [{}]", watch.id());
        watches.put(watch.id(), watch);
    }

    @Override
    public void pauseExecution() {
        watches.clear();
    }

    @Override
    public boolean remove(String jobId) {
        return watches.remove(jobId) != null;
    }

    public boolean trigger(String jobName) {
        return trigger(jobName, 1, null);
    }

    public boolean trigger(String jobName, int times, TimeValue interval) {
        if (watches.containsKey(jobName) == false) {
            return false;
        }

        for (int i = 0; i < times; i++) {
            DateTime now = new DateTime(clock.millis());
            logger.debug("firing watch [{}] at [{}]", jobName, now);
            ScheduleTriggerEvent event = new ScheduleTriggerEvent(jobName, now, now);
            consumers.forEach(consumer -> consumer.accept(Collections.singletonList(event)));
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

        return true;
    }
}
