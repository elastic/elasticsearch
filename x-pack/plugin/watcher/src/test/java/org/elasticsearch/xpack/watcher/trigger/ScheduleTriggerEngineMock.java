/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.trigger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.watch.ClockMock;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleRegistry;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTrigger;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTriggerEngine;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTriggerEvent;

import java.io.IOException;
import java.time.Clock;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A mock scheduler to help with unit testing. Provide {@link ScheduleTriggerEngineMock#trigger} method to manually trigger
 * jobCount.
 */
public class ScheduleTriggerEngineMock extends ScheduleTriggerEngine {
    private static final Logger logger = LogManager.getLogger(ScheduleTriggerEngineMock.class);

    private final AtomicReference<Map<String, Watch>> watches = new AtomicReference<>(new ConcurrentHashMap<>());
    private final AtomicBoolean paused = new AtomicBoolean(false);

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
    public synchronized void start(Collection<Watch> jobs) {
        Map<String, Watch> newWatches = new ConcurrentHashMap<>();
        jobs.forEach((watch) -> newWatches.put(watch.id(), watch));
        watches.set(newWatches);
        paused.set(false);
    }

    @Override
    public void stop() {
        watches.set(new ConcurrentHashMap<>());
    }

    @Override
    public synchronized void add(Watch watch) {
        logger.debug("adding watch [{}]", watch.id());
        watches.get().put(watch.id(), watch);
    }

    @Override
    public void pauseExecution() {
        paused.set(true);
    }

    @Override
    public synchronized boolean remove(String jobId) {
        return watches.get().remove(jobId) != null;
    }

    public boolean trigger(String jobName) {
        return trigger(jobName, 1, null);
    }

    public boolean trigger(String jobName, int times, TimeValue interval) {
        if (watches.get().containsKey(jobName) == false) {
            return false;
        }
        if (paused.get()) {
            logger.info("not executing watch [{}] on this scheduler because it is paused", jobName);
            return false;
        }

        for (int i = 0; i < times; i++) {
            ZonedDateTime now = ZonedDateTime.now(clock);
            logger.debug("firing watch [{}] at [{}]", jobName, now);
            ScheduleTriggerEvent event = new ScheduleTriggerEvent(jobName, now, now);
            consumers.forEach(consumer -> consumer.accept(Collections.singletonList(event)));
            if (interval != null) {
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
