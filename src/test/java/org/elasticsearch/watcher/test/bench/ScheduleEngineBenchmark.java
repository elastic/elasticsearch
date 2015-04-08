/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.test.bench;

import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.support.clock.SystemClock;
import org.elasticsearch.watcher.trigger.Trigger;
import org.elasticsearch.watcher.trigger.TriggerEngine;
import org.elasticsearch.watcher.trigger.TriggerEvent;
import org.elasticsearch.watcher.trigger.schedule.*;
import org.elasticsearch.watcher.trigger.schedule.engine.SchedulerScheduleTriggerEngine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class ScheduleEngineBenchmark {

    static final ESLogger logger = Loggers.getLogger(ScheduleEngineBenchmark.class);

    public static void main(String[] args) throws Exception {
        Settings settings = ImmutableSettings.builder()
                .put(SchedulerScheduleTriggerEngine.additionalSettings(ImmutableSettings.EMPTY))
                .put("name", "test")
                .build();
        ThreadPool threadPool = new ThreadPool(settings, null);

        ScheduleRegistry scheduleRegistry = new ScheduleRegistry(Collections.<String, Schedule.Parser>emptyMap());

        SchedulerScheduleTriggerEngine scheduler = new SchedulerScheduleTriggerEngine(ImmutableSettings.EMPTY, SystemClock.INSTANCE, scheduleRegistry, threadPool);

        List<TriggerEngine.Job> jobs = new ArrayList<>(10000);
        for (int i = 0; i < 10000; i++) {
            jobs.add(new SimpleJob("job_" + i, new CronSchedule("0/3 * * * * ?")));
        }
        scheduler.start(jobs);

        scheduler.register(new TriggerEngine.Listener() {
            @Override
            public void triggered(String jobName, TriggerEvent event) {
                ScheduleTriggerEvent e = (ScheduleTriggerEvent) event;
                logger.info("triggered [{}] at fire_time [{}], scheduled time [{}], now [{}]", jobName, event.triggeredTime(), e.scheduledTime(), SystemClock.INSTANCE.now());
            }
        });

        Terminal.DEFAULT.readText("press enter to quit...");
        scheduler.stop();
    }

    static class SimpleJob implements TriggerEngine.Job {

        private final String name;
        private final ScheduleTrigger trigger;

        public SimpleJob(String name, Schedule schedule) {
            this.name = name;
            this.trigger = new ScheduleTrigger(schedule);
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Trigger trigger() {
            return trigger;
        }
    }
}
