/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.test.bench;

import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.watcher.trigger.TriggerEvent;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.condition.InternalAlwaysCondition;
import org.elasticsearch.xpack.watcher.input.none.ExecutableNoneInput;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleRegistry;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTrigger;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTriggerEngine;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.elasticsearch.xpack.watcher.trigger.schedule.engine.TickerScheduleTriggerEngine;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.emptySet;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;

@SuppressForbidden(reason = "benchmark")
public class ScheduleEngineTriggerBenchmark {
    public static void main(String[] args) throws Exception {
        int numWatches = 1000;
        int interval = 2;
        int benchTime = 60000;

        if (args.length % 2 != 0) {
            throw new IllegalArgumentException("Uneven number of arguments");
        }
        for (int i = 0; i < args.length; i += 2) {
            String value = args[i + 1];
            if ("--num_watches".equals(args[i])) {
                numWatches = Integer.valueOf(value);
            } else if ("--bench_time".equals(args[i])) {
                benchTime = Integer.valueOf(value);
            } else if ("--interval".equals(args[i])) {
                interval = Integer.valueOf(value);
            }
        }
        System.out.println("Running benchmark with numWatches=" + numWatches + " benchTime=" + benchTime + " interval=" + interval);

        Settings settings = Settings.builder()
                .put("name", "test")
                .build();
        List<Watch> watches = new ArrayList<>(numWatches);
        for (int i = 0; i < numWatches; i++) {
            watches.add(new Watch("job_" + i, new ScheduleTrigger(interval(interval + "s")), new ExecutableNoneInput(),
                    InternalAlwaysCondition.INSTANCE, null, null, Collections.emptyList(), null, null, 1L, 1L));
        }
        ScheduleRegistry scheduleRegistry = new ScheduleRegistry(emptySet());

        List<Stats> results = new ArrayList<>();
        System.gc();
        System.out.println("=====================================");
        System.out.println("===> Testing scheduler");
        System.out.println("=====================================");
        final AtomicBoolean running = new AtomicBoolean(false);
        final AtomicInteger total = new AtomicInteger();
        final MeanMetric triggerMetric = new MeanMetric();
        final MeanMetric tooEarlyMetric = new MeanMetric();

        final ScheduleTriggerEngine scheduler = new TickerScheduleTriggerEngine(settings, scheduleRegistry, Clock.systemUTC()) {
            @Override
            protected void notifyListeners(List<TriggerEvent> events) {
                if (running.get()) {
                    for (TriggerEvent event : events) {
                        ScheduleTriggerEvent scheduleTriggerEvent = (ScheduleTriggerEvent) event;
                        measure(total, triggerMetric, tooEarlyMetric, event.triggeredTime().toInstant().toEpochMilli(),
                                scheduleTriggerEvent.scheduledTime().toInstant().toEpochMilli());
                    }
                }
            }
        };
        scheduler.start(watches);
        System.out.println("Added [" + numWatches + "] jobs");
        running.set(true);
        Thread.sleep(benchTime);
        running.set(false);
        scheduler.stop();
        System.out.println("done, triggered [" + total.get() + "] times, delayed triggered [" + triggerMetric.count() +
                "] times, avg [" + triggerMetric.mean() + "] ms");
        results.add(new Stats(total.get(), triggerMetric.count(), triggerMetric.mean(), tooEarlyMetric.count(), tooEarlyMetric.mean()));

        System.out.println("       Name     | # triggered | # delayed | avg delay | # too early triggered | avg too early delay");
        System.out.println("--------------- | ----------- | --------- | --------- | --------------------- | ------------------ ");
        for (Stats stats : results) {
            System.out.printf(
                    Locale.ENGLISH,
                    "%11d | %9d | %9d | %21d | %18d\n",
                    stats.numberOfTimesTriggered, stats.numberOfTimesDelayed, stats.avgDelayTime,
                    stats.numberOfEarlyTriggered, stats.avgEarlyDelayTime
            );
        }
    }

    private static void measure(AtomicInteger total, MeanMetric triggerMetric, MeanMetric tooEarlyMetric, long triggeredTime,
                                long scheduledTime) {
        total.incrementAndGet();
        if (Long.compare(triggeredTime, scheduledTime) != 0) {
            long delta = triggeredTime - scheduledTime;
            triggerMetric.inc(delta);
            if (delta < 0) {
                tooEarlyMetric.inc(delta);
            }
        }
    }

    static class Stats {

        final int numberOfTimesTriggered;
        final long numberOfTimesDelayed;
        final long avgDelayTime;
        final long numberOfEarlyTriggered;
        final long avgEarlyDelayTime;

        Stats(int numberOfTimesTriggered, long numberOfTimesDelayed, double avgDelayTime,
              long numberOfEarlyTriggered, double avgEarlyDelayTime) {
            this.numberOfTimesTriggered = numberOfTimesTriggered;
            this.numberOfTimesDelayed = numberOfTimesDelayed;
            this.avgDelayTime = Math.round(avgDelayTime);
            this.numberOfEarlyTriggered = numberOfEarlyTriggered;
            this.avgEarlyDelayTime = Math.round(avgEarlyDelayTime);
        }
    }
}
