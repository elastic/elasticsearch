/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.scheduler;

import org.elasticsearch.watcher.scheduler.schedule.*;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class SchedulerModule extends AbstractModule {

    private final Class<? extends Scheduler> schedulerClass;

    private final Map<String, Class<? extends Schedule.Parser>> parsers = new HashMap<>();

    public SchedulerModule() {
        this(InternalScheduler.class);
    }

    protected SchedulerModule(Class<? extends Scheduler> schedulerClass) {
        this.schedulerClass = schedulerClass;
    }

    @Override
    protected void configure() {

        MapBinder<String, Schedule.Parser> mbinder = MapBinder.newMapBinder(binder(), String.class, Schedule.Parser.class);
        bind(IntervalSchedule.Parser.class).asEagerSingleton();
        mbinder.addBinding(IntervalSchedule.TYPE).to(IntervalSchedule.Parser.class);
        bind(CronSchedule.Parser.class).asEagerSingleton();
        mbinder.addBinding(CronSchedule.TYPE).to(CronSchedule.Parser.class);
        bind(HourlySchedule.Parser.class).asEagerSingleton();
        mbinder.addBinding(HourlySchedule.TYPE).to(HourlySchedule.Parser.class);
        bind(DailySchedule.Parser.class).asEagerSingleton();
        mbinder.addBinding(DailySchedule.TYPE).to(DailySchedule.Parser.class);
        bind(WeeklySchedule.Parser.class).asEagerSingleton();
        mbinder.addBinding(WeeklySchedule.TYPE).to(WeeklySchedule.Parser.class);
        bind(MonthlySchedule.Parser.class).asEagerSingleton();
        mbinder.addBinding(MonthlySchedule.TYPE).to(MonthlySchedule.Parser.class);
        bind(YearlySchedule.Parser.class).asEagerSingleton();
        mbinder.addBinding(YearlySchedule.TYPE).to(YearlySchedule.Parser.class);

        for (Map.Entry<String, Class<? extends Schedule.Parser>> entry : parsers.entrySet()) {
            bind(entry.getValue()).asEagerSingleton();
            mbinder.addBinding(entry.getKey()).to(entry.getValue());
        }

        bind(ScheduleRegistry.class).asEagerSingleton();
        bind(schedulerClass).asEagerSingleton();
        bind(Scheduler.class).to(schedulerClass);
    }
}
