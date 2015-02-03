/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.scheduler;

import org.elasticsearch.alerts.scheduler.schedule.CronSchedule;
import org.elasticsearch.alerts.scheduler.schedule.Schedule;
import org.elasticsearch.alerts.scheduler.schedule.ScheduleRegistry;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class SchedulerModule extends AbstractModule {

    private final Map<String, Class<? extends Schedule.Parser>> parsers = new HashMap<>();

    public void registerSchedule(String type, Class<? extends Schedule.Parser> parser) {
        parsers.put(type, parser);
    }

    @Override
    protected void configure() {

        MapBinder<String, Schedule.Parser> mbinder = MapBinder.newMapBinder(binder(), String.class, Schedule.Parser.class);
        bind(CronSchedule.Parser.class).asEagerSingleton();
        mbinder.addBinding(CronSchedule.TYPE).to(CronSchedule.Parser.class);

        for (Map.Entry<String, Class<? extends Schedule.Parser>> entry : parsers.entrySet()) {
            bind(entry.getValue()).asEagerSingleton();
            mbinder.addBinding(entry.getKey()).to(entry.getValue());
        }

        bind(ScheduleRegistry.class).asEagerSingleton();
        bind(Scheduler.class).asEagerSingleton();
    }
}
