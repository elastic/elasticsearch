/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.trigger.schedule;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.watcher.trigger.TriggerEngine;
import org.elasticsearch.watcher.trigger.schedule.engine.SchedulerScheduleTriggerEngine;
import org.elasticsearch.watcher.trigger.schedule.engine.TickerScheduleTriggerEngine;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 *
 */
public class ScheduleModule extends AbstractModule {

    private final Map<String, Class<? extends Schedule.Parser>> parsers = new HashMap<>();

    public ScheduleModule() {
        registerScheduleParser(CronSchedule.TYPE, CronSchedule.Parser.class);
        registerScheduleParser(DailySchedule.TYPE, DailySchedule.Parser.class);
        registerScheduleParser(HourlySchedule.TYPE, HourlySchedule.Parser.class);
        registerScheduleParser(IntervalSchedule.TYPE, IntervalSchedule.Parser.class);
        registerScheduleParser(MonthlySchedule.TYPE, MonthlySchedule.Parser.class);
        registerScheduleParser(WeeklySchedule.TYPE, WeeklySchedule.Parser.class);
        registerScheduleParser(YearlySchedule.TYPE, YearlySchedule.Parser.class);
    }

    public static Class<? extends TriggerEngine> triggerEngineType(Settings nodeSettings) {
        Engine engine = Engine.resolve(nodeSettings);
        Loggers.getLogger(ScheduleModule.class, nodeSettings).info("using [{}] schedule trigger engine",
                engine.name().toLowerCase(Locale.ROOT));
        return engine.engineType();
    }

    public void registerScheduleParser(String parserType, Class<? extends Schedule.Parser> parserClass) {
        parsers.put(parserType, parserClass);
    }

    @Override
    protected void configure() {
        MapBinder<String, Schedule.Parser> mbinder = MapBinder.newMapBinder(binder(), String.class, Schedule.Parser.class);
        for (Map.Entry<String, Class<? extends Schedule.Parser>> entry : parsers.entrySet()) {
            bind(entry.getValue()).asEagerSingleton();
            mbinder.addBinding(entry.getKey()).to(entry.getValue());
        }

        bind(ScheduleRegistry.class).asEagerSingleton();
    }

    public enum Engine {

        SCHEDULER() {
            @Override
            protected Class<? extends TriggerEngine> engineType() {
                return SchedulerScheduleTriggerEngine.class;
            }

        },
        TICKER() {
            @Override
            protected Class<? extends TriggerEngine> engineType() {
                return TickerScheduleTriggerEngine.class;
            }

        };

        protected abstract Class<? extends TriggerEngine> engineType();

        public static Engine resolve(Settings settings) {
            String engine = settings.get("xpack.watcher.trigger.schedule.engine", "ticker");
            switch (engine.toLowerCase(Locale.ROOT)) {
                case "ticker"    : return TICKER;
                case "scheduler" : return SCHEDULER;
                default:
                    return TICKER;
            }
        }
    }

}
