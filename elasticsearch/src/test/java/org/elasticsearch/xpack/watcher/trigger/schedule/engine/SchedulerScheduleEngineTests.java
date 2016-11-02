/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.trigger.schedule.engine;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.watcher.trigger.TriggerEngine;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleRegistry;
import org.joda.time.DateTime;

import static org.mockito.Mockito.mock;

public class SchedulerScheduleEngineTests extends BaseTriggerEngineTestCase {

    protected TriggerEngine createEngine() {
        return new SchedulerScheduleTriggerEngine(Settings.EMPTY, mock(ScheduleRegistry.class), clock);
    }

    // the scheduler configures hard when to schedule, and does not check the clock
    @Override
    protected void advanceClockIfNeeded(DateTime newCurrentDateTime) {
    }

}
