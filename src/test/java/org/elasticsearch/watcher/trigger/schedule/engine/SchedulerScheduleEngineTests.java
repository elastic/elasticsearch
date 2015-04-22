/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.trigger.schedule.engine;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.watcher.support.clock.SystemClock;
import org.elasticsearch.watcher.trigger.TriggerEngine;
import org.elasticsearch.watcher.trigger.schedule.ScheduleRegistry;

import static org.mockito.Mockito.mock;

/**
 *
 */
@Slow
public class SchedulerScheduleEngineTests extends BaseTriggerEngineTests {

    protected TriggerEngine createEngine() {
        return new SchedulerScheduleTriggerEngine(ImmutableSettings.EMPTY, mock(ScheduleRegistry.class), SystemClock.INSTANCE);
    }

}
