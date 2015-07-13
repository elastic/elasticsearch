/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.trigger;

import org.elasticsearch.watcher.trigger.schedule.Schedule;
import org.elasticsearch.watcher.trigger.schedule.ScheduleTrigger;

/**
 *
 */
public final class TriggerBuilders {

    private TriggerBuilders() {
    }

    public static ScheduleTrigger.Builder schedule(Schedule schedule) {
        return ScheduleTrigger.builder(schedule);
    }
}
