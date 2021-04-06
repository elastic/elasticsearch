/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.trigger;

import org.elasticsearch.xpack.watcher.trigger.schedule.Schedule;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTrigger;

public final class TriggerBuilders {

    private TriggerBuilders() {
    }

    public static ScheduleTrigger.Builder schedule(Schedule schedule) {
        return ScheduleTrigger.builder(schedule);
    }
}
