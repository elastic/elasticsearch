/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.trigger.schedule;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.watcher.trigger.Trigger;

import java.io.IOException;

public class ScheduleTrigger implements Trigger {

    public static final String TYPE = "schedule";

    private final Schedule schedule;

    public ScheduleTrigger(Schedule schedule) {
        this.schedule = schedule;
    }

    @Override
    public String type() {
        return TYPE;
    }

    public Schedule getSchedule() {
        return schedule;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ScheduleTrigger trigger = (ScheduleTrigger) o;

        if (schedule.equals(trigger.schedule) == false) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return schedule.hashCode();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field(schedule.type(), schedule, params).endObject();
    }

    public static Builder builder(Schedule schedule) {
        return new Builder(schedule);
    }

    public static class Builder implements Trigger.Builder<ScheduleTrigger> {

        private final Schedule schedule;

        private Builder(Schedule schedule) {
            this.schedule = schedule;
        }

        @Override
        public ScheduleTrigger build() {
            return new ScheduleTrigger(schedule);
        }
    }
}
