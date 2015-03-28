/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.trigger.schedule;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.watcher.trigger.Trigger;

import java.io.IOException;

/**
 *
 */
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

    public Schedule schedule() {
        return schedule;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field(schedule.type(), schedule).endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ScheduleTrigger trigger = (ScheduleTrigger) o;

        if (!schedule.equals(trigger.schedule)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return schedule.hashCode();
    }

    public static class SourceBuilder implements Trigger.SourceBuilder {

        private final Schedule schedule;

        public SourceBuilder(Schedule schedule) {
            this.schedule = schedule;
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject()
                    .field(schedule.type(), schedule)
                    .endObject();
        }
    }
}
