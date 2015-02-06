/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.scheduler.schedule;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 *
 */
public class CronSchedule implements Schedule {

    public static final String TYPE = "cron";

    private final String cron;

    public CronSchedule(String cron) {
        this.cron = cron;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public String cron() {
        return cron;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.value(cron);
    }

    public static class Parser implements Schedule.Parser<CronSchedule> {

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public CronSchedule parse(XContentParser parser) throws IOException {
            assert parser.currentToken() == XContentParser.Token.VALUE_STRING : "expecting a string value with cron expression";
            String cron = parser.text();
            return new CronSchedule(cron);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CronSchedule that = (CronSchedule) o;

        if (cron != null ? !cron.equals(that.cron) : that.cron != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return cron != null ? cron.hashCode() : 0;
    }
}
