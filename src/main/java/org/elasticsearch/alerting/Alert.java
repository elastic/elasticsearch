/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerting;

import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

/**
 * Created by brian on 8/12/14.
 */
public class Alert implements ToXContent{
    private final String alertName;
    private String queryName;
    private AlertTrigger trigger;
    private TimeValue timePeriod;
    private List<AlertAction> actions;
    private String schedule;
    private DateTime lastRan;
    private long version;
    private DateTime running;

    public DateTime running() {
        return running;
    }

    public void running(DateTime running) {
        this.running = running;
    }

    public long version() {
        return version;
    }

    public void version(long version) {
        this.version = version;
    }

    public List<String> indices() {
        return indices;
    }

    public void indices(List<String> indices) {
        this.indices = indices;
    }

    private List<String> indices;

    public String alertName() {
        return alertName;
    }

    public String queryName() {
        return queryName;
    }

    public void queryName(String queryName) {
        this.queryName = queryName;
    }

    public AlertTrigger trigger() {
        return trigger;
    }

    public void trigger(AlertTrigger trigger) {
        this.trigger = trigger;
    }

    public TimeValue timePeriod() {
        return timePeriod;
    }

    public void timePeriod(TimeValue timePeriod) {
        this.timePeriod = timePeriod;
    }

    public List<AlertAction> actions() {
        return actions;
    }

    public void actions(List<AlertAction> action) {
        this.actions = action;
    }

    public String schedule() {
        return schedule;
    }

    public void schedule(String schedule) {
        this.schedule = schedule;
    }

    public DateTime lastRan() {
        return lastRan;
    }

    public void lastRan(DateTime lastRan) {
        this.lastRan = lastRan;
    }

    public Alert(String alertName, String queryName, AlertTrigger trigger,
                 TimeValue timePeriod, List<AlertAction> actions, String schedule, DateTime lastRan,
                 List<String> indices, DateTime running, long version){
        this.alertName = alertName;
        this.queryName = queryName;
        this.trigger = trigger;
        this.timePeriod = timePeriod;
        this.actions = actions;
        this.lastRan = lastRan;
        this.schedule = schedule;
        this.indices = indices;
        this.version = version;
        this.running = running;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        //Note we deliberately don't serialize the version here
        builder.startObject();
        builder.field(AlertManager.QUERY_FIELD.getPreferredName(), queryName);
        builder.field(AlertManager.SCHEDULE_FIELD.getPreferredName(), schedule);
        builder.field(AlertManager.TIMEPERIOD_FIELD.getPreferredName(), timePeriod);
        builder.field(AlertManager.LASTRAN_FIELD.getPreferredName(), lastRan);
        builder.field(AlertManager.CURRENTLY_RUNNING.getPreferredName(), running);
        builder.field(AlertManager.TRIGGER_FIELD.getPreferredName());
        trigger.toXContent(builder, params);
        builder.field(AlertManager.ACTION_FIELD.getPreferredName());

        builder.startObject();
        for (AlertAction action : actions){
            builder.field(action.getActionName());
            action.toXContent(builder, params);
        }
        builder.endObject();

        if (indices != null && !indices.isEmpty()) {
            builder.field(AlertManager.INDICES.getPreferredName());
            builder.startArray();
            for (String index : indices){
                builder.value(index);
            }
            builder.endArray();
        }

        builder.endObject();
        return builder;
    }
}
