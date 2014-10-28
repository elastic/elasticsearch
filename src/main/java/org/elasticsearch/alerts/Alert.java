/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

import org.elasticsearch.alerts.actions.AlertAction;
import org.elasticsearch.alerts.triggers.AlertTrigger;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

public class Alert implements ToXContent {

    private String alertName;
    private String queryName;
    private AlertTrigger trigger;
    private TimeValue timePeriod;
    private List<AlertAction> actions;
    private String schedule;
    private DateTime lastRan;
    private DateTime lastActionFire;
    private long version;
    private DateTime running;
    private boolean enabled;
    private boolean simpleQuery;
    private String timestampString = "@timestamp";

    public String timestampString() {
        return timestampString;
    }

    public void timestampString(String timestampString) {
        this.timestampString = timestampString;
    }

    public DateTime lastActionFire() {
        return lastActionFire;
    }

    public void lastActionFire(DateTime lastActionFire) {
        this.lastActionFire = lastActionFire;
    }

    public boolean simpleQuery() {
        return simpleQuery;
    }

    public void simpleQuery(boolean simpleQuery) {
        this.simpleQuery = simpleQuery;
    }

    public boolean enabled() {
        return enabled;
    }

    public void enabled(boolean enabled) {
        this.enabled = enabled;
    }

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

    public void alertName(String alertName) {
        this.alertName = alertName;
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

    public Alert() {
    }

    public Alert(String alertName, String queryName, AlertTrigger trigger,
                 TimeValue timePeriod, List<AlertAction> actions, String schedule, DateTime lastRan,
                 List<String> indices, DateTime running, long version, boolean enabled, boolean simpleQuery){
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
        this.enabled = enabled;
        this.simpleQuery = simpleQuery;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        //Note we deliberately don't serialize the version here
        builder.startObject();
        if (queryName != null) {
            builder.field(AlertsStore.QUERY_NAME_FIELD.getPreferredName(), queryName);
        }
        if (schedule != null) {
            builder.field(AlertsStore.SCHEDULE_FIELD.getPreferredName(), schedule);
        }
        if (timePeriod != null) {
            builder.field(AlertsStore.TIMEPERIOD_FIELD.getPreferredName(), timePeriod);
        }
        if (lastRan != null) {
            builder.field(AlertsStore.LASTRAN_FIELD.getPreferredName(), lastRan);
        }
        if (running != null) {
            builder.field(AlertsStore.CURRENTLY_RUNNING.getPreferredName(), running);
        }
        builder.field(AlertsStore.ENABLED.getPreferredName(), enabled);
        builder.field(AlertsStore.SIMPLE_QUERY.getPreferredName(), simpleQuery);
        if (lastActionFire != null) {
            builder.field(AlertsStore.LAST_ACTION_FIRE.getPreferredName(), lastActionFire);
        }

        if (actions != null && !actions.isEmpty()) {
            builder.startObject(AlertsStore.ACTION_FIELD.getPreferredName());
            for (AlertAction action : actions){
                builder.field(action.getActionName());
                action.toXContent(builder, params);
            }
            builder.endObject();
        }

        if (indices != null && !indices.isEmpty()) {
            builder.startArray(AlertsStore.INDICES.getPreferredName());
            for (String index : indices){
                builder.value(index);
            }
            builder.endArray();
        }

        if (trigger != null) {
            builder.field(AlertsStore.TRIGGER_FIELD.getPreferredName());
            trigger.toXContent(builder, params);
        }

        builder.endObject();
        return builder;
    }

    public boolean isSameAlert(Alert otherAlert) {

        if (this == otherAlert) return true;

        if (enabled != otherAlert.enabled) return false;
        if (simpleQuery != otherAlert.simpleQuery) return false;
        if (actions != null ? !actions.equals(otherAlert.actions) : otherAlert.actions != null) return false;
        if (alertName != null ? !alertName.equals(otherAlert.alertName) : otherAlert.alertName != null) return false;
        if (indices != null ? !indices.equals(otherAlert.indices) : otherAlert.indices != null) return false;
        if (queryName != null ? !queryName.equals(otherAlert.queryName) : otherAlert.queryName != null) return false;
        if (schedule != null ? !schedule.equals(otherAlert.schedule) : otherAlert.schedule != null) return false;
        if (timePeriod != null ? !timePeriod.equals(otherAlert.timePeriod) : otherAlert.timePeriod != null) return false;
        if (timestampString != null ? !timestampString.equals(otherAlert.timestampString) : otherAlert.timestampString != null)
            return false;
        if (trigger != null ? !trigger.equals(otherAlert.trigger) : otherAlert.trigger != null) return false;

        return true;
    }
}
