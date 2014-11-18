/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.alerts.actions.AlertAction;
import org.elasticsearch.alerts.triggers.AlertTrigger;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

public class Alert implements ToXContent {

    private String alertName;
    private SearchRequest searchRequest;
    private AlertTrigger trigger;
    private List<AlertAction> actions;
    private String schedule;
    private DateTime lastActionFire;
    private long version;
    private boolean enabled;


    public Alert() {
    }

    public Alert(String alertName, SearchRequest searchRequest, AlertTrigger trigger, List<AlertAction> actions, String schedule, DateTime lastActionFire, long version, boolean enabled) {
        this.alertName = alertName;
        this.searchRequest = searchRequest;
        this.trigger = trigger;
        this.actions = actions;
        this.schedule = schedule;
        this.lastActionFire = lastActionFire;
        this.version = version;
        this.enabled = enabled;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(AlertsStore.SCHEDULE_FIELD.getPreferredName(), schedule);
        builder.field(AlertsStore.ENABLE.getPreferredName(), enabled);
        builder.field(AlertsStore.REQUEST_FIELD.getPreferredName());
        AlertUtils.writeSearchRequest(searchRequest, builder, params);
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
        if (trigger != null) {
            builder.field(AlertsStore.TRIGGER_FIELD.getPreferredName());
            builder.startObject();
            builder.field(trigger.getTriggerName());
            trigger.toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    /**
     * @return The last time this alert ran.
     */
    public DateTime lastActionFire() {
        return lastActionFire;
    }

    public void lastActionFire(DateTime lastActionFire) {
        this.lastActionFire = lastActionFire;
    }

    /**
     * @return Whether this alert has been enabled.
     */
    public boolean enabled() {
        return enabled;
    }

    public void enabled(boolean enabled) {
        this.enabled = enabled;
    }

    /**
     * @return The current version of the alert. (es document version)
     */
    public long version() {
        return version;
    }

    public void version(long version) {
        this.version = version;
    }

    /**
     * @return The unique name of this alert.
     */
    public String alertName() {
        return alertName;
    }

    public void alertName(String alertName) {
        this.alertName = alertName;
    }

    /**
     * @return The search request that runs when the alert runs by the sc
     */
    public SearchRequest getSearchRequest() {
        return searchRequest;
    }

    public void setSearchRequest(SearchRequest searchRequest) {
        this.searchRequest = searchRequest;
    }

    /**
     * @return The trigger that is going to evaluate if the alert is going to execute the alert actions.
     */
    public AlertTrigger trigger() {
        return trigger;
    }

    public void trigger(AlertTrigger trigger) {
        this.trigger = trigger;
    }

    /**
     * @return the actions to be executed if the alert matches the trigger
     */
    public List<AlertAction> actions() {
        return actions;
    }

    public void actions(List<AlertAction> action) {
        this.actions = action;
    }

    /**
     * @return The cron schedule expression that expresses when to run the alert.
     */
    public String schedule() {
        return schedule;
    }

    public void schedule(String schedule) {
        this.schedule = schedule;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Alert alert = (Alert) o;

        if (enabled != alert.enabled) return false;
        if (version != alert.version) return false;
        if (!actions.equals(alert.actions)) return false;
        if (!alertName.equals(alert.alertName)) return false;
        if ( (lastActionFire == null || alert.lastActionFire == null)) {
            if (lastActionFire != alert.lastActionFire) {
                return false;
            }
        } else if (lastActionFire.getMillis() != alert.lastActionFire.getMillis()) {
            return false;
        }
        if (!schedule.equals(alert.schedule)) return false;

        if ( (searchRequest.source() == null || alert.searchRequest.source() == null)) {
            if (searchRequest.source() != alert.searchRequest.source()) {
                return false;
            }
        } else if (!searchRequest.source().equals(alert.searchRequest.source())) {
            return false;
        }

        if (!trigger.equals(alert.trigger)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = alertName.hashCode();
        result = 31 * result + searchRequest.hashCode();
        result = 31 * result + trigger.hashCode();
        result = 31 * result + actions.hashCode();
        result = 31 * result + schedule.hashCode();
        result = 31 * result + (lastActionFire != null ? lastActionFire.hashCode() : 0);
        result = 31 * result + (int) (version ^ (version >>> 32));
        result = 31 * result + (enabled ? 1 : 0);
        return result;
    }

}
