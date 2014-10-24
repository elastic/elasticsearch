/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions;

import org.elasticsearch.alerts.triggers.AlertTrigger;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 */
public class AlertActionEntry implements ToXContent{

    private long version;
    private String alertName;
    private boolean triggered;
    private DateTime fireTime;
    private AlertTrigger trigger;
    private String triggeringQuery;
    private long numberOfResults;
    private List<AlertAction> actions;
    private List<String> indices;
    private AlertActionState entryState;
    private DateTime scheduledTime;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    private String id;

    public DateTime getScheduledTime() {
        return scheduledTime;
    }

    public void setScheduledTime(DateTime scheduledTime) {
        this.scheduledTime = scheduledTime;
    }

    public String getAlertName() {
        return alertName;
    }

    public void setAlertName(String alertName) {
        this.alertName = alertName;
    }

    public boolean isTriggered() {
        return triggered;
    }

    public void setTriggered(boolean triggered) {
        this.triggered = triggered;
    }

    public DateTime getFireTime() {
        return fireTime;
    }

    public void setFireTime(DateTime fireTime) {
        this.fireTime = fireTime;
    }

    public AlertTrigger getTrigger() {
        return trigger;
    }

    public void setTrigger(AlertTrigger trigger) {
        this.trigger = trigger;
    }

    public String getTriggeringQuery() {
        return triggeringQuery;
    }

    public void setTriggeringQuery(String triggeringQuery) {
        this.triggeringQuery = triggeringQuery;
    }

    public long getNumberOfResults() {
        return numberOfResults;
    }

    public void setNumberOfResults(long numberOfResults) {
        this.numberOfResults = numberOfResults;
    }

    public List<AlertAction> getActions() {
        return actions;
    }

    public void setActions(List<AlertAction> actions) {
        this.actions = actions;
    }

    public List<String> getIndices() {
        return indices;
    }

    public void setIndices(List<String> indices) {
        this.indices = indices;
    }

    public AlertActionState getEntryState() {
        return entryState;
    }

    public void setEntryState(AlertActionState entryState) {
        this.entryState = entryState;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    protected AlertActionEntry() {
    }

    public AlertActionEntry(String id, long version, String alertName, boolean triggered, DateTime fireTime, DateTime scheduledTime, AlertTrigger trigger,
                            String queryRan, long numberOfResults, List<AlertAction> actions,
                            List<String> indices, AlertActionState state) {
        this.id = id;
        this.version = version;
        this.alertName = alertName;
        this.triggered = triggered;
        this.fireTime = fireTime;
        this.scheduledTime = scheduledTime;
        this.trigger = trigger;
        this.triggeringQuery = queryRan;
        this.numberOfResults = numberOfResults;
        this.actions = actions;
        this.indices = indices;
        this.entryState = state;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder historyEntry, Params params) throws IOException {
        historyEntry.startObject();
        historyEntry.field("alertName", alertName);
        historyEntry.field("triggered", triggered);
        historyEntry.field("fireTime", fireTime.toDateTimeISO());
        historyEntry.field(AlertActionManager.SCHEDULED_FIRE_TIME_FIELD, scheduledTime.toDateTimeISO());

        historyEntry.field("trigger");
        trigger.toXContent(historyEntry, ToXContent.EMPTY_PARAMS);
        
        historyEntry.field("queryRan", triggeringQuery);

        historyEntry.field("numberOfResults", numberOfResults);

        historyEntry.field("actions");
        historyEntry.startObject();
        for (AlertAction action : actions) {
            historyEntry.field(action.getActionName());
            action.toXContent(historyEntry, params);
        }
        historyEntry.endObject();


        if (indices != null) {
            historyEntry.field("indices");
            historyEntry.startArray();
            for (String index : indices) {
                historyEntry.value(index);
            }
            historyEntry.endArray();
        }        

        historyEntry.field(AlertActionState.FIELD_NAME, entryState.toString());

        historyEntry.endObject();
        
        return historyEntry;
    }


    @Override
    public String toString() {
        return "AlertHistoryEntry{" +
                "version=" + version +
                ", alertName='" + alertName + '\'' +
                ", triggered=" + triggered +
                ", fireTime=" + fireTime +
                ", trigger=" + trigger +
                ", triggeringQuery='" + triggeringQuery + '\'' +
                ", numberOfResults=" + numberOfResults +
                ", actions=" + actions +
                ", indices=" + indices +
                ", entryState=" + entryState +
                ", scheduledTime=" + scheduledTime +
                ", id='" + id + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AlertActionEntry that = (AlertActionEntry) o;

        if (numberOfResults != that.numberOfResults) return false;
        if (triggered != that.triggered) return false;
        if (version != that.version) return false;
        if (actions != null ? !actions.equals(that.actions) : that.actions != null) return false;
        if (alertName != null ? !alertName.equals(that.alertName) : that.alertName != null) return false;
        if (entryState != that.entryState) return false;
        if (fireTime != null ? !fireTime.equals(that.fireTime) : that.fireTime != null) return false;
        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (indices != null ? !indices.equals(that.indices) : that.indices != null) return false;
        if (scheduledTime != null ? !scheduledTime.equals(that.scheduledTime) : that.scheduledTime != null)
            return false;
        if (trigger != null ? !trigger.equals(that.trigger) : that.trigger != null) return false;
        if (triggeringQuery != null ? !triggeringQuery.equals(that.triggeringQuery) : that.triggeringQuery != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (version ^ (version >>> 32));
        result = 31 * result + (alertName != null ? alertName.hashCode() : 0);
        result = 31 * result + (triggered ? 1 : 0);
        result = 31 * result + (fireTime != null ? fireTime.hashCode() : 0);
        result = 31 * result + (trigger != null ? trigger.hashCode() : 0);
        result = 31 * result + (triggeringQuery != null ? triggeringQuery.hashCode() : 0);
        result = 31 * result + (int) (numberOfResults ^ (numberOfResults >>> 32));
        result = 31 * result + (actions != null ? actions.hashCode() : 0);
        result = 31 * result + (indices != null ? indices.hashCode() : 0);
        result = 31 * result + (entryState != null ? entryState.hashCode() : 0);
        result = 31 * result + (scheduledTime != null ? scheduledTime.hashCode() : 0);
        result = 31 * result + (id != null ? id.hashCode() : 0);
        return result;
    }

}
