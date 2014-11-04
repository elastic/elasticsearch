/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.triggers.AlertTrigger;
import org.elasticsearch.alerts.triggers.TriggerResult;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * An alert action entry is an event of an alert that fired on particular moment in time.
 */
public class AlertActionEntry implements ToXContent{

    private String id;
    private long version;
    private String alertName;
    private boolean triggered;
    private DateTime fireTime;
    private DateTime scheduledTime;
    private AlertTrigger trigger;
    private SearchRequest searchRequest;
    private SearchResponse searchResponse;
    private List<AlertAction> actions;
    private AlertActionState entryState;

    AlertActionEntry() {
    }

    public AlertActionEntry(Alert alert, TriggerResult result, DateTime scheduledTime, DateTime fireTime, AlertActionState state) throws IOException {
        this.id = alert.alertName() + "#" + scheduledTime.toDateTimeISO();
        this.version = 1;
        this.alertName = alert.alertName();
        this.triggered = result.isTriggered();
        this.fireTime = fireTime;
        this.scheduledTime = scheduledTime;
        this.trigger = alert.trigger();
        this.searchRequest = result.getRequest();
        this.searchResponse = result.getResponse();
        this.actions = alert.actions();
        this.entryState = state;
    }

    /**
     * @return The unique id of the alert action entry
     */
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * @return The time the alert was scheduled to be triggered
     */
    public DateTime getScheduledTime() {
        return scheduledTime;
    }

    public void setScheduledTime(DateTime scheduledTime) {
        this.scheduledTime = scheduledTime;
    }

    /**
     * @return The name of the alert that triggered
     */
    public String getAlertName() {
        return alertName;
    }

    public void setAlertName(String alertName) {
        this.alertName = alertName;
    }

    /**
     * @return Whether the search request that run as part of the alert on a fire time matched with the defined trigger.
     */
    public boolean isTriggered() {
        return triggered;
    }

    public void setTriggered(boolean triggered) {
        this.triggered = triggered;
    }

    /**
     * @return The time the alert actually ran.
     */
    public DateTime getFireTime() {
        return fireTime;
    }

    public void setFireTime(DateTime fireTime) {
        this.fireTime = fireTime;
    }

    /**
     * @return The trigger that evaluated the search response
     */
    public AlertTrigger getTrigger() {
        return trigger;
    }

    public void setTrigger(AlertTrigger trigger) {
        this.trigger = trigger;
    }

    /**
     * @return The query that ran at fire time
     */
    public SearchRequest getSearchRequest() {
        return searchRequest;
    }

    public void setSearchRequest(SearchRequest searchRequest) {
        this.searchRequest = searchRequest;
    }

    /**
     * @return The search response that resulted at out the search request that ran.
     */
    public SearchResponse getSearchResponse() {
        return searchResponse;
    }

    public void setSearchResponse(SearchResponse searchResponse) {
        this.searchResponse = searchResponse;
    }

    /**
     * @return The list of actions that ran if the search response matched with the trigger
     */
    public List<AlertAction> getActions() {
        return actions;
    }

    public void setActions(List<AlertAction> actions) {
        this.actions = actions;
    }

    /**
     * @return The current state of the alert event.
     */
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

    @Override
    public XContentBuilder toXContent(XContentBuilder historyEntry, Params params) throws IOException {
        historyEntry.startObject();
        historyEntry.field("alertName", alertName);
        historyEntry.field("triggered", triggered);
        historyEntry.field("fireTime", fireTime.toDateTimeISO());
        historyEntry.field(AlertActionManager.SCHEDULED_FIRE_TIME_FIELD, scheduledTime.toDateTimeISO());
        historyEntry.field("trigger", trigger, params);
        BytesStreamOutput out = new BytesStreamOutput();
        searchRequest.writeTo(out);
        historyEntry.field("request_binary", out.bytes());
        out = new BytesStreamOutput();
        searchResponse.writeTo(out);
        historyEntry.field("response_binary", out.bytes());
        // Serializing it as xcontent allows the search response to be encapsulated in a doc as a json object
        historyEntry.startObject("response");
        searchResponse.toXContent(historyEntry, params);
        historyEntry.endObject();
        historyEntry.startObject("actions");
        for (AlertAction action : actions) {
            historyEntry.field(action.getActionName());
            action.toXContent(historyEntry, params);
        }
        historyEntry.endObject();
        historyEntry.field(AlertActionState.FIELD_NAME, entryState.toString());
        historyEntry.endObject();
        return historyEntry;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AlertActionEntry entry = (AlertActionEntry) o;
        if (!id.equals(entry.id)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }
}
