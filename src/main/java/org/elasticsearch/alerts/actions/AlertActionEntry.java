/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.AlertUtils;
import org.elasticsearch.alerts.triggers.AlertTrigger;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * An alert action entry is an event of an alert that fired on particular moment in time.
 */
public class AlertActionEntry implements ToXContent {

    private String id;
    private String alertName;
    private DateTime fireTime;

    private DateTime scheduledTime;
    private AlertTrigger trigger;
    private List<AlertAction> actions;
    private AlertActionState state;
    private SearchRequest searchRequest;
    /*Optional*/
    private Map<String, Object> searchResponse;

    private boolean triggered;
    private String errorMsg;
    private Map<String,Object> metadata;

    private transient long version;
    private transient XContentType contentType;

    AlertActionEntry() {
    }

    public AlertActionEntry(Alert alert, DateTime scheduledTime, DateTime fireTime, AlertActionState state) throws IOException {
        this.id = alert.alertName() + "#" + scheduledTime.toDateTimeISO();
        this.alertName = alert.alertName();
        this.fireTime = fireTime;
        this.scheduledTime = scheduledTime;
        this.trigger = alert.trigger();
        this.actions = alert.actions();
        this.state = state;
        this.searchRequest = alert.getSearchRequest();
        this.metadata = alert.getMetadata();

        this.version = 1;
        this.contentType = alert.getContentType();
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
    public Map<String, Object> getSearchResponse() {
        return searchResponse;
    }

    public void setSearchResponse(Map<String, Object> searchResponse) {
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
    public AlertActionState getState() {
        return state;
    }

    public void setState(AlertActionState state) {
        this.state = state;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    /**
     * @return xcontext type of the _source of this action entry.
     */
    public XContentType getContentType() {
        return contentType;
    }

    void setContentType(XContentType contentType) {
        this.contentType = contentType;
    }

    /**
     * @return The error if an error occured otherwise null
     */
    public String getErrorMsg(){
        return this.errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    /**
     * @return The metadata that was associated with the alert when this entry was created
     */
    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder historyEntry, Params params) throws IOException {
        historyEntry.startObject();
        historyEntry.field("alert_name", alertName);
        historyEntry.field("triggered", triggered);
        historyEntry.field("fire_time", fireTime.toDateTimeISO());
        historyEntry.field(AlertActionManager.SCHEDULED_FIRE_TIME_FIELD, scheduledTime.toDateTimeISO());
        historyEntry.field("trigger");
        historyEntry.startObject();
        historyEntry.field(trigger.getTriggerName(), trigger, params);
        historyEntry.endObject();
        historyEntry.field("request");
        AlertUtils.writeSearchRequest(searchRequest, historyEntry, params);
        if (searchResponse != null) {
            historyEntry.field("response", searchResponse);
        }

        historyEntry.startObject("actions");
        for (AlertAction action : actions) {
            historyEntry.field(action.getActionName());
            action.toXContent(historyEntry, params);
        }
        historyEntry.endObject();
        historyEntry.field(AlertActionManager.STATE, state.toString());

        if (errorMsg != null) {
            historyEntry.field("error_msg", errorMsg);
        }

        if (metadata != null) {
            historyEntry.field("meta", metadata);
        }

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
