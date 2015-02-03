/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.support.AlertUtils;
import org.elasticsearch.alerts.triggers.AlertTrigger;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * An alert history is an event of an alert that fired on particular moment in time.
 */
public class AlertHistory implements ToXContent {

    private String id;
    private String alertName;
    private DateTime fireTime;

    private DateTime scheduledTime;
    private AlertTrigger trigger;
    private List<AlertAction> actions;
    private AlertActionState state;
    private SearchRequest triggerRequest;


    /*Optional*/
    private Map<String, Object> triggerResponse;
    private SearchRequest payloadRequest;
    private Map<String, Object> payloadResponse;
    private boolean triggered;
    private String errorMsg;
    private Map<String,Object> metadata;
    private transient long version;
    private transient XContentType contentType;

    AlertHistory() {
    }

    public AlertHistory(Alert alert, DateTime scheduledTime, DateTime fireTime, AlertActionState state) throws IOException {
        this.id = alert.getAlertName() + "#" + scheduledTime.toDateTimeISO();
        this.alertName = alert.getAlertName();
        this.fireTime = fireTime;
        this.scheduledTime = scheduledTime;
        this.trigger = alert.getTrigger();
        this.actions = alert.getActions();
        this.state = state;
        this.metadata = alert.getMetadata();
        this.version = 1;
        this.contentType = alert.getContentType();
        this.triggerRequest = alert.getTriggerSearchRequest();
    }

    /**
     * @return The unique id of the alert action entry
     */
    public String getId() {
        return id;
    }

    void setId(String id) {
        this.id = id;
    }

    /**
     * @return The time the alert was scheduled to be triggered
     */
    public DateTime getScheduledTime() {
        return scheduledTime;
    }

    void setScheduledTime(DateTime scheduledTime) {
        this.scheduledTime = scheduledTime;
    }

    /**
     * @return The name of the alert that triggered
     */
    public String getAlertName() {
        return alertName;
    }

    void setAlertName(String alertName) {
        this.alertName = alertName;
    }

    /**
     * @return Whether the search request that run as part of the alert on a fire time matched with the defined trigger.
     */
    public boolean isTriggered() {
        return triggered;
    }

    void setTriggered(boolean triggered) {
        this.triggered = triggered;
    }

    /**
     * @return The time the alert actually ran.
     */
    public DateTime getFireTime() {
        return fireTime;
    }

    void setFireTime(DateTime fireTime) {
        this.fireTime = fireTime;
    }

    /**
     * @return The trigger that evaluated the search response
     */
    public AlertTrigger getTrigger() {
        return trigger;
    }

    void setTrigger(AlertTrigger trigger) {
        this.trigger = trigger;
    }

    /**
     * @return The query that ran at fire time
     */
    public SearchRequest getTriggerRequest() {
        return triggerRequest;
    }

    public void setTriggerRequest(SearchRequest triggerRequest) {
        this.triggerRequest = triggerRequest;
    }

    /**
     * @return The search response that resulted at out the search request that ran.
     */
    public Map<String, Object> getTriggerResponse() {
        return triggerResponse;
    }

    public void setTriggerResponse(Map<String, Object> triggerResponse) {
        this.triggerResponse = triggerResponse;
    }

    /**
     * @return The list of actions that ran if the search response matched with the trigger
     */
    public List<AlertAction> getActions() {
        return actions;
    }

    void setActions(List<AlertAction> actions) {
        this.actions = actions;
    }

    /**
     * @return The current state of the alert event.
     */
    public AlertActionState getState() {
        return state;
    }

    void setState(AlertActionState state) {
        this.state = state;
    }

    public long getVersion() {
        return version;
    }

    void setVersion(long version) {
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
     * @return The error if an error occurred otherwise null
     */
    public String getErrorMsg(){
        return this.errorMsg;
    }

    void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    /**
     * @return The metadata that was associated with the alert when this entry was created
     */
    public Map<String, Object> getMetadata() {
        return metadata;
    }

    void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }

    /**
     * @return the payload search response
     */
    public Map<String, Object> getPayloadResponse() {
        return payloadResponse;
    }

    public void setPayloadResponse(Map<String, Object> payloadResponse) {
        this.payloadResponse = payloadResponse;
    }

    /**
     * @return the payload search request
     */
    public SearchRequest getPayloadRequest() {
        return payloadRequest;
    }

    public void setPayloadRequest(SearchRequest payloadRequest) {
        this.payloadRequest = payloadRequest;
    }



    @Override
    public XContentBuilder toXContent(XContentBuilder historyEntry, Params params) throws IOException {
        historyEntry.startObject();
        historyEntry.field("alert_name", alertName);
        historyEntry.field("triggered", triggered);
        historyEntry.field("fire_time", fireTime.toDateTimeISO());
        historyEntry.field(AlertActionService.SCHEDULED_FIRE_TIME_FIELD, scheduledTime.toDateTimeISO());
        historyEntry.field("trigger");
        historyEntry.startObject();
        historyEntry.field(trigger.getTriggerName(), trigger, params);
        historyEntry.endObject();
        historyEntry.field("trigger_request");
        AlertUtils.writeSearchRequest(triggerRequest, historyEntry, params);
        if (triggerResponse != null) {
            historyEntry.field("trigger_response", triggerResponse);
        }

        if (payloadRequest != null) {
            historyEntry.field("payload_request");
            AlertUtils.writeSearchRequest(payloadRequest, historyEntry, params);
        }
        if (payloadResponse != null) {
            historyEntry.field("payload_response", triggerResponse);
        }

        historyEntry.startObject("actions");
        for (AlertAction action : actions) {
            historyEntry.field(action.getActionName());
            action.toXContent(historyEntry, params);
        }
        historyEntry.endObject();
        historyEntry.field(AlertActionService.STATE, state.toString());

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

        AlertHistory entry = (AlertHistory) o;
        if (!id.equals(entry.id)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }
}
