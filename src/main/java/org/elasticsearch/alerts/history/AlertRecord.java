/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.history;

import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.AlertsService;
import org.elasticsearch.alerts.actions.AlertAction;
import org.elasticsearch.alerts.actions.AlertActionState;
import org.elasticsearch.alerts.actions.AlertActions;
import org.elasticsearch.alerts.support.AlertUtils;
import org.elasticsearch.alerts.support.Payload;
import org.elasticsearch.alerts.trigger.Trigger;
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
public class AlertRecord implements ToXContent {

    private String id;
    private String name;
    private DateTime fireTime;
    private DateTime scheduledTime;
    private Trigger trigger;
    private AlertActions actions;
    private AlertActionState state;


    /*Optional*/
    private Trigger.Result triggerResult;
    private Payload payload;
    private String errorMsg;
    private Map<String,Object> metadata;
    private transient long version;
    private transient XContentType contentType;

    AlertRecord() {
    }

    public AlertRecord(Alert alert, DateTime scheduledTime, DateTime fireTime, AlertActionState state) throws IOException {
        this.id = alert.getName() + "#" + scheduledTime.toDateTimeISO();
        this.name = alert.getName();
        this.fireTime = fireTime;
        this.scheduledTime = scheduledTime;
        this.trigger = alert.getTrigger();
        this.actions = alert.getActions();
        this.state = state;
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

    void setId(String id) {
        this.id = id;
    }

    public void execution(Alert alert, AlertsService.AlertRun alertRun) {
        triggerResult = alertRun.evaluation();
        if (triggerResult.triggered()) {
            if (triggerResult.throttled()) {
                if (alert.getStatus() != Alert.Status.NOT_TRIGGERED) {
                    state = AlertActionState.THROTTLED;
                }
            } else if (state != AlertActionState.THROTTLED) {
                state = AlertActionState.ACTION_PERFORMED;
            }
            payload = alertRun.payload();
        } else {
             state = AlertActionState.NO_ACTION_NEEDED;
        }
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
    public String getName() {
        return name;
    }

    void setName(String name) {
        this.name = name;
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
    public Trigger getTrigger() {
        return trigger;
    }

    void setTrigger(Trigger trigger) {
        this.trigger = trigger;
    }

    public Trigger.Result triggerEvaluation() {
        return triggerResult;
    }

    public void triggerEvaluation(Trigger.Result result) {
        this.triggerResult = result;
    }

    /**
     * @return The list of actions that ran if the search response matched with the trigger
     */
    public List<AlertAction> getActions() {
        return actions;
    }

    void setActions(AlertActions actions) {
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

    public Payload payload() {
        return payload;
    }

    public void payload(Payload payload) {
        this.payload = payload;
    }


    @Override
    public XContentBuilder toXContent(XContentBuilder historyEntry, Params params) throws IOException {
        historyEntry.startObject();
        historyEntry.field("alert_name", name);
        historyEntry.field("triggered", triggerResult.triggered());
        historyEntry.field("fire_time", fireTime.toDateTimeISO());
        historyEntry.field(HistoryService.SCHEDULED_FIRE_TIME_FIELD, scheduledTime.toDateTimeISO());
        historyEntry.field("trigger");
        historyEntry.startObject();
        historyEntry.field(trigger.type(), trigger, params);
        historyEntry.endObject();
        historyEntry.field("trigger_response", triggerResult.data());


        if (payload != null) {
            historyEntry.field("payload_request");
            AlertUtils.writeSearchRequest(payload.request(), historyEntry, params);
            historyEntry.field("payload_response", payload.data());
        }

        historyEntry.startObject("actions");
        for (AlertAction action : actions) {
            historyEntry.field(action.getActionName());
            action.toXContent(historyEntry, params);
        }
        historyEntry.endObject();
        historyEntry.field(HistoryService.STATE, state.toString());

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

        AlertRecord entry = (AlertRecord) o;
        if (!id.equals(entry.id)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }
}
