/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport.actions.execute;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.master.MasterNodeOperationRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.watcher.trigger.TriggerEvent;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * An execute watch request to execute a watch by id
 */
public class ExecuteWatchRequest extends MasterNodeOperationRequest<ExecuteWatchRequest> {

    private String id;
    private boolean ignoreCondition = false;
    private boolean ignoreThrottle = false;
    private boolean recordExecution = false;
    private Map<String, Object> alternativeInput = null;
    private BytesReference triggerSource = null;
    private String triggerType = null;
    private Set<String> simulatedActionIds = new HashSet<>();

    ExecuteWatchRequest() {
    }

    /**
     * @param id the id of the watch to execute
     */
    public ExecuteWatchRequest(String id) {
        this.id = id;
    }

    /**
     * @return The id of the watch to be executed
     */
    public String getId() {
        return id;
    }

    /**
     * Sets the id of the watch to be executed
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * @return Should the condition for this execution be ignored
     */
    public boolean isIgnoreCondition() {
        return ignoreCondition;
    }

    /**
     * @param ignoreCondition set if the condition for this execution be ignored
     */
    public void setIgnoreCondition(boolean ignoreCondition) {
        this.ignoreCondition = ignoreCondition;
    }

    /**
     * @return Should the throttle be ignored for this execution
     */
    public boolean isIgnoreThrottle() {
        return ignoreThrottle;
    }

    /**
     * @param ignoreThrottle Sets if the throttle should be ignored for this execution
     */
    public void setIgnoreThrottle(boolean ignoreThrottle) {
        this.ignoreThrottle = ignoreThrottle;
    }

    /**
     * @return Should this execution be recorded in the history index
     */
    public boolean isRecordExecution() {
        return recordExecution;
    }

    /**
     * @param recordExecution Sets if this execution be recorded in the history index
     */
    public void setRecordExecution(boolean recordExecution) {
        this.recordExecution = recordExecution;
    }

    /**
     * @return The alertnative input to use (may be null)
     */
    public Map<String, Object> getAlternativeInput() {
        return alternativeInput;
    }

    /**
     * @param alternativeInput Set's the alernative input
     */
    public void setAlternativeInput(Map<String, Object> alternativeInput) {
        this.alternativeInput = alternativeInput;
    }

    /**
     * @return the trigger to use
     */
    public BytesReference getTriggerSource() {
        return triggerSource;
    }

    /**
     * @param triggerType the type of trigger to use
     * @param triggerSource the trigger source to use
     */
    public void setTriggerEvent(String triggerType, BytesReference triggerSource) {
        this.triggerType = triggerType;
        this.triggerSource = triggerSource;
    }

    /**
     * @param triggerEvent the trigger event to use
     * @throws IOException
     */
    public void setTriggerEvent(TriggerEvent triggerEvent) throws IOException {
        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
        triggerEvent.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);
        setTriggerEvent(triggerEvent.type(), jsonBuilder.bytes());
    }

    /**
     * @return the type of trigger to use
     */
    public String getTriggerType() { return triggerType; }


    /**
     * @return the trigger data to use
     */
    public Set<String> getSimulatedActionIds() {
        return simulatedActionIds;
    }

    /**
     * @param simulatedActionIds a list of action ids to run in simulations for this execution
     */
    public void addSimulatedActions(String ... simulatedActionIds) {
        for (String simulatedActionId : simulatedActionIds) {
            this.simulatedActionIds.add(simulatedActionId);
        }
    }

    /**
     * @return true if all actions should be simulated
     */
    public boolean isSimulateAllActions() {
        return this.simulatedActionIds.contains("_all");
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (id == null){
            validationException = ValidateActions.addValidationError("watch id is missing", validationException);
        }
        if (triggerSource == null || triggerType == null) {
            validationException = ValidateActions.addValidationError("trigger event is missing", validationException);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        id = in.readString();
        ignoreCondition = in.readBoolean();
        ignoreThrottle = in.readBoolean();
        recordExecution = in.readBoolean();
        if (in.readBoolean()){
            alternativeInput = in.readMap();
        }
        triggerSource = in.readBytesReference();
        triggerType = in.readString();
        long simulatedIdCount = in.readLong();
        for(long i = 0; i < simulatedIdCount; ++i) {
            simulatedActionIds.add(in.readString());
        }
    }


    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(id);
        out.writeBoolean(ignoreCondition);
        out.writeBoolean(ignoreThrottle);
        out.writeBoolean(recordExecution);
        out.writeBoolean(alternativeInput != null);
        if (alternativeInput != null) {
            out.writeMap(alternativeInput);
        }
        out.writeBytesReference(triggerSource);
        out.writeString(triggerType);
        out.writeLong(simulatedActionIds.size());
        for (String simulatedId : simulatedActionIds) {
            out.writeString(simulatedId);
        }
    }

    @Override
    public String toString() {
        return "execute[" + id + "]";
    }
}
