/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.transport.actions.execute;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.watcher.client.WatchSourceBuilder;
import org.elasticsearch.xpack.core.watcher.execution.ActionExecutionMode;
import org.elasticsearch.xpack.core.watcher.support.WatcherUtils;
import org.elasticsearch.xpack.core.watcher.trigger.TriggerEvent;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * A request to execute a watch by id
 */
public class ExecuteWatchRequest extends LegacyActionRequest {

    public static final String INLINE_WATCH_ID = "_inlined_";

    private String id;
    private boolean ignoreCondition = false;
    private boolean recordExecution = false;
    @Nullable
    private Map<String, Object> triggerData = null;
    @Nullable
    private Map<String, Object> alternativeInput = null;
    private Map<String, ActionExecutionMode> actionModes = new HashMap<>();
    private BytesReference watchSource;
    private XContentType xContentType = XContentType.JSON;

    private boolean debug = false;

    public ExecuteWatchRequest() {}

    /**
     * @param id the id of the watch to execute
     */
    public ExecuteWatchRequest(String id) {
        this.id = id;
    }

    public ExecuteWatchRequest(StreamInput in) throws IOException {
        super(in);
        id = in.readOptionalString();
        ignoreCondition = in.readBoolean();
        recordExecution = in.readBoolean();
        alternativeInput = in.readOptional(StreamInput::readGenericMap);
        triggerData = in.readOptional(StreamInput::readGenericMap);
        long actionModesCount = in.readLong();
        actionModes = new HashMap<>();
        for (int i = 0; i < actionModesCount; i++) {
            actionModes.put(in.readString(), ActionExecutionMode.resolve(in.readByte()));
        }
        if (in.readBoolean()) {
            watchSource = in.readBytesReference();
            xContentType = in.readEnum(XContentType.class);
        }
        debug = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(id);
        out.writeBoolean(ignoreCondition);
        out.writeBoolean(recordExecution);
        out.writeOptional(StreamOutput::writeGenericMap, alternativeInput);
        out.writeOptional(StreamOutput::writeGenericMap, triggerData);
        out.writeLong(actionModes.size());
        for (Map.Entry<String, ActionExecutionMode> entry : actionModes.entrySet()) {
            out.writeString(entry.getKey());
            out.writeByte(entry.getValue().id());
        }
        out.writeBoolean(watchSource != null);
        if (watchSource != null) {
            out.writeBytesReference(watchSource);
            XContentHelper.writeTo(out, xContentType);
        }
        out.writeBoolean(debug);
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
     * @param data The data that should be associated with the trigger event.
     */
    public void setTriggerData(Map<String, Object> data) throws IOException {
        this.triggerData = data;
    }

    /**
     * @param event the trigger event to use
     */
    public void setTriggerEvent(TriggerEvent event) throws IOException {
        setTriggerData(event.data());
    }

    /**
     * @return the trigger to use
     */
    public Map<String, Object> getTriggerData() {
        return triggerData;
    }

    /**
     * @return the source of the watch to execute
     */
    public BytesReference getWatchSource() {
        return watchSource;
    }

    public XContentType getXContentType() {
        return xContentType;
    }

    /**
     * @param watchSource instead of using an existing watch use this non persisted watch
     */
    @SuppressWarnings("HiddenField")
    public void setWatchSource(BytesReference watchSource, XContentType xContentType) {
        this.watchSource = watchSource;
        this.xContentType = xContentType;
    }

    /**
     * @param watchSource instead of using an existing watch use this non persisted watch
     */
    public void setWatchSource(WatchSourceBuilder watchSource) {
        this.watchSource = watchSource.buildAsBytes(XContentType.JSON);
        this.xContentType = XContentType.JSON;
    }

    /**
     * @return the execution modes for the actions. These modes determine the nature of the execution
     * of the watch actions while the watch is executing.
     */
    public Map<String, ActionExecutionMode> getActionModes() {
        return actionModes;
    }

    /**
     * Sets the action execution mode for the give action (identified by its id).
     *
     * @param actionId   the action id.
     * @param actionMode the execution mode of the action.
     */
    public void setActionMode(String actionId, ActionExecutionMode actionMode) {
        actionModes.put(actionId, actionMode);
    }

    /**
     * @return whether the watch should execute in debug mode. In debug mode the execution {@code vars}
     * will be returned as part of the watch record.
     */
    public boolean isDebug() {
        return debug;
    }

    /**
     * @param debug indicates whether the watch should execute in debug mode. In debug mode the
     *              returned watch record will hold the execution {@code vars}
     */
    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (id == null && watchSource == null) {
            validationException = ValidateActions.addValidationError(
                "a watch execution request must either have a watch id or an inline " + "watch source, but both are missing",
                validationException
            );
        }
        if (id != null && WatcherUtils.isValidId(id) == false) {
            validationException = ValidateActions.addValidationError("watch id contains whitespace", validationException);
        }
        for (String actionId : actionModes.keySet()) {
            if (actionId == null) {
                validationException = ValidateActions.addValidationError(
                    String.format(Locale.ROOT, "action id may not be null"),
                    validationException
                );
            } else if (WatcherUtils.isValidId(actionId) == false) {
                validationException = ValidateActions.addValidationError(
                    String.format(Locale.ROOT, "action id [%s] contains whitespace", actionId),
                    validationException
                );
            }
        }
        if (watchSource != null && id != null) {
            validationException = ValidateActions.addValidationError(
                "a watch execution request must either have a watch id or an inline " + "watch source but not both",
                validationException
            );
        }
        if (watchSource != null && recordExecution) {
            validationException = ValidateActions.addValidationError(
                "the execution of an inline watch cannot be recorded",
                validationException
            );
        }
        return validationException;
    }

    @Override
    public String toString() {
        return "execute[" + id + "]";
    }
}
