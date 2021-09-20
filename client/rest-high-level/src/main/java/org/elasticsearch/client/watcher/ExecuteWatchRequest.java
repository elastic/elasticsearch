/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.watcher;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * An execute watch request to execute a watch by id or inline
 */
public class ExecuteWatchRequest implements Validatable, ToXContentObject {

    public enum ActionExecutionMode {
        SIMULATE, FORCE_SIMULATE, EXECUTE, FORCE_EXECUTE, SKIP
    }

    private final String id;
    private final BytesReference watchContent;

    private boolean ignoreCondition = false;
    private boolean recordExecution = false;
    private boolean debug = false;

    @Nullable
    private BytesReference triggerData = null;

    @Nullable
    private BytesReference alternativeInput = null;

    private Map<String, ActionExecutionMode> actionModes = new HashMap<>();

    /**
     * Execute an existing watch on the cluster
     *
     * @param id the id of the watch to execute
     */
    public static ExecuteWatchRequest byId(String id) {
        return new ExecuteWatchRequest(Objects.requireNonNull(id, "Watch id cannot be null"), null);
    }

    /**
     * Execute an inline watch
     * @param watchContent the JSON definition of the watch
     */
    public static ExecuteWatchRequest inline(String watchContent) {
        return new ExecuteWatchRequest(null, Objects.requireNonNull(watchContent, "Watch content cannot be null"));
    }

    private ExecuteWatchRequest(String id, String watchContent) {
        this.id = id;
        this.watchContent = watchContent == null ? null : new BytesArray(watchContent);
    }

    public String getId() {
        return this.id;
    }

    /**
     * @param ignoreCondition set if the condition for this execution be ignored
     */
    public void setIgnoreCondition(boolean ignoreCondition) {
        this.ignoreCondition = ignoreCondition;
    }

    public boolean ignoreCondition() {
        return ignoreCondition;
    }

    /**
     * @param recordExecution Sets if this execution be recorded in the history index
     */
    public void setRecordExecution(boolean recordExecution) {
        if (watchContent != null && recordExecution) {
            throw new IllegalArgumentException("The execution of an inline watch cannot be recorded");
        }
        this.recordExecution = recordExecution;
    }

    public boolean recordExecution() {
        return recordExecution;
    }

    /**
     * @param alternativeInput Sets the alternative input
     */
    public void setAlternativeInput(String alternativeInput) {
        this.alternativeInput = new BytesArray(alternativeInput);
    }

    /**
     * @param data A JSON string representing the data that should be associated with the trigger event.
     */
    public void setTriggerData(String data) {
        this.triggerData = new BytesArray(data);
    }

    /**
     * Sets the action execution mode for the give action (identified by its id).
     *
     * @param actionId      the action id.
     * @param actionMode    the execution mode of the action.
     */
    public void setActionMode(String actionId, ActionExecutionMode actionMode) {
        Objects.requireNonNull(actionId, "actionId cannot be null");
        actionModes.put(actionId, actionMode);
    }

    public Map<String, ActionExecutionMode> getActionModes() {
        return this.actionModes;
    }

    /**
     * @param debug indicates whether the watch should execute in debug mode. In debug mode the
     *              returned watch record will hold the execution {@code vars}
     */
    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    public boolean isDebug() {
        return debug;
    }

    @Override
    public String toString() {
        return "execute[" + id + "]";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (triggerData != null) {
            builder.rawField("trigger_data", triggerData.streamInput(), XContentType.JSON);
        }
        if (alternativeInput != null) {
            builder.rawField("alternative_input", alternativeInput.streamInput(), XContentType.JSON);
        }
        if (actionModes.size() > 0) {
            builder.field("action_modes", actionModes);
        }
        if (watchContent != null) {
            builder.rawField("watch", watchContent.streamInput(), XContentType.JSON);
        }
        builder.endObject();
        return builder;
    }
}

