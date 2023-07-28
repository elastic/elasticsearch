/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.transport.actions.execute;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.watcher.execution.ActionExecutionMode;
import org.elasticsearch.xpack.core.watcher.trigger.TriggerEvent;

import java.io.IOException;
import java.util.Map;

/**
 * A execute watch action request builder.
 */
public class ExecuteWatchRequestBuilder extends ActionRequestBuilder<ExecuteWatchRequest, ExecuteWatchResponse> {

    public ExecuteWatchRequestBuilder(ElasticsearchClient client) {
        super(client, ExecuteWatchAction.INSTANCE, new ExecuteWatchRequest());
    }

    public ExecuteWatchRequestBuilder(ElasticsearchClient client, String watchName) {
        super(client, ExecuteWatchAction.INSTANCE, new ExecuteWatchRequest(watchName));
    }

    /**
     * Sets the id of the watch to be executed
     */
    public ExecuteWatchRequestBuilder setId(String id) {
        this.request().setId(id);
        return this;
    }

    /**
     * @param ignoreCondition set if the condition for this execution be ignored
     */
    public ExecuteWatchRequestBuilder setIgnoreCondition(boolean ignoreCondition) {
        request.setIgnoreCondition(ignoreCondition);
        return this;
    }

    /**
     * @param recordExecution Sets if this execution be recorded in the history index and reflected in the watch
     */
    public ExecuteWatchRequestBuilder setRecordExecution(boolean recordExecution) {
        request.setRecordExecution(recordExecution);
        return this;
    }

    /**
     * @param alternativeInput Set's the alernative input
     */
    public ExecuteWatchRequestBuilder setAlternativeInput(Map<String, Object> alternativeInput) {
        request.setAlternativeInput(alternativeInput);
        return this;
    }

    /**
     * @param data The data that should be associated with the trigger event
     */
    public ExecuteWatchRequestBuilder setTriggerData(Map<String, Object> data) throws IOException {
        request.setTriggerData(data);
        return this;
    }

    /**
     * @param triggerEvent the trigger event to use
     */
    public ExecuteWatchRequestBuilder setTriggerEvent(TriggerEvent triggerEvent) throws IOException {
        request.setTriggerEvent(triggerEvent);
        return this;
    }

    /**
     * @param watchSource instead of using an existing watch use this non persisted watch
     */
    public ExecuteWatchRequestBuilder setWatchSource(BytesReference watchSource, XContentType xContentType) {
        request.setWatchSource(watchSource, xContentType);
        return this;
    }

    /**
     * Sets the mode in which the given action (identified by its id) will be handled.
     *
     * @param actionId   The id of the action
     * @param actionMode The mode in which the action will be handled in the execution
     */
    public ExecuteWatchRequestBuilder setActionMode(String actionId, ActionExecutionMode actionMode) {
        request.setActionMode(actionId, actionMode);
        return this;
    }

    /**
     * @param debug indicates whether the watch should execute in debug mode. In debug mode the
     *              returned watch record will hold the execution {@code vars}
     */
    public ExecuteWatchRequestBuilder setDebug(boolean debug) {
        request.setDebug(debug);
        return this;
    }
}
