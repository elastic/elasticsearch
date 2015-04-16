/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport.actions.execute;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.watcher.client.WatcherClient;

import java.util.Map;

/**
 * A execute watch action request builder.
 */
public class ExecuteWatchRequestBuilder extends MasterNodeOperationRequestBuilder<ExecuteWatchRequest, ExecuteWatchResponse, ExecuteWatchRequestBuilder, Client> {

    public ExecuteWatchRequestBuilder(Client client) {
        super(client, new ExecuteWatchRequest());
    }

    public ExecuteWatchRequestBuilder(Client client, String watchName) {
        super(client, new ExecuteWatchRequest(watchName));
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
     * @param ignoreThrottle Sets if the throttle should be ignored for this execution
     */
    public ExecuteWatchRequestBuilder setIgnoreThrottle(boolean ignoreThrottle) {
        request.setIgnoreThrottle(ignoreThrottle);
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
     * @param triggerData the trigger data to use
     */
    public ExecuteWatchRequestBuilder setTriggerData(Map<String, Object> triggerData) {
        request.setTriggerData(triggerData);
        return this;
    }

    /**
     * @param simulatedActionIds a list of action ids to run in simulations for this execution
     */
    public ExecuteWatchRequestBuilder addSimulatedActions(String ... simulatedActionIds) {
        request.addSimulatedActions(simulatedActionIds);
        return this;
    }

    @Override
    protected void doExecute(final ActionListener<ExecuteWatchResponse> listener) {
        new WatcherClient(client).executeWatch(request, listener);
    }

}
