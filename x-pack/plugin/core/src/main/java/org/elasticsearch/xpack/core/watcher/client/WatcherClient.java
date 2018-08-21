/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.watcher.client;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.protocol.xpack.watcher.DeleteWatchRequest;
import org.elasticsearch.protocol.xpack.watcher.DeleteWatchResponse;
import org.elasticsearch.protocol.xpack.watcher.PutWatchRequest;
import org.elasticsearch.protocol.xpack.watcher.PutWatchResponse;
import org.elasticsearch.xpack.core.watcher.transport.actions.ack.AckWatchAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.ack.AckWatchRequest;
import org.elasticsearch.xpack.core.watcher.transport.actions.ack.AckWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.ack.AckWatchResponse;
import org.elasticsearch.xpack.core.watcher.transport.actions.activate.ActivateWatchAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.activate.ActivateWatchRequest;
import org.elasticsearch.xpack.core.watcher.transport.actions.activate.ActivateWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.activate.ActivateWatchResponse;
import org.elasticsearch.xpack.core.watcher.transport.actions.delete.DeleteWatchAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.delete.DeleteWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.execute.ExecuteWatchAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.execute.ExecuteWatchRequest;
import org.elasticsearch.xpack.core.watcher.transport.actions.execute.ExecuteWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.execute.ExecuteWatchResponse;
import org.elasticsearch.xpack.core.watcher.transport.actions.get.GetWatchAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.get.GetWatchRequest;
import org.elasticsearch.xpack.core.watcher.transport.actions.get.GetWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.get.GetWatchResponse;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.PutWatchAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.PutWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.service.WatcherServiceAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.service.WatcherServiceRequest;
import org.elasticsearch.xpack.core.watcher.transport.actions.service.WatcherServiceRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.stats.WatcherStatsAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.stats.WatcherStatsRequest;
import org.elasticsearch.xpack.core.watcher.transport.actions.stats.WatcherStatsRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.stats.WatcherStatsResponse;

import java.util.Map;

public class WatcherClient {

    private final Client client;

    public WatcherClient(Client client) {
        this.client = client;
    }

    /**
     * Creates a request builder that gets an watch by id
     *
     * @param id the id of the watch
     * @return The request builder
     */
    public GetWatchRequestBuilder prepareGetWatch(String id) {
        return new GetWatchRequestBuilder(client, id);
    }

    /**
     * Creates a request builder that gets an watch
     *
     * @return the request builder
     */
    public GetWatchRequestBuilder prepareGetWatch() {
        return new GetWatchRequestBuilder(client);
    }

    /**
     * Gets an watch from the watch index
     *
     * @param request The get watch request
     * @param listener The listener for the get watch response containing the GetResponse for this watch
     */
    public void getWatch(GetWatchRequest request, ActionListener<GetWatchResponse> listener) {
        client.execute(GetWatchAction.INSTANCE, request, listener);
    }

    /**
     * Gets an watch from the watch index
     *
     * @param request The get watch request with the watch id
     * @return The response containing the GetResponse for this watch
     */
    public ActionFuture<GetWatchResponse> getWatch(GetWatchRequest request) {
        return client.execute(GetWatchAction.INSTANCE, request);
    }

    /**
     * Creates a request builder to delete an watch by id
     *
     * @param id the id of the watch
     * @return The request builder
     */
    public DeleteWatchRequestBuilder prepareDeleteWatch(String id) {
        return new DeleteWatchRequestBuilder(client, id);
    }

    /**
     * Creates a request builder that deletes an watch
     *
     * @return The request builder
     */
    public DeleteWatchRequestBuilder prepareDeleteWatch() {
        return new DeleteWatchRequestBuilder(client);
    }

    /**
     * Deletes an watch
     *
     * @param request The delete request with the watch id to be deleted
     * @param listener The listener for the delete watch response containing the DeleteResponse for this action
     */
    public void deleteWatch(DeleteWatchRequest request, ActionListener<DeleteWatchResponse> listener) {
        client.execute(DeleteWatchAction.INSTANCE, request, listener);
    }

    /**
     * Deletes an watch
     *
     * @param request The delete request with the watch id to be deleted
     * @return The response containing the DeleteResponse for this action
     */
    public ActionFuture<DeleteWatchResponse> deleteWatch(DeleteWatchRequest request) {
        return client.execute(DeleteWatchAction.INSTANCE, request);
    }

    /**
     * Creates a request builder to build a request to put an watch
     *
     * @param id The id of the watch to put
     * @return The builder to create the watch
     */
    public PutWatchRequestBuilder preparePutWatch(String id) {
        return new PutWatchRequestBuilder(client, id);
    }

    /**
     * Creates a request builder to build a request to put a watch
     *
     * @return The builder
     */
    public PutWatchRequestBuilder preparePutWatch() {
        return new PutWatchRequestBuilder(client);
    }

    /**
     * Adds the given watch to the watcher
     *
     * @param request The request containing the watch to be added
     * @param listener The listener for the response containing the IndexResponse for this watch
     */
    public void putWatch(PutWatchRequest request, ActionListener<PutWatchResponse> listener) {
        client.execute(PutWatchAction.INSTANCE, request, listener);
    }

    /**
     * Adds a new watch
     *
     * @param request The request containing the watch to be added
     * @return The response containing the IndexResponse for this watch
     */
    public ActionFuture<PutWatchResponse> putWatch(PutWatchRequest request) {
        return client.execute(PutWatchAction.INSTANCE, request);
    }

    /**
     * Gets the watcher stats
     *
     * @param request The request for the watcher stats
     * @return The response containing the StatsResponse for this action
     */
    public ActionFuture<WatcherStatsResponse> watcherStats(WatcherStatsRequest request) {
        return client.execute(WatcherStatsAction.INSTANCE, request);
    }

    /**
     * Creates a request builder to build a request to get the watcher stats
     *
     * @return The builder get the watcher stats
     */
    public WatcherStatsRequestBuilder prepareWatcherStats() {
        return new WatcherStatsRequestBuilder(client);
    }

    /**
     * Gets the watcher stats
     *
     * @param request The request for the watcher stats
     * @param listener The listener for the response containing the WatcherStatsResponse
     */
    public void watcherStats(WatcherStatsRequest request, ActionListener<WatcherStatsResponse> listener) {
        client.execute(WatcherStatsAction.INSTANCE, request, listener);
    }

    /**
     * Creates a request builder to ack a watch by id
     *
     * @param id the id of the watch
     * @return The request builder
     */
    public AckWatchRequestBuilder prepareAckWatch(String id) {
        return new AckWatchRequestBuilder(client, id);
    }

    /**
     * Ack a watch
     *
     * @param request The ack request with the watch id to be acked
     * @param listener The listener for the ack watch response
     */
    public void ackWatch(AckWatchRequest request, ActionListener<AckWatchResponse> listener) {
        client.execute(AckWatchAction.INSTANCE, request, listener);
    }

    /**
     * Acks a watch
     *
     * @param request The ack request with the watch id to be acked
     * @return The AckWatchResponse
     */
    public ActionFuture<AckWatchResponse> ackWatch(AckWatchRequest request) {
        return client.execute(AckWatchAction.INSTANCE, request);
    }

    /**
     * Creates a request builder to activate a watch by id
     *
     * @param id the id of the watch
     * @param activate indicates whether to activate or deactivate the watch
     * @return The request builder
     */
    public ActivateWatchRequestBuilder prepareActivateWatch(String id, boolean activate) {
        return new ActivateWatchRequestBuilder(client, id, activate);
    }

    /**
     * Activate a watch
     *
     * @param request The activate request with the watch id
     * @param listener The listener for the activate watch response
     */
    public void activateWatch(ActivateWatchRequest request, ActionListener<ActivateWatchResponse> listener) {
        client.execute(ActivateWatchAction.INSTANCE, request, listener);
    }

    /**
     * Activates a watch
     *
     * @param request The de/activate request with the watch id.
     */
    public ActionFuture<ActivateWatchResponse> activateWatch(ActivateWatchRequest request) {
        return client.execute(ActivateWatchAction.INSTANCE, request);
    }


    /**
     * Prepare a watch service request.
     */
    public WatcherServiceRequestBuilder prepareWatchService() {
        return new WatcherServiceRequestBuilder(client);
    }

    /**
     * Perform an watcher service request to either start, stop or restart the service.
     */
    public void watcherService(WatcherServiceRequest request, ActionListener<AcknowledgedResponse> listener) {
        client.execute(WatcherServiceAction.INSTANCE, request, listener);
    }

    /**
     * Perform an watcher service request to either start, stop or restart the service.
     */
    public ActionFuture<AcknowledgedResponse> watcherService(WatcherServiceRequest request) {
        return client.execute(WatcherServiceAction.INSTANCE, request);
    }



    /**
     * Creates a request builder to execute a watch by id
     *
     * @param id the id of the watch
     * @return The request builder
     */
    public ExecuteWatchRequestBuilder prepareExecuteWatch(String id) {
        return new ExecuteWatchRequestBuilder(client, id);
    }

    /**
     * Creates a request builder that executes a watch
     *
     * @return The request builder
     */
    public ExecuteWatchRequestBuilder prepareExecuteWatch() {
        return new ExecuteWatchRequestBuilder(client);
    }

    /**
     * executes a watch
     *
     * @param request The run request with the watch id to be executed
     * @param listener The listener for the run watch response
     */
    public void executeWatch(ExecuteWatchRequest request, ActionListener<ExecuteWatchResponse> listener) {
        client.execute(ExecuteWatchAction.INSTANCE, request, listener);
    }

    /**
     * Executes an watch
     *
     * @param request The execute request with the watch id to be executed
     * @return The AckWatchResponse
     */
    public ActionFuture<ExecuteWatchResponse> executeWatch(ExecuteWatchRequest request) {
        return client.execute(ExecuteWatchAction.INSTANCE, request);
    }

    public WatcherClient filterWithHeader(Map<String, String> headers) {
        return new WatcherClient(client.filterWithHeader(headers));
    }
}
