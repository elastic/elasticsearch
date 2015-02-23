/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.client;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.alerts.transport.actions.ack.AckAlertAction;
import org.elasticsearch.alerts.transport.actions.ack.AckAlertRequest;
import org.elasticsearch.alerts.transport.actions.ack.AckAlertRequestBuilder;
import org.elasticsearch.alerts.transport.actions.ack.AckAlertResponse;
import org.elasticsearch.alerts.transport.actions.delete.DeleteAlertAction;
import org.elasticsearch.alerts.transport.actions.delete.DeleteAlertRequest;
import org.elasticsearch.alerts.transport.actions.delete.DeleteAlertRequestBuilder;
import org.elasticsearch.alerts.transport.actions.delete.DeleteAlertResponse;
import org.elasticsearch.alerts.transport.actions.get.GetAlertAction;
import org.elasticsearch.alerts.transport.actions.get.GetAlertRequest;
import org.elasticsearch.alerts.transport.actions.get.GetAlertRequestBuilder;
import org.elasticsearch.alerts.transport.actions.get.GetAlertResponse;
import org.elasticsearch.alerts.transport.actions.put.PutAlertAction;
import org.elasticsearch.alerts.transport.actions.put.PutAlertRequest;
import org.elasticsearch.alerts.transport.actions.put.PutAlertRequestBuilder;
import org.elasticsearch.alerts.transport.actions.put.PutAlertResponse;
import org.elasticsearch.alerts.transport.actions.service.AlertsServiceAction;
import org.elasticsearch.alerts.transport.actions.service.AlertsServiceRequest;
import org.elasticsearch.alerts.transport.actions.service.AlertsServiceRequestBuilder;
import org.elasticsearch.alerts.transport.actions.service.AlertsServiceResponse;
import org.elasticsearch.alerts.transport.actions.stats.AlertsStatsAction;
import org.elasticsearch.alerts.transport.actions.stats.AlertsStatsRequest;
import org.elasticsearch.alerts.transport.actions.stats.AlertsStatsRequestBuilder;
import org.elasticsearch.alerts.transport.actions.stats.AlertsStatsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;

/**
 */
public class AlertsClient {

    private final Client client;

    @Inject
    public AlertsClient(Client client) {
        this.client = client;
    }

    /**
     * Creates a request builder that gets an alert by name (id)
     *
     * @param alertName the name (id) of the alert
     * @return The request builder
     */
    public GetAlertRequestBuilder prepareGetAlert(String alertName) {
        return new GetAlertRequestBuilder(client, alertName);
    }

    /**
     * Creates a request builder that gets an alert
     *
     * @return the request builder
     */
    public GetAlertRequestBuilder prepareGetAlert() {
        return new GetAlertRequestBuilder(client);
    }

    /**
     * Gets an alert from the alert index
     *
     * @param request The get alert request
     * @param listener The listener for the get alert response containing the GetResponse for this alert
     */
    public void getAlert(GetAlertRequest request, ActionListener<GetAlertResponse> listener) {
        client.execute(GetAlertAction.INSTANCE, request, listener);
    }

    /**
     * Gets an alert from the alert index
     *
     * @param request The get alert request with the alert name (id)
     * @return The response containing the GetResponse for this alert
     */
    public ActionFuture<GetAlertResponse> getAlert(GetAlertRequest request) {
        return client.execute(GetAlertAction.INSTANCE, request);
    }

    /**
     * Creates a request builder to delete an alert by name (id)
     *
     * @param alertName the name (id) of the alert
     * @return The request builder
     */
    public DeleteAlertRequestBuilder prepareDeleteAlert(String alertName) {
        return new DeleteAlertRequestBuilder(client, alertName);
    }

    /**
     * Creates a request builder that deletes an alert
     *
     * @return The request builder
     */
    public DeleteAlertRequestBuilder prepareDeleteAlert() {
        return new DeleteAlertRequestBuilder(client);
    }

    /**
     * Deletes an alert
     *
     * @param request The delete request with the alert name (id) to be deleted
     * @param listener The listener for the delete alert response containing the DeleteResponse for this action
     */
    public void deleteAlert(DeleteAlertRequest request, ActionListener<DeleteAlertResponse> listener) {
        client.execute(DeleteAlertAction.INSTANCE, request, listener);
    }

    /**
     * Deletes an alert
     *
     * @param request The delete request with the alert name (id) to be deleted
     * @return The response containing the DeleteResponse for this action
     */
    public ActionFuture<DeleteAlertResponse> deleteAlert(DeleteAlertRequest request) {
        return client.execute(DeleteAlertAction.INSTANCE, request);
    }

    /**
     * Creates a request builder to build a request to put an alert
     *
     * @param alertName The name of the alert to put
     * @return The builder to create the alert
     */
    public PutAlertRequestBuilder preparePutAlert(String alertName) {
        return new PutAlertRequestBuilder(client, alertName);
    }

    /**
     * Creates a request builder to build a request to put an alert
     *
     * @return The builder
     */
    public PutAlertRequestBuilder preparePutAlert() {
        return new PutAlertRequestBuilder(client);
    }

    /**
     * Put an alert and registers it with the scheduler
     *
     * @param request The request containing the alert to index and register
     * @param listener The listener for the response containing the IndexResponse for this alert
     */
    public void putAlert(PutAlertRequest request, ActionListener<PutAlertResponse> listener) {
        client.execute(PutAlertAction.INSTANCE, request, listener);
    }

    /**
     * Put an alert and registers it with the scheduler
     *
     * @param request The request containing the alert to index and register
     * @return The response containing the IndexResponse for this alert
     */
    public ActionFuture<PutAlertResponse> putAlert(PutAlertRequest request) {
        return client.execute(PutAlertAction.INSTANCE, request);
    }

    /**
     * Gets the alert stats
     *
     * @param request The request for the alert stats
     * @return The response containing the StatsResponse for this action
     */
    public ActionFuture<AlertsStatsResponse> alertsStats(AlertsStatsRequest request) {
        return client.execute(AlertsStatsAction.INSTANCE, request);
    }

    /**
     * Creates a request builder to build a request to get the alerts stats
     *
     * @return The builder get the alerts stats
     */
    public AlertsStatsRequestBuilder prepareAlertsStats() {
        return new AlertsStatsRequestBuilder(client);
    }

    /**
     * Gets the alert stats
     *
     * @param request The request for the alert stats
     * @param listener The listener for the response containing the AlertsStatsResponse
     */
    public void alertsStats(AlertsStatsRequest request, ActionListener<AlertsStatsResponse> listener) {
        client.execute(AlertsStatsAction.INSTANCE, request, listener);
    }

    /**
     * Creates a request builder to ack an alert by name (id)
     *
     * @param alertName the name (id) of the alert
     * @return The request builder
     */
    public AckAlertRequestBuilder prepareAckAlert(String alertName) {
        return new AckAlertRequestBuilder(client, alertName);
    }

    /**
     * Creates a request builder that acks an alert
     *
     * @return The request builder
     */
    public AckAlertRequestBuilder prepareAckAlert() {
        return new AckAlertRequestBuilder(client);
    }

    /**
     * Ack an alert
     *
     * @param request The ack request with the alert name (id) to be acked
     * @param listener The listener for the ack alert response
     */
    public void ackAlert(AckAlertRequest request, ActionListener<AckAlertResponse> listener) {
        client.execute(AckAlertAction.INSTANCE, request, listener);
    }

    /**
     * Acks an alert
     *
     * @param request The ack request with the alert name (id) to be acked
     * @return The AckAlertResponse
     */
    public ActionFuture<AckAlertResponse> ackAlert(AckAlertRequest request) {
        return client.execute(AckAlertAction.INSTANCE, request);
    }

    /**
     * Prepare make an alert service request.
     */
    public AlertsServiceRequestBuilder prepareAlertService() {
        return new AlertsServiceRequestBuilder(client);
    }

    /**
     * Perform an alert service request to either start, stop or restart the alerting plugin.
     */
    public void alertService(AlertsServiceRequest request, ActionListener<AlertsServiceResponse> listener) {
        client.execute(AlertsServiceAction.INSTANCE, request, listener);
    }

    /**
     * Perform an alert service request to either start, stop or restart the alerting plugin.
     */
    public ActionFuture<AlertsServiceResponse> alertService(AlertsServiceRequest request) {
        return client.execute(AlertsServiceAction.INSTANCE, request);
    }
}
