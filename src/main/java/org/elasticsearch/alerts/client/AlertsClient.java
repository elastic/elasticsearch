/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.client;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.alerts.transport.actions.ack.AckAlertRequest;
import org.elasticsearch.alerts.transport.actions.ack.AckAlertRequestBuilder;
import org.elasticsearch.alerts.transport.actions.ack.AckAlertResponse;
import org.elasticsearch.alerts.transport.actions.config.ConfigAlertRequest;
import org.elasticsearch.alerts.transport.actions.config.ConfigAlertRequestBuilder;
import org.elasticsearch.alerts.transport.actions.config.ConfigAlertResponse;
import org.elasticsearch.alerts.transport.actions.delete.DeleteAlertRequest;
import org.elasticsearch.alerts.transport.actions.delete.DeleteAlertRequestBuilder;
import org.elasticsearch.alerts.transport.actions.delete.DeleteAlertResponse;
import org.elasticsearch.alerts.transport.actions.get.GetAlertRequest;
import org.elasticsearch.alerts.transport.actions.get.GetAlertRequestBuilder;
import org.elasticsearch.alerts.transport.actions.get.GetAlertResponse;
import org.elasticsearch.alerts.transport.actions.put.PutAlertRequest;
import org.elasticsearch.alerts.transport.actions.put.PutAlertRequestBuilder;
import org.elasticsearch.alerts.transport.actions.put.PutAlertResponse;
import org.elasticsearch.alerts.transport.actions.service.AlertServiceRequestBuilder;
import org.elasticsearch.alerts.transport.actions.service.AlertsServiceRequest;
import org.elasticsearch.alerts.transport.actions.service.AlertsServiceResponse;
import org.elasticsearch.alerts.transport.actions.stats.AlertsStatsRequest;
import org.elasticsearch.alerts.transport.actions.stats.AlertsStatsRequestBuilder;
import org.elasticsearch.alerts.transport.actions.stats.AlertsStatsResponse;
import org.elasticsearch.client.ElasticsearchClient;

/**
 */
public interface AlertsClient extends ElasticsearchClient<AlertsClient> {

    /**
     * Creates a request builder that gets an alert by name (id)
     *
     * @param alertName the name (id) of the alert
     * @return The request builder
     */
    GetAlertRequestBuilder prepareGetAlert(String alertName);

    /**
     * Creates a request builder that gets an alert
     *
     * @return the request builder
     */
    GetAlertRequestBuilder prepareGetAlert();

    /**
     * Gets an alert from the alert index
     *
     * @param request The get alert request
     * @param listener The listener for the get alert response containing the GetResponse for this alert
     */
    void getAlert(GetAlertRequest request, ActionListener<GetAlertResponse> listener);

    /**
     * Gets an alert from the alert index
     *
     * @param request The get alert request with the alert name (id)
     * @return The response containing the GetResponse for this alert
     */
    ActionFuture<GetAlertResponse> getAlert(GetAlertRequest request);

    /**
     * Creates a request builder to delete an alert by name (id)
     *
     * @param alertName the name (id) of the alert
     * @return The request builder
     */
    DeleteAlertRequestBuilder prepareDeleteAlert(String alertName);

    /**
     * Creates a request builder that deletes an alert
     *
     * @return The request builder
     */
    DeleteAlertRequestBuilder prepareDeleteAlert();

    /**
     * Deletes an alert
     *
     * @param request The delete request with the alert name (id) to be deleted
     * @param listener The listener for the delete alert response containing the DeleteResponse for this action
     */
    void deleteAlert(DeleteAlertRequest request, ActionListener<DeleteAlertResponse> listener);

    /**
     * Deletes an alert
     *
     * @param request The delete request with the alert name (id) to be deleted
     * @return The response containing the DeleteResponse for this action
     */
    ActionFuture<DeleteAlertResponse> deleteAlert(DeleteAlertRequest request);

    /**
     * Creates a request builder to build a request to put an alert
     *
     * @param alertName The name of the alert to put
     * @return The builder to create the alert
     */
    PutAlertRequestBuilder preparePutAlert(String alertName);

    /**
     * Creates a request builder to build a request to put an alert
     *
     * @return The builder
     */
    PutAlertRequestBuilder preparePutAlert();

    /**
     * Put an alert and registers it with the scheduler
     *
     * @param request The request containing the alert to index and register
     * @param listener The listener for the response containing the IndexResponse for this alert
     */
    void putAlert(PutAlertRequest request, ActionListener<PutAlertResponse> listener);

    /**
     * Put an alert and registers it with the scheduler
     *
     * @param request The request containing the alert to index and register
     * @return The response containing the IndexResponse for this alert
     */
    ActionFuture<PutAlertResponse> putAlert(PutAlertRequest request);


    /**
     * Gets the alert stats
     *
     * @param request The request for the alert stats
     * @return The response containing the StatsResponse for this action
     */
    ActionFuture<AlertsStatsResponse> alertsStats(AlertsStatsRequest request);

    /**
     * Creates a request builder to build a request to get the alerts stats
     *
     * @return The builder get the alerts stats
     */
    AlertsStatsRequestBuilder prepareAlertsStats();

    /**
     * Gets the alert stats
     *
     * @param request The request for the alert stats
     * @param listener The listener for the response containing the AlertsStatsResponse
     */
    void alertsStats(AlertsStatsRequest request, ActionListener<AlertsStatsResponse> listener);

    /**
     * Creates a request builder to ack an alert by name (id)
     *
     * @param alertName the name (id) of the alert
     * @return The request builder
     */
    AckAlertRequestBuilder prepareAckAlert(String alertName);

    /**
     * Creates a request builder that acks an alert
     *
     * @return The request builder
     */
    AckAlertRequestBuilder prepareAckAlert();

    /**
     * Ack an alert
     *
     * @param request The ack request with the alert name (id) to be acked
     * @param listener The listener for the ack alert response
     */
    void ackAlert(AckAlertRequest request, ActionListener<AckAlertResponse> listener);

    /**
     * Acks an alert
     *
     * @param request The ack request with the alert name (id) to be acked
     * @return The AckAlertResponse
     */
    ActionFuture<AckAlertResponse> ackAlert(AckAlertRequest request);

    /**
     * Prepare make an alert service request.
     */
    AlertServiceRequestBuilder prepareAlertService();

    /**
     * Perform an alert service request to either start, stop or restart the alerting plugin.
     */
    void alertService(AlertsServiceRequest request, ActionListener<AlertsServiceResponse> listener);

    /**
     * Perform an alert service request to either start, stop or restart the alerting plugin.
     */
    ActionFuture<AlertsServiceResponse> alertService(AlertsServiceRequest request);


    /**
     * Prepare make an alert config request.
     */
    ConfigAlertRequestBuilder prepareAlertConfig();

    /**
     * Perform an config alert request
     */
    void alertConfig(ConfigAlertRequest request, ActionListener<ConfigAlertResponse> listener);

    /**
     * Perform an config alert request
     */
    ActionFuture<ConfigAlertResponse> alertConfig(ConfigAlertRequest request);


}
