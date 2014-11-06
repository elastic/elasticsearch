/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.client;

import org.elasticsearch.action.*;
import org.elasticsearch.alerts.transport.actions.index.IndexAlertRequest;
import org.elasticsearch.alerts.transport.actions.index.IndexAlertRequestBuilder;
import org.elasticsearch.alerts.transport.actions.index.IndexAlertResponse;
import org.elasticsearch.alerts.transport.actions.delete.DeleteAlertRequest;
import org.elasticsearch.alerts.transport.actions.delete.DeleteAlertRequestBuilder;
import org.elasticsearch.alerts.transport.actions.delete.DeleteAlertResponse;
import org.elasticsearch.alerts.transport.actions.get.GetAlertRequest;
import org.elasticsearch.alerts.transport.actions.get.GetAlertRequestBuilder;
import org.elasticsearch.alerts.transport.actions.get.GetAlertResponse;
import org.elasticsearch.client.ElasticsearchClient;

/**
 */
public interface AlertsClientInterface extends ElasticsearchClient<AlertsClientInterface> {

    GetAlertRequestBuilder prepareGetAlert(String alertName);

    GetAlertRequestBuilder prepareGetAlert();

    public void getAlert(GetAlertRequest request, ActionListener<GetAlertResponse> response);

    ActionFuture<GetAlertResponse> getAlert(GetAlertRequest request);


    DeleteAlertRequestBuilder prepareDeleteAlert(String alertName);

    DeleteAlertRequestBuilder prepareDeleteAlert();

    public void deleteAlert(DeleteAlertRequest request, ActionListener<DeleteAlertResponse> response);

    ActionFuture<DeleteAlertResponse> deleteAlert(DeleteAlertRequest request);

    IndexAlertRequestBuilder prepareIndexAlert(String alertName);

    IndexAlertRequestBuilder prepareIndexAlert();

    public void indexAlert(IndexAlertRequest request, ActionListener<IndexAlertResponse> response);

    ActionFuture<IndexAlertResponse> indexAlert(IndexAlertRequest request);

}
