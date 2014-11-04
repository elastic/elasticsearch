/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.client;

import org.elasticsearch.action.*;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.transport.actions.create.CreateAlertRequest;
import org.elasticsearch.alerts.transport.actions.create.CreateAlertRequestBuilder;
import org.elasticsearch.alerts.transport.actions.create.CreateAlertResponse;
import org.elasticsearch.alerts.transport.actions.delete.DeleteAlertRequest;
import org.elasticsearch.alerts.transport.actions.delete.DeleteAlertRequestBuilder;
import org.elasticsearch.alerts.transport.actions.delete.DeleteAlertResponse;
import org.elasticsearch.alerts.transport.actions.get.GetAlertRequest;
import org.elasticsearch.alerts.transport.actions.get.GetAlertRequestBuilder;
import org.elasticsearch.alerts.transport.actions.get.GetAlertResponse;
import org.elasticsearch.alerts.transport.actions.update.UpdateAlertRequest;
import org.elasticsearch.alerts.transport.actions.update.UpdateAlertRequestBuilder;
import org.elasticsearch.alerts.transport.actions.update.UpdateAlertResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.lease.Releasable;

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


    CreateAlertRequestBuilder prepareCreateAlert(Alert alert);

    CreateAlertRequestBuilder prepareCreateAlert();

    public void createAlert(CreateAlertRequest request, ActionListener<CreateAlertResponse> response);

    ActionFuture<CreateAlertResponse> createAlert(CreateAlertRequest request);


    UpdateAlertRequestBuilder prepareUpdateAlert(Alert alert);

    UpdateAlertRequestBuilder prepareUpdateAlert();

    public void updateAlert(UpdateAlertRequest request, ActionListener<UpdateAlertResponse> response);

    ActionFuture<UpdateAlertResponse> updateAlert(UpdateAlertRequest request);




}
