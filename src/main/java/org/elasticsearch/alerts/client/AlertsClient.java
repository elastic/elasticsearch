/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.client;

import org.elasticsearch.action.*;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.update.TransportUpdateAction;
import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.AlertManager;
import org.elasticsearch.alerts.transport.actions.create.*;
import org.elasticsearch.alerts.transport.actions.delete.*;
import org.elasticsearch.alerts.transport.actions.get.*;
import org.elasticsearch.alerts.transport.actions.update.*;
import org.elasticsearch.client.support.Headers;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.HashMap;
import java.util.Map;

public class AlertsClient implements AlertsClientInterface {

    private final Headers headers;
    private final Map<GenericAction, TransportAction> internalActions;
    private final ThreadPool threadPool;

    @Inject
    public AlertsClient(ThreadPool threadPool,
                        Settings settings,
                        Headers headers,
                        ActionFilters filters,
                        TransportService transportService, ClusterService clusterService, AlertManager alertManager) {
        this.headers = headers;
        internalActions = new HashMap<>();
        this.threadPool = threadPool;

        internalActions.put(CreateAlertAction.INSTANCE, new TransportCreateAlertAction(settings,
                CreateAlertAction.NAME, transportService, clusterService, threadPool, filters, alertManager));

        internalActions.put(GetAlertAction.INSTANCE, new TransportGetAlertAction(settings,
                GetAlertAction.NAME, transportService, clusterService, threadPool, filters, alertManager));

        internalActions.put(DeleteAlertAction.INSTANCE, new TransportDeleteAlertAction(settings,
                DeleteAlertAction.NAME, transportService, clusterService, threadPool, filters, alertManager));

        internalActions.put(UpdateAlertAction.INSTANCE, new TransportUpdateAlertAction(settings,
                UpdateAlertAction.NAME, transportService, clusterService, threadPool, filters, alertManager));
    }


    @Override
    public GetAlertRequestBuilder prepareGetAlert(String alertName) {
        return new GetAlertRequestBuilder(this, alertName);
    }

    @Override
    public GetAlertRequestBuilder prepareGetAlert() {
        return new GetAlertRequestBuilder(this);
    }

    public void getAlert(GetAlertRequest request, ActionListener<GetAlertResponse> response){
        execute(GetAlertAction.INSTANCE, request, response);
    }

    @Override
    public ActionFuture<GetAlertResponse> getAlert(GetAlertRequest request) {
        return execute(GetAlertAction.INSTANCE, request);
    }



    @Override
    public DeleteAlertRequestBuilder prepareDeleteAlert(String alertName) {
        return new DeleteAlertRequestBuilder(this, alertName);
    }

    @Override
    public DeleteAlertRequestBuilder prepareDeleteAlert() {
        return new DeleteAlertRequestBuilder(this);
    }

    @Override
    public void deleteAlert(DeleteAlertRequest request, ActionListener<DeleteAlertResponse> response) {
        execute(DeleteAlertAction.INSTANCE, request, response);
    }

    @Override
    public ActionFuture<DeleteAlertResponse> deleteAlert(DeleteAlertRequest request) {
        return execute(DeleteAlertAction.INSTANCE, request);
    }



    @Override
    public CreateAlertRequestBuilder prepareCreateAlert(Alert alert) {
        return new CreateAlertRequestBuilder(this, alert);
    }

    @Override
    public CreateAlertRequestBuilder prepareCreateAlert() {
        return new CreateAlertRequestBuilder(this, null);
    }

    @Override
    public void createAlert(CreateAlertRequest request, ActionListener<CreateAlertResponse> response) {
        execute(CreateAlertAction.INSTANCE, request, response);
    }

    @Override
    public ActionFuture<CreateAlertResponse> createAlert(CreateAlertRequest request) {
        return execute(CreateAlertAction.INSTANCE, request);
    }


    @Override
    public UpdateAlertRequestBuilder prepareUpdateAlert(Alert alert) {
        return new UpdateAlertRequestBuilder(this, alert);
    }

    @Override
    public UpdateAlertRequestBuilder prepareUpdateAlert() {
        return new UpdateAlertRequestBuilder(this);
    }

    @Override
    public void updateAlert(UpdateAlertRequest request, ActionListener<UpdateAlertResponse> response) {
        execute(UpdateAlertAction.INSTANCE, request, response);
    }

    @Override
    public ActionFuture<UpdateAlertResponse> updateAlert(UpdateAlertRequest request) {
        return execute(UpdateAlertAction.INSTANCE, request);
    }


    @SuppressWarnings("unchecked")
    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends
            ActionRequestBuilder<Request, Response, RequestBuilder, AlertsClientInterface>>
    ActionFuture<Response> execute(Action<Request, Response, RequestBuilder, AlertsClientInterface> action, Request request) {
        headers.applyTo(request);
        TransportAction<Request, Response> transportAction = internalActions.get((AlertsClientAction)action);
        return transportAction.execute(request);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends
            ActionRequestBuilder<Request, Response, RequestBuilder, AlertsClientInterface>>
    void execute(Action<Request, Response, RequestBuilder, AlertsClientInterface> action, Request request, ActionListener<Response> listener) {
        headers.applyTo(request);
        TransportAction<Request, Response> transportAction = internalActions.get((AlertsClientAction)action);
        transportAction.execute(request, listener);
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, AlertsClientInterface>> RequestBuilder prepareExecute(Action<Request, Response, RequestBuilder, AlertsClientInterface> action) {
        return action.newRequestBuilder(this);
    }

    @Override
    public ThreadPool threadPool() {
        return threadPool;
    }
}
