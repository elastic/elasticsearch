/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.monitoring.client;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringBulkAction;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringBulkRequest;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringBulkRequestBuilder;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringBulkResponse;

import java.util.Map;

public class MonitoringClient {

    private final Client client;

    @Inject
    public MonitoringClient(Client client) {
        this.client = client;
    }


    /**
     * Creates a request builder that bulk index monitoring documents.
     *
     * @return The request builder
     */
    public MonitoringBulkRequestBuilder prepareMonitoringBulk() {
        return new MonitoringBulkRequestBuilder(client);
    }

    /**
     * Executes a bulk of index operations that concern monitoring documents.
     *
     * @param request  The monitoring bulk request
     * @param listener A listener to be notified with a result
     */
    public void bulk(MonitoringBulkRequest request, ActionListener<MonitoringBulkResponse> listener) {
        client.execute(MonitoringBulkAction.INSTANCE, request, listener);
    }

    /**
     * Executes a bulk of index operations that concern monitoring documents.
     *
     * @param request  The monitoring bulk request
     */
    public ActionFuture<MonitoringBulkResponse> bulk(MonitoringBulkRequest request) {
        return client.execute(MonitoringBulkAction.INSTANCE, request);
    }

    public MonitoringClient filterWithHeader(Map<String, String> headers) {
        return new MonitoringClient(client.filterWithHeader(headers));
    }
}
