/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.datastreams;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.xpack.core.action.CreateDataStreamAction;
import org.elasticsearch.xpack.core.action.DataStreamsStatsAction;
import org.elasticsearch.xpack.core.action.DeleteDataStreamAction;
import org.elasticsearch.xpack.core.action.GetDataStreamAction;

import java.util.Objects;

public class DataStreamClient {

    private final ElasticsearchClient client;

    public DataStreamClient(ElasticsearchClient client) {
        this.client = Objects.requireNonNull(client);
    }

    public void createDataStream(CreateDataStreamAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        client.execute(CreateDataStreamAction.INSTANCE, request, listener);
    }

    public ActionFuture<AcknowledgedResponse> createDataStream(CreateDataStreamAction.Request request) {
        final PlainActionFuture<AcknowledgedResponse> listener = PlainActionFuture.newFuture();
        client.execute(CreateDataStreamAction.INSTANCE, request, listener);
        return listener;
    }

    public void getDataStream(GetDataStreamAction.Request request, ActionListener<GetDataStreamAction.Response> listener) {
        client.execute(GetDataStreamAction.INSTANCE, request, listener);
    }

    public ActionFuture<GetDataStreamAction.Response> getDataStream(GetDataStreamAction.Request request) {
        final PlainActionFuture<GetDataStreamAction.Response> listener = PlainActionFuture.newFuture();
        client.execute(GetDataStreamAction.INSTANCE, request, listener);
        return listener;
    }

    public void deleteDataStream(DeleteDataStreamAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        client.execute(DeleteDataStreamAction.INSTANCE, request, listener);
    }

    public ActionFuture<AcknowledgedResponse> deleteDataStream(DeleteDataStreamAction.Request request) {
        final PlainActionFuture<AcknowledgedResponse> listener = PlainActionFuture.newFuture();
        client.execute(DeleteDataStreamAction.INSTANCE, request, listener);
        return listener;
    }

    public void dataStreamsStats(DataStreamsStatsAction.Request request, ActionListener<DataStreamsStatsAction.Response> listener) {
        client.execute(DataStreamsStatsAction.INSTANCE, request, listener);
    }

    public ActionFuture<DataStreamsStatsAction.Response> dataStreamsStats(DataStreamsStatsAction.Request request) {
        final PlainActionFuture<DataStreamsStatsAction.Response> listener = PlainActionFuture.newFuture();
        client.execute(DataStreamsStatsAction.INSTANCE, request, listener);
        return listener;
    }
}
