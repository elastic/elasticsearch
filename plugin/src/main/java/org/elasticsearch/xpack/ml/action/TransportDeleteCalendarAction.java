/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.MlMetaIndex;
import org.elasticsearch.xpack.ml.calendars.Calendar;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;

import static org.elasticsearch.xpack.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.ClientHelper.executeAsyncWithOrigin;

public class TransportDeleteCalendarAction extends HandledTransportAction<DeleteCalendarAction.Request, DeleteCalendarAction.Response> {

    private final Client client;

    @Inject
    public TransportDeleteCalendarAction(Settings settings, ThreadPool threadPool,
                                         TransportService transportService, ActionFilters actionFilters,
                                         IndexNameExpressionResolver indexNameExpressionResolver,
                                         Client client) {
        super(settings, DeleteCalendarAction.NAME, threadPool, transportService, actionFilters,
                indexNameExpressionResolver, DeleteCalendarAction.Request::new);
        this.client = client;
    }

    @Override
    protected void doExecute(DeleteCalendarAction.Request request, ActionListener<DeleteCalendarAction.Response> listener) {

        final String calendarId = request.getCalendarId();

        DeleteRequest deleteRequest = new DeleteRequest(MlMetaIndex.INDEX_NAME, MlMetaIndex.TYPE, Calendar.documentId(calendarId));

        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        bulkRequestBuilder.add(deleteRequest);
        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        executeAsyncWithOrigin(client, ML_ORIGIN, BulkAction.INSTANCE, bulkRequestBuilder.request(),
                new ActionListener<BulkResponse>() {
                    @Override
                    public void onResponse(BulkResponse bulkResponse) {
                        if (bulkResponse.getItems()[0].status() == RestStatus.NOT_FOUND) {
                            listener.onFailure(new ResourceNotFoundException("Could not delete calendar with ID [" + calendarId
                                    + "] because it does not exist"));
                        } else {
                            listener.onResponse(new DeleteCalendarAction.Response(true));
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(ExceptionsHelper.serverError("Could not delete calendar with ID [" + calendarId + "]", e));
                    }
                });
    }
}
