/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.MlMetaIndex;
import org.elasticsearch.xpack.ml.calendars.Calendar;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xpack.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.ClientHelper.executeAsyncWithOrigin;

public class DeleteCalendarAction extends Action<DeleteCalendarAction.Request, DeleteCalendarAction.Response,
        DeleteCalendarAction.RequestBuilder> {

    public static final DeleteCalendarAction INSTANCE = new DeleteCalendarAction();
    public static final String NAME = "cluster:admin/xpack/ml/calendars/delete";

    private DeleteCalendarAction() {
        super(NAME);
    }

    @Override
    public RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RequestBuilder(client, this);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends AcknowledgedRequest<Request> {


        private String calendarId;

        Request() {

        }

        public Request(String calendarId) {
            this.calendarId = ExceptionsHelper.requireNonNull(calendarId, Calendar.ID.getPreferredName());
        }

        public String getCalendarId() {
            return calendarId;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            calendarId = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(calendarId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(calendarId);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }

            Request other = (Request) obj;
            return Objects.equals(calendarId, other.calendarId);
        }
    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, Response,
                RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client, DeleteCalendarAction action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends AcknowledgedResponse {

        public Response(boolean acknowledged) {
            super(acknowledged);
        }

        private Response() {}

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            readAcknowledged(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            writeAcknowledged(out);
        }
    }

    public static class TransportAction extends HandledTransportAction<DeleteCalendarAction.Request, DeleteCalendarAction.Response> {

        private final Client client;

        @Inject
        public TransportAction(Settings settings, ThreadPool threadPool,
                               TransportService transportService, ActionFilters actionFilters,
                               IndexNameExpressionResolver indexNameExpressionResolver,
                               Client client) {
            super(settings, NAME, threadPool, transportService, actionFilters,
                    indexNameExpressionResolver, Request::new);
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
                                listener.onResponse(new Response(true));
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            listener.onFailure(ExceptionsHelper.serverError("Could not delete calendar with ID [" + calendarId + "]", e));
                        }
                    });
        }
    }
}
