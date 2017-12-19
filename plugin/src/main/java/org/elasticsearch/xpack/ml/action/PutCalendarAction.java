/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.MlMetaIndex;
import org.elasticsearch.xpack.ml.MlMetadata;
import org.elasticsearch.xpack.ml.calendars.Calendar;
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.watcher.support.Exceptions;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xpack.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.ClientHelper.executeAsyncWithOrigin;

public class PutCalendarAction extends Action<PutCalendarAction.Request, PutCalendarAction.Response, PutCalendarAction.RequestBuilder>  {
    public static final PutCalendarAction INSTANCE = new PutCalendarAction();
    public static final String NAME = "cluster:admin/xpack/ml/calendars/put";

    private PutCalendarAction() {
        super(NAME);
    }

    @Override
    public RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RequestBuilder(client);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends ActionRequest implements ToXContentObject {

        public static Request parseRequest(String calendarId, XContentParser parser) {
            Calendar.Builder builder = Calendar.PARSER.apply(parser, null);
            if (builder.getId() == null) {
                builder.setId(calendarId);
            } else if (!Strings.isNullOrEmpty(calendarId) && !calendarId.equals(builder.getId())) {
                // If we have both URI and body filter ID, they must be identical
                throw new IllegalArgumentException(Messages.getMessage(Messages.INCONSISTENT_ID, Calendar.ID.getPreferredName(),
                        builder.getId(), calendarId));
            }
            return new Request(builder.build());
        }

        private Calendar calendar;

        Request() {

        }

        public Request(Calendar calendar) {
            this.calendar = ExceptionsHelper.requireNonNull(calendar, "calendar");
        }

        public Calendar getCalendar() {
            return calendar;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if ("_all".equals(calendar.getId())) {
                validationException =
                        addValidationError("Cannot create a Calendar with the reserved name [_all]",
                                validationException);
            }
            return validationException;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            calendar = new Calendar(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            calendar.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            calendar.toXContent(builder, params);
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(calendar);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(calendar, other.calendar);
        }
    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client) {
            super(client, INSTANCE, new Request());
        }
    }

    public static class Response extends AcknowledgedResponse implements ToXContentObject {

        private Calendar calendar;

        Response() {
        }

        public Response(Calendar calendar) {
            super(true);
            this.calendar = calendar;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            readAcknowledged(in);
            calendar = new Calendar(in);

        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            writeAcknowledged(out);
            calendar.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return calendar.toXContent(builder, params);
        }

        @Override
        public int hashCode() {
            return Objects.hash(isAcknowledged(), calendar);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Response other = (Response) obj;
            return Objects.equals(isAcknowledged(), other.isAcknowledged()) && Objects.equals(calendar, other.calendar);
        }
    }

    public static class TransportAction extends HandledTransportAction<Request, Response> {

        private final Client client;
        private final ClusterService clusterService;

        @Inject
        public TransportAction(Settings settings, ThreadPool threadPool,
                               TransportService transportService, ActionFilters actionFilters,
                               IndexNameExpressionResolver indexNameExpressionResolver,
                               Client client, ClusterService clusterService) {
            super(settings, NAME, threadPool, transportService, actionFilters,
                    indexNameExpressionResolver, Request::new);
            this.client = client;
            this.clusterService = clusterService;
        }

        @Override
        protected void doExecute(Request request, ActionListener<Response> listener) {
            Calendar calendar = request.getCalendar();

            checkJobsExist(calendar.getJobIds(), listener::onFailure);

            IndexRequest indexRequest = new IndexRequest(MlMetaIndex.INDEX_NAME, MlMetaIndex.TYPE, calendar.documentId());
            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                indexRequest.source(calendar.toXContent(builder,
                        new ToXContent.MapParams(Collections.singletonMap(MlMetaIndex.INCLUDE_TYPE_KEY, "true"))));
            } catch (IOException e) {
                throw new IllegalStateException("Failed to serialise calendar with id [" + calendar.getId() + "]", e);
            }

            // Make it an error to overwrite an existing calendar
            indexRequest.opType(DocWriteRequest.OpType.CREATE);
            indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

            executeAsyncWithOrigin(client, ML_ORIGIN, IndexAction.INSTANCE, indexRequest,
                    new ActionListener<IndexResponse>() {
                        @Override
                        public void onResponse(IndexResponse indexResponse) {
                            listener.onResponse(new Response(calendar));
                        }

                        @Override
                        public void onFailure(Exception e) {
                            listener.onFailure(
                                    ExceptionsHelper.serverError("Error putting calendar with id [" + calendar.getId() + "]", e));
                        }
                    });
        }

        private void checkJobsExist(List<String> jobIds, Consumer<Exception> errorHandler) {
            ClusterState state = clusterService.state();
            MlMetadata mlMetadata = state.getMetaData().custom(MlMetadata.TYPE);
            for (String jobId: jobIds) {
                Set<String> jobs = mlMetadata.expandJobIds(jobId, true);
                if (jobs.isEmpty()) {
                    errorHandler.accept(ExceptionsHelper.missingJobException(jobId));
                    return;
                }
            }
        }
    }    
}
