/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
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
import org.elasticsearch.xpack.ml.job.config.MlFilter;
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.watcher.watch.Payload;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;


public class PutFilterAction extends Action<PutFilterAction.Request, PutFilterAction.Response, PutFilterAction.RequestBuilder> {

    public static final PutFilterAction INSTANCE = new PutFilterAction();
    public static final String NAME = "cluster:admin/xpack/ml/filters/put";

    private PutFilterAction() {
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

        public static Request parseRequest(String filterId, XContentParser parser) {
            MlFilter.Builder filter = MlFilter.PARSER.apply(parser, null);
            if (filter.getId() == null) {
                filter.setId(filterId);
            } else if (!Strings.isNullOrEmpty(filterId) && !filterId.equals(filter.getId())) {
                // If we have both URI and body filter ID, they must be identical
                throw new IllegalArgumentException(Messages.getMessage(Messages.INCONSISTENT_ID, MlFilter.ID.getPreferredName(),
                        filter.getId(), filterId));
            }
            return new Request(filter.build());
        }

        private MlFilter filter;

        Request() {

        }

        public Request(MlFilter filter) {
            this.filter = ExceptionsHelper.requireNonNull(filter, "filter");
        }

        public MlFilter getFilter() {
            return this.filter;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            filter = new MlFilter(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            filter.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            filter.toXContent(builder, params);
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(filter);
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
            return Objects.equals(filter, other.filter);
        }
    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client) {
            super(client, INSTANCE, new Request());
        }
    }

    public static class Response extends AcknowledgedResponse {

        public Response() {
            super(true);
        }

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

    public static class TransportAction extends HandledTransportAction<Request, Response> {

        private final TransportBulkAction transportBulkAction;

        @Inject
        public TransportAction(Settings settings, ThreadPool threadPool,
                TransportService transportService, ActionFilters actionFilters,
                IndexNameExpressionResolver indexNameExpressionResolver,
                TransportBulkAction transportBulkAction) {
            super(settings, NAME, threadPool, transportService, actionFilters,
                    indexNameExpressionResolver, Request::new);
            this.transportBulkAction = transportBulkAction;
        }

        @Override
        protected void doExecute(Request request, ActionListener<Response> listener) {
            MlFilter filter = request.getFilter();
            IndexRequest indexRequest = new IndexRequest(MlMetaIndex.INDEX_NAME, MlMetaIndex.TYPE, filter.documentId());
            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                Payload.XContent.MapParams params = new ToXContent.MapParams(Collections.singletonMap(MlFilter.INCLUDE_TYPE_KEY, "true"));
                indexRequest.source(filter.toXContent(builder, params));
            } catch (IOException e) {
                throw new IllegalStateException("Failed to serialise filter with id [" + filter.getId() + "]", e);
            }
            BulkRequest bulkRequest = new BulkRequest().add(indexRequest);
            bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

            transportBulkAction.execute(bulkRequest, new ActionListener<BulkResponse>() {
                @Override
                public void onResponse(BulkResponse indexResponse) {
                    listener.onResponse(new Response());
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(new ResourceNotFoundException("Could not create filter with ID [" + filter.getId() + "]", e));
                }
            });
        }
    }
}

