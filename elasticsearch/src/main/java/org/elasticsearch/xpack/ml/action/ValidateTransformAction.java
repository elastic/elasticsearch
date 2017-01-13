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
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.job.transform.TransformConfig;
import org.elasticsearch.xpack.ml.job.transform.verification.TransformConfigVerifier;

import java.io.IOException;
import java.util.Objects;

public class ValidateTransformAction
extends Action<ValidateTransformAction.Request, ValidateTransformAction.Response, ValidateTransformAction.RequestBuilder> {

    public static final ValidateTransformAction INSTANCE = new ValidateTransformAction();
    public static final String NAME = "cluster:admin/ml/validate/transform";

    protected ValidateTransformAction() {
        super(NAME);
    }

    @Override
    public RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RequestBuilder(client, INSTANCE);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder> {

        protected RequestBuilder(ElasticsearchClient client, ValidateTransformAction action) {
            super(client, action, new Request());
        }

    }

    public static class Request extends ActionRequest implements ToXContent {

        private TransformConfig transform;

        public static Request parseRequest(XContentParser parser) {
            TransformConfig transform = TransformConfig.PARSER.apply(parser, null);
            return new Request(transform);
        }

        Request() {
            this.transform = null;
        }

        public Request(TransformConfig transform) {
            this.transform = transform;
        }

        public TransformConfig getTransform() {
            return transform;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            transform.writeTo(out);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            transform = new TransformConfig(in);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            transform.toXContent(builder, params);
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(transform);
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
            return Objects.equals(transform, other.transform);
        }

    }

    public static class Response extends AcknowledgedResponse {

        public Response() {
            super();
        }

        public Response(boolean acknowledged) {
            super(acknowledged);
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

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
            super(settings, ValidateTransformAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver,
                    Request::new);
        }

        @Override
        protected void doExecute(Request request, ActionListener<Response> listener) {
            TransformConfigVerifier.verify(request.getTransform());
            listener.onResponse(new Response(true));
        }

    }
}
