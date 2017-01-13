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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.job.transform.TransformConfig;
import org.elasticsearch.xpack.ml.job.transform.verification.TransformConfigsVerifier;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class ValidateTransformsAction
extends Action<ValidateTransformsAction.Request, ValidateTransformsAction.Response, ValidateTransformsAction.RequestBuilder> {

    public static final ValidateTransformsAction INSTANCE = new ValidateTransformsAction();
    public static final String NAME = "cluster:admin/ml/validate/transforms";

    protected ValidateTransformsAction() {
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

        protected RequestBuilder(ElasticsearchClient client, ValidateTransformsAction action) {
            super(client, action, new Request());
        }

    }

    public static class Request extends ActionRequest implements ToXContent {

        public static final ParseField TRANSFORMS = new ParseField("transforms");

        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<Request, Void> PARSER = new ConstructingObjectParser<>(NAME,
                a -> new Request((List<TransformConfig>) a[0]));

        static {
            PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), TransformConfig.PARSER, TRANSFORMS);
        }

        private List<TransformConfig> transforms;

        Request() {
            this.transforms = null;
        }

        public Request(List<TransformConfig> transforms) {
            this.transforms = transforms;
        }

        public List<TransformConfig> getTransforms() {
            return transforms;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeList(transforms);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            transforms = in.readList(TransformConfig::new);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.array(TRANSFORMS.getPreferredName(), transforms.toArray(new Object[transforms.size()]));
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(transforms);
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
            return Objects.equals(transforms, other.transforms);
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
            super(settings, ValidateTransformsAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver,
                    Request::new);
        }

        @Override
        protected void doExecute(Request request, ActionListener<Response> listener) {
            TransformConfigsVerifier.verify(request.getTransforms());
            listener.onResponse(new Response(true));
        }

    }
}
