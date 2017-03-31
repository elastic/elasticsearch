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
import org.elasticsearch.xpack.ml.job.config.Job;

import java.io.IOException;
import java.util.Date;
import java.util.Objects;

public class ValidateJobConfigAction
extends Action<ValidateJobConfigAction.Request, ValidateJobConfigAction.Response, ValidateJobConfigAction.RequestBuilder> {

    public static final ValidateJobConfigAction INSTANCE = new ValidateJobConfigAction();
    public static final String NAME = "cluster:admin/xpack/ml/job/validate";

    protected ValidateJobConfigAction() {
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

        protected RequestBuilder(ElasticsearchClient client, ValidateJobConfigAction action) {
            super(client, action, new Request());
        }

    }

    public static class Request extends ActionRequest implements ToXContent {

        private Job job;

        public static Request parseRequest(XContentParser parser) {
            Job.Builder job = Job.PARSER.apply(parser, null);
            // When jobs are PUT their ID must be supplied in the URL - assume this will
            // be valid unless an invalid job ID is specified in the JSON to be validated
            job.setId(job.getId() != null ? job.getId() : "ok");
            if (job.getCreateTime() == null) {
                job.setCreateTime(new Date());
            }
            return new Request(job.build());
        }

        Request() {
            this.job = null;
        }

        public Request(Job job) {
            this.job = job;
        }

        public Job getJob() {
            return job;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            job.writeTo(out);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            job = new Job(in);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            job.toXContent(builder, params);
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(job);
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
            return Objects.equals(job, other.job);
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
            super(settings, ValidateJobConfigAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver,
                    Request::new);
        }

        @Override
        protected void doExecute(Request request, ActionListener<Response> listener) {
            listener.onResponse(new Response(true));
        }

    }
}
