/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.featureindexbuilder.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.featureindexbuilder.job.DataFrameJobConfig;

import java.io.IOException;
import java.util.Objects;

public class PutDataFrameJobAction extends Action<PutDataFrameJobAction.Response> {

    public static final PutDataFrameJobAction INSTANCE = new PutDataFrameJobAction();
    public static final String NAME = "cluster:admin/data_frame/put";

    private PutDataFrameJobAction() {
        super(NAME);
    }
    
    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends AcknowledgedRequest<Request> implements ToXContentObject {

        private DataFrameJobConfig config;

        public Request(DataFrameJobConfig config) {
            this.setConfig(config);
        }

        public Request() {

        }

        public static Request fromXContent(final XContentParser parser, final String id) throws IOException {
            return new Request(DataFrameJobConfig.fromXContent(parser, id));
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return this.config.toXContent(builder, params);
        }

        public DataFrameJobConfig getConfig() {
            return config;
        }

        public void setConfig(DataFrameJobConfig config) {
            this.config = config;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            this.config = new DataFrameJobConfig(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            this.config.writeTo(out);
        }

        @Override
        public int hashCode() {
            return Objects.hash(config);
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
            return Objects.equals(config, other.config);
        }
    }

    public static class RequestBuilder extends MasterNodeOperationRequestBuilder<Request, Response, RequestBuilder> {

        protected RequestBuilder(ElasticsearchClient client, PutDataFrameJobAction action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends AcknowledgedResponse {
        public Response() {
            super();
        }

        public Response(boolean acknowledged) {
            super(acknowledged);
        }
    }
}
