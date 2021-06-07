/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;

import java.io.IOException;
import java.util.Objects;

public class PutDatafeedAction extends ActionType<PutDatafeedAction.Response> {

    public static final PutDatafeedAction INSTANCE = new PutDatafeedAction();
    public static final String NAME = "cluster:admin/xpack/ml/datafeeds/put";

    private PutDatafeedAction() {
        super(NAME, Response::new);
    }

    public static class Request extends AcknowledgedRequest<Request> implements ToXContentObject {

        public static Request parseRequest(String datafeedId, IndicesOptions indicesOptions, XContentParser parser) {
            DatafeedConfig.Builder datafeed = DatafeedConfig.STRICT_PARSER.apply(parser, null);
            if (datafeed.getIndicesOptions() == null) {
                datafeed.setIndicesOptions(indicesOptions);
            }
            datafeed.setId(datafeedId);
            return new Request(datafeed.build());
        }

        private DatafeedConfig datafeed;

        public Request(DatafeedConfig datafeed) {
            this.datafeed = datafeed;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            datafeed = new DatafeedConfig(in);
        }

        public DatafeedConfig getDatafeed() {
            return datafeed;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            datafeed.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            datafeed.toXContent(builder, params);
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(datafeed, request.datafeed);
        }

        @Override
        public int hashCode() {
            return Objects.hash(datafeed);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private DatafeedConfig datafeed;

        public Response(DatafeedConfig datafeed) {
            this.datafeed = datafeed;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            datafeed = new DatafeedConfig(in);
        }

        public DatafeedConfig getResponse() {
            return datafeed;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            datafeed.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            datafeed.toXContent(builder, params);
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(datafeed, response.datafeed);
        }

        @Override
        public int hashCode() {
            return Objects.hash(datafeed);
        }
    }

}
