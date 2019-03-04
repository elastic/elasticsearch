/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.master.MasterNodeReadOperationRequestBuilder;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class ValidateDataFrameTransformAction extends Action<ValidateDataFrameTransformAction.Response> {

    public static final ValidateDataFrameTransformAction INSTANCE = new ValidateDataFrameTransformAction();
    public static final String NAME = "cluster:admin/data_frame/validate";

    private ValidateDataFrameTransformAction() {
        super(NAME);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends MasterNodeReadRequest<Request> {

        private DataFrameTransformConfig config;

        public Request(DataFrameTransformConfig config) {
            this.setConfig(config);
        }

        public Request() { }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.config = new DataFrameTransformConfig(in);
        }

        public static Request fromXContent(final XContentParser parser) throws IOException {
            return new Request(DataFrameTransformConfig.fromXContent(parser, null, false));
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public DataFrameTransformConfig getConfig() {
            return config;
        }

        public void setConfig(DataFrameTransformConfig config) {
            this.config = config;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
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
            if (obj == this) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(config, other.config);
        }
    }

    public static class RequestBuilder extends MasterNodeReadOperationRequestBuilder<Request, Response, RequestBuilder> {

        protected RequestBuilder(ElasticsearchClient client, ValidateDataFrameTransformAction action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private List<String> warnings;

        public Response() {
        }

        public Response(Set<String> warnings) {
            this.warnings = Collections.unmodifiableList(new ArrayList<>(warnings));
        }

        public Response(List<String> warnings) {
            this.warnings = Collections.unmodifiableList(new ArrayList<>(warnings));
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            warnings = Collections.unmodifiableList(in.readStringList());
        }

        public List<String> getWarnings() {
            return warnings;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringCollection(warnings);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("warnings", warnings);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(warnings);
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
            return Objects.equals(warnings, other.warnings);
        }
    }

}
