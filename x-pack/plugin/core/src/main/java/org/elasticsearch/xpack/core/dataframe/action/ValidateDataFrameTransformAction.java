/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeReadOperationRequestBuilder;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;

import java.io.IOException;
import java.util.Objects;

public class ValidateDataFrameTransformAction extends Action<AcknowledgedResponse> {

    public static final ValidateDataFrameTransformAction INSTANCE = new ValidateDataFrameTransformAction();
    public static final String NAME = "cluster:admin/data_frame/validate";

    private ValidateDataFrameTransformAction() {
        super(NAME);
    }

    @Override
    public AcknowledgedResponse newResponse() {
        return new AcknowledgedResponse();
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

    public static class RequestBuilder extends MasterNodeReadOperationRequestBuilder<Request, AcknowledgedResponse, RequestBuilder> {

        protected RequestBuilder(ElasticsearchClient client, ValidateDataFrameTransformAction action) {
            super(client, action, new Request());
        }
    }

}
