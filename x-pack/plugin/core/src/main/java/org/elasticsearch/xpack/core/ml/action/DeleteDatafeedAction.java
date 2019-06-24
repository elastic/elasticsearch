/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class DeleteDatafeedAction extends Action<AcknowledgedResponse> {

    public static final DeleteDatafeedAction INSTANCE = new DeleteDatafeedAction();
    public static final String NAME = "cluster:admin/xpack/ml/datafeeds/delete";

    private DeleteDatafeedAction() {
        super(NAME);
    }

    @Override
    public AcknowledgedResponse newResponse() {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    public Writeable.Reader<AcknowledgedResponse> getResponseReader() {
        return AcknowledgedResponse::new;
    }

    public static class Request extends AcknowledgedRequest<Request> implements ToXContentFragment {

        public static final ParseField FORCE = new ParseField("force");

        private String datafeedId;
        private boolean force;

        public Request(String datafeedId) {
            this.datafeedId = ExceptionsHelper.requireNonNull(datafeedId, DatafeedConfig.ID.getPreferredName());
        }

        public Request() {
        }

        public String getDatafeedId() {
            return datafeedId;
        }

        public boolean isForce() {
            return force;
        }

         public void setForce(boolean force) {
            this.force = force;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            datafeedId = in.readString();
            force = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(datafeedId);
            out.writeBoolean(force);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(DatafeedConfig.ID.getPreferredName(), datafeedId);
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request other = (Request) o;
            return Objects.equals(datafeedId, other.datafeedId) && Objects.equals(force, other.force);
        }

        @Override
        public int hashCode() {
            return Objects.hash(datafeedId, force);
        }
    }

    public static class RequestBuilder extends MasterNodeOperationRequestBuilder<Request, AcknowledgedResponse, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client, DeleteDatafeedAction action) {
            super(client, action, new Request());
        }
    }
}
