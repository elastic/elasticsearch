/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class DeleteDataFrameAnalyticsAction extends ActionType<AcknowledgedResponse> {

    public static final DeleteDataFrameAnalyticsAction INSTANCE = new DeleteDataFrameAnalyticsAction();
    public static final String NAME = "cluster:admin/xpack/ml/data_frame/analytics/delete";

    private DeleteDataFrameAnalyticsAction() {
        super(NAME, AcknowledgedResponse::new);
    }

    public static class Request extends AcknowledgedRequest<Request> {

        public static final ParseField FORCE = new ParseField("force");

        private String id;
        private boolean force;

        public Request(StreamInput in) throws IOException {
            super(in);
            id = in.readString();
            if (in.getVersion().onOrAfter(Version.V_7_6_0)) {
                force = in.readBoolean();
            } else {
                force = false;
            }
        }

        public Request() {}

        public Request(String id) {
            this.id = ExceptionsHelper.requireNonNull(id, DataFrameAnalyticsConfig.ID);
        }

        public String getId() {
            return id;
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
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DeleteDataFrameAnalyticsAction.Request request = (DeleteDataFrameAnalyticsAction.Request) o;
            return Objects.equals(id, request.id) && force == request.force;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(id);
            if (out.getVersion().onOrAfter(Version.V_7_6_0)) {
                out.writeBoolean(force);
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, force);
        }
    }

    public static class RequestBuilder extends MasterNodeOperationRequestBuilder<Request, AcknowledgedResponse, RequestBuilder> {

        protected RequestBuilder(ElasticsearchClient client, DeleteDataFrameAnalyticsAction action) {
            super(client, action, new Request());
        }
    }
}
