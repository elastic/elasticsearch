/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;


public class DeleteFilterAction extends ActionType<AcknowledgedResponse> {

    public static final DeleteFilterAction INSTANCE = new DeleteFilterAction();
    public static final String NAME = "cluster:admin/xpack/ml/filters/delete";

    private DeleteFilterAction() {
        super(NAME, AcknowledgedResponse::new);
    }

    public static class Request extends AcknowledgedRequest<Request> {

        public static final ParseField FILTER_ID = new ParseField("filter_id");

        private String filterId;

        public Request(StreamInput in) throws IOException {
            super(in);
            filterId = in.readString();
        }

        public Request() {

        }

        public Request(String filterId) {
            this.filterId = ExceptionsHelper.requireNonNull(filterId, FILTER_ID.getPreferredName());
        }

        public String getFilterId() {
            return filterId;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(filterId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(filterId);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(filterId, other.filterId);
        }
    }

    public static class RequestBuilder extends MasterNodeOperationRequestBuilder<Request, AcknowledgedResponse, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client, DeleteFilterAction action) {
            super(client, action, new Request());
        }
    }
}

