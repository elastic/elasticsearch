/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class DeleteAutoFollowPatternAction
    extends Action<DeleteAutoFollowPatternAction.Request, AcknowledgedResponse, DeleteAutoFollowPatternAction.RequestBuilder> {

    public static final String NAME = "cluster:admin/xpack/ccr/auto_follow_pattern/delete";
    public static final DeleteAutoFollowPatternAction INSTANCE = new DeleteAutoFollowPatternAction();

    private DeleteAutoFollowPatternAction() {
        super(NAME);
    }

    @Override
    public AcknowledgedResponse newResponse() {
        return new AcknowledgedResponse();
    }

    @Override
    public RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RequestBuilder(client);
    }

    public static class Request extends AcknowledgedRequest<Request> {

        private String leaderClusterAlias;

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (leaderClusterAlias == null) {
                validationException = addValidationError("leaderClusterAlias is missing", validationException);
            }
            return validationException;
        }

        public String getLeaderClusterAlias() {
            return leaderClusterAlias;
        }

        public void setLeaderClusterAlias(String leaderClusterAlias) {
            this.leaderClusterAlias = leaderClusterAlias;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            leaderClusterAlias = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(leaderClusterAlias);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(leaderClusterAlias, request.leaderClusterAlias);
        }

        @Override
        public int hashCode() {
            return Objects.hash(leaderClusterAlias);
        }
    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, AcknowledgedResponse, RequestBuilder> {

        RequestBuilder(ElasticsearchClient client) {
            super(client, INSTANCE, new Request());
        }
    }

}
