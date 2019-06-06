/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.reindex;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

public class PutReindexJobAction extends Action<AcknowledgedResponse> {

    public static final PutReindexJobAction INSTANCE = new PutReindexJobAction();
    public static final String NAME = "cluster:admin/xpack/rollup/put";

    private PutReindexJobAction() {
        super(NAME);
    }

    @Override
    public AcknowledgedResponse newResponse() {
        return new AcknowledgedResponse();
    }

    public static class Request extends AcknowledgedRequest<Request> implements IndicesRequest, ToXContentObject {

        private IndicesOptions indicesOptions = IndicesOptions.fromOptions(false, false, true, false);

        public Request() {

        }

        public static Request fromXContent(final XContentParser parser, final String id) throws IOException {
            return new Request();
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder;
        }

        @Override
        public String[] indices() {
            return new String[]{""};
        }

        @Override
        public IndicesOptions indicesOptions() {
            return indicesOptions;
        }

//        @Override
//        public int hashCode() {
//            return Objects.hash(config);
//        }
//
//        @Override
//        public boolean equals(Object obj) {
//            if (obj == null) {
//                return false;
//            }
//
//            return getClass() == obj.getClass();
//        }
    }

    public static class RequestBuilder extends MasterNodeOperationRequestBuilder<Request, AcknowledgedResponse, RequestBuilder> {

        protected RequestBuilder(ElasticsearchClient client, PutReindexJobAction action) {
            super(client, action, new Request());
        }
    }
}
