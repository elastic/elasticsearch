/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexlifecycle.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.master.info.ClusterInfoRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class ExplainLifecycleAction
        extends Action<ExplainLifecycleAction.Request, ExplainLifecycleAction.Response> {
    public static final ExplainLifecycleAction INSTANCE = new ExplainLifecycleAction();
    public static final String NAME = "indices:admin/xpack/index_lifecycle/explain";

    protected ExplainLifecycleAction() {
        super(NAME);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private List<IndexLifecycleExplainResponse> indexResponses;

        public Response() {
        }

        public Response(List<IndexLifecycleExplainResponse> indexResponses) {
            this.indexResponses = indexResponses;
        }

        public List<IndexLifecycleExplainResponse> getIndexResponses() {
            return indexResponses;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            for (IndexLifecycleExplainResponse indexResponse : indexResponses) {
                builder.field(indexResponse.getIndex(), indexResponse);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            indexResponses = in.readList(IndexLifecycleExplainResponse::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeList(indexResponses);
        }

        @Override
        public int hashCode() {
            return Objects.hash(indexResponses);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (obj.getClass() != getClass()) {
                return false;
            }
            Response other = (Response) obj;
            return Objects.equals(indexResponses, other.indexResponses);
        }

        @Override
        public String toString() {
            return Strings.toString(this, true, true);
        }

    }

    public static class Request extends ClusterInfoRequest<Request> {

        public Request() {
            super();
        }

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(indices()), indicesOptions());
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (obj.getClass() != getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.deepEquals(indices(), other.indices()) &&
                    Objects.equals(indicesOptions(), other.indicesOptions());
        }

        @Override
        public String toString() {
            return "Request [indices()=" + Arrays.toString(indices()) + ", indicesOptions()=" + indicesOptions() + "]";
        }

    }

}
