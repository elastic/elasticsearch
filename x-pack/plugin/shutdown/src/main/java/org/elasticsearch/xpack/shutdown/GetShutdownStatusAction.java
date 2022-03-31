/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class GetShutdownStatusAction extends ActionType<GetShutdownStatusAction.Response> {

    public static final GetShutdownStatusAction INSTANCE = new GetShutdownStatusAction();
    public static final String NAME = "cluster:admin/shutdown/get";

    public GetShutdownStatusAction() {
        super(NAME, Response::new);
    }

    public static class Request extends MasterNodeRequest<Request> {

        private final String[] nodeIds;

        public Request(String... nodeIds) {
            this.nodeIds = nodeIds;
        }

        public static Request readFrom(StreamInput in) throws IOException {
            return new Request(in.readStringArray());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringArray(this.nodeIds);
        }

        public String[] getNodeIds() {
            return nodeIds;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if ((o instanceof Request) == false) return false;
            Request request = (Request) o;
            return Arrays.equals(nodeIds, request.nodeIds);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(nodeIds);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {
        final List<SingleNodeShutdownStatus> shutdownStatuses;

        public Response(List<SingleNodeShutdownStatus> shutdownStatuses) {
            this.shutdownStatuses = Objects.requireNonNull(shutdownStatuses, "shutdown statuses must not be null");
        }

        public Response(StreamInput in) throws IOException {
            this.shutdownStatuses = in.readList(SingleNodeShutdownStatus::new);
        }

        public List<SingleNodeShutdownStatus> getShutdownStatuses() {
            return shutdownStatuses;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.startArray("nodes");
                for (SingleNodeShutdownStatus nodeShutdownStatus : shutdownStatuses) {
                    nodeShutdownStatus.toXContent(builder, params);
                }
                builder.endArray();
            }
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeList(shutdownStatuses);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if ((o instanceof Response) == false) return false;
            Response response = (Response) o;
            return shutdownStatuses.equals(response.shutdownStatuses);
        }

        @Override
        public int hashCode() {
            return Objects.hash(shutdownStatuses);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }

}
