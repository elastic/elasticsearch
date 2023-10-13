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
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "", parentTaskId, headers);
        }
    }

    public static class Response extends ActionResponse implements ChunkedToXContentObject {
        final List<SingleNodeShutdownStatus> shutdownStatuses;

        public Response(List<SingleNodeShutdownStatus> shutdownStatuses) {
            this.shutdownStatuses = Objects.requireNonNull(shutdownStatuses, "shutdown statuses must not be null");
        }

        public Response(StreamInput in) throws IOException {
            this.shutdownStatuses = in.readCollectionAsList(SingleNodeShutdownStatus::new);
        }

        public List<SingleNodeShutdownStatus> getShutdownStatuses() {
            return shutdownStatuses;
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return Iterators.concat(
                ChunkedToXContentHelper.startObject(),
                ChunkedToXContentHelper.startArray("nodes"),
                Iterators.flatMap(shutdownStatuses.iterator(), status -> status.toXContentChunked(params)),
                ChunkedToXContentHelper.endArray(),
                ChunkedToXContentHelper.endObject()
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(shutdownStatuses);
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
