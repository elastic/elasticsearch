/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.local.LocalClusterStateRequest;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class GetViewAction extends ActionType<GetViewAction.Response> {

    public static final GetViewAction INSTANCE = new GetViewAction();
    public static final String NAME = "cluster:admin/xpack/esql/view/get";

    private GetViewAction() {
        super(NAME);
    }

    public static class Request extends LocalClusterStateRequest {
        private final List<String> names;

        public Request(TimeValue masterNodeTimeout, String... names) {
            super(masterNodeTimeout);
            this.names = List.of(names);
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers);
        }

        public List<String> names() {
            return names;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(names, request.names);
        }

        @Override
        public int hashCode() {
            return Objects.hash(names);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final List<View> views;

        public Response(List<View> views) {
            Objects.requireNonNull(views, "views cannot be null");
            this.views = Collections.unmodifiableList(views);
        }

        public List<View> getViews() {
            return views;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            TransportAction.localOnly();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            var v = ChunkedToXContentHelper.array("views", views.iterator());
            while (v.hasNext()) {
                v.next().toXContent(builder, params);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            return views.equals(((Response) o).views);
        }

        @Override
        public int hashCode() {
            return views.hashCode();
        }

        @Override
        public String toString() {
            return "GetViewAction.Response" + views.stream().map(View::name).toList();
        }
    }
}
