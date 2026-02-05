/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
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
import org.elasticsearch.xpack.core.esql.EsqlViewActionNames;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.support.IndicesOptions.ConcreteTargetOptions.ERROR_WHEN_UNAVAILABLE_TARGETS;

public class GetViewAction extends ActionType<GetViewAction.Response> {

    public static final GetViewAction INSTANCE = new GetViewAction();
    public static final String NAME = EsqlViewActionNames.ESQL_GET_VIEW_ACTION_NAME;

    private static final IndicesOptions VIEW_INDICES_OPTIONS = IndicesOptions.builder()
        .wildcardOptions(IndicesOptions.WildcardOptions.builder().resolveViews(true))
        .concreteTargetOptions(ERROR_WHEN_UNAVAILABLE_TARGETS)
        .build();

    private GetViewAction() {
        super(NAME);
    }

    public static class Request extends LocalClusterStateRequest implements IndicesRequest.Replaceable {
        private String[] indices;

        public Request(TimeValue masterNodeTimeout) {
            super(masterNodeTimeout);
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers);
        }

        @Override
        public String[] indices() {
            return indices;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return VIEW_INDICES_OPTIONS;
        }

        @Override
        public IndicesRequest indices(String... indices) {
            this.indices = indices;
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Arrays.equals(this.indices, request.indices);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(indices);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final Collection<View> views;

        public Response(Collection<View> views) {
            Objects.requireNonNull(views, "views cannot be null");
            this.views = views;
        }

        public Collection<View> getViews() {
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
            if (o instanceof GetViewAction.Response response) {
                return this.views.equals(response.views);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return views.hashCode();
        }

        @Override
        public String toString() {
            return "GetViewAction.Response" + Arrays.toString(views.stream().map(View::name).toArray(String[]::new));
        }
    }
}
