/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasources.dataset;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.ResolvedIndexExpressions;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.local.LocalClusterStateRequest;
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.esql.EsqlDatasetActionNames;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.support.IndicesOptions.ConcreteTargetOptions.ERROR_WHEN_UNAVAILABLE_TARGETS;

/** Get one or more datasets by name (supports wildcards via {@code IndexNameExpressionResolver}). */
public class GetDatasetAction extends ActionType<GetDatasetAction.Response> {

    public static final GetDatasetAction INSTANCE = new GetDatasetAction();
    public static final String NAME = EsqlDatasetActionNames.ESQL_GET_DATASET_ACTION_NAME;

    private static final IndicesOptions DATASET_INDICES_OPTIONS = IndicesOptions.builder()
        .indexAbstractionOptions(IndicesOptions.IndexAbstractionOptions.builder().resolveDatasets(true).build())
        .concreteTargetOptions(ERROR_WHEN_UNAVAILABLE_TARGETS)
        .build();

    private GetDatasetAction() {
        super(NAME);
    }

    public static class Request extends LocalClusterStateRequest implements IndicesRequest.Replaceable {
        private String[] indices;
        private ResolvedIndexExpressions resolvedIndexExpressions;

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
            return DATASET_INDICES_OPTIONS;
        }

        @Override
        public IndicesRequest indices(String... indices) {
            this.indices = indices;
            return this;
        }

        @Override
        public void setResolvedIndexExpressions(ResolvedIndexExpressions expressions) {
            this.resolvedIndexExpressions = expressions;
        }

        @Override
        public ResolvedIndexExpressions getResolvedIndexExpressions() {
            return this.resolvedIndexExpressions;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Arrays.equals(indices, request.indices);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(indices);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final Collection<Dataset> datasets;

        public Response(Collection<Dataset> datasets) {
            Objects.requireNonNull(datasets, "datasets cannot be null");
            this.datasets = datasets;
        }

        public Collection<Dataset> getDatasets() {
            return datasets;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            TransportAction.localOnly();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startArray("datasets");
            for (Dataset ds : datasets) {
                ds.toXContent(builder, params);
            }
            builder.endArray();
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o instanceof Response response) {
                return this.datasets.equals(response.datasets);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return datasets.hashCode();
        }

        @Override
        public String toString() {
            return "GetDatasetAction.Response" + Arrays.toString(datasets.stream().map(Dataset::name).toArray(String[]::new));
        }
    }
}
