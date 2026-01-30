/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.action.RemoteClusterActionType;
import org.elasticsearch.action.ResolvedIndexExpressions;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import static org.elasticsearch.core.Strings.format;

/**
 * Transform internal API (no REST layer) to retrieve index checkpoints.
 */
public class GetCheckpointAction extends ActionType<GetCheckpointAction.Response> {

    static final TransportVersion CPS_SUPPORT = TransportVersion.fromName("transform_checkpoint_cps_support");

    public static final GetCheckpointAction INSTANCE = new GetCheckpointAction();

    // note: this is an index action and requires `monitor` or `view_index_metadata`
    public static final String NAME = "indices:monitor/transform/checkpoint";
    public static final RemoteClusterActionType<Response> REMOTE_TYPE = new RemoteClusterActionType<>(NAME, Response::new);

    private GetCheckpointAction() {
        super(NAME);
    }

    public static class Request extends LegacyActionRequest implements IndicesRequest.Replaceable {

        private String[] indices;
        private final IndicesOptions indicesOptions;
        private final QueryBuilder query;
        private final String cluster;
        private final TimeValue timeout;
        private final String projectRouting;
        private final Map<String, String> headers;
        private final boolean includeResolvedIndexExpressions;

        /**
         * ResolvedIndexExpressions is set in the Security Action Filter.
         */
        private ResolvedIndexExpressions resolvedIndexExpressions = null;

        public Request(StreamInput in) throws IOException {
            super(in);
            indices = in.readStringArray();
            indicesOptions = IndicesOptions.readIndicesOptions(in);
            query = in.readOptionalNamedWriteable(QueryBuilder.class);
            cluster = in.readOptionalString();
            timeout = in.readOptionalTimeValue();
            projectRouting = in.getTransportVersion().supports(CPS_SUPPORT) ? in.readOptionalString() : null;
            headers = in.getTransportVersion().supports(CPS_SUPPORT) ? in.readMap(StreamInput::readString) : Map.of();
            includeResolvedIndexExpressions = in.getTransportVersion().supports(CPS_SUPPORT) && in.readBoolean();
        }

        public Request(
            String[] indices,
            IndicesOptions indicesOptions,
            QueryBuilder query,
            String cluster,
            TimeValue timeout,
            String projectRouting,
            Map<String, String> headers,
            boolean includeResolvedIndexExpressions
        ) {
            this.indices = indices != null ? indices : Strings.EMPTY_ARRAY;
            this.indicesOptions = indicesOptions;
            this.query = query;
            this.cluster = cluster;
            this.timeout = timeout;
            this.projectRouting = projectRouting;
            this.headers = headers != null ? headers : Map.of();
            this.includeResolvedIndexExpressions = includeResolvedIndexExpressions;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public String[] indices() {
            return indices;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return indicesOptions;
        }

        public QueryBuilder getQuery() {
            return query;
        }

        public String getCluster() {
            return cluster;
        }

        public TimeValue getTimeout() {
            return timeout;
        }

        @Override
        public boolean allowsCrossProject() {
            return true;
        }

        @Override
        @Nullable
        public ResolvedIndexExpressions getResolvedIndexExpressions() {
            return resolvedIndexExpressions;
        }

        @Override
        public void setResolvedIndexExpressions(ResolvedIndexExpressions expressions) {
            this.resolvedIndexExpressions = expressions;
        }

        @Override
        @Nullable
        public String getProjectRouting() {
            return projectRouting;
        }

        public Map<String, String> headers() {
            return headers;
        }

        public boolean includeResolvedIndexExpressions() {
            return includeResolvedIndexExpressions;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj == null || obj.getClass() != getClass()) {
                return false;
            }
            Request that = (Request) obj;

            return Arrays.equals(indices, that.indices)
                && Objects.equals(indicesOptions, that.indicesOptions)
                && Objects.equals(query, that.query)
                && Objects.equals(cluster, that.cluster)
                && Objects.equals(timeout, that.timeout)
                && Objects.equals(projectRouting, that.projectRouting)
                && Objects.equals(headers, that.headers)
                && includeResolvedIndexExpressions == that.includeResolvedIndexExpressions;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                Arrays.hashCode(indices),
                indicesOptions,
                query,
                cluster,
                timeout,
                projectRouting,
                headers,
                includeResolvedIndexExpressions
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(indices);
            indicesOptions.writeIndicesOptions(out);
            out.writeOptionalNamedWriteable(query);
            out.writeOptionalString(cluster);
            out.writeOptionalTimeValue(timeout);
            if (out.getTransportVersion().supports(CPS_SUPPORT)) {
                out.writeOptionalString(projectRouting);
                out.writeMap(headers, StreamOutput::writeString);
                out.writeBoolean(includeResolvedIndexExpressions);
            }
        }

        @Override
        public IndicesRequest indices(String... indices) {
            this.indices = indices;
            return this;
        }

        @Override
        public boolean allowsRemoteIndices() {
            return true;
        }

        @Override
        public CancellableTask createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, format("get_checkpoint[%d]", indices.length), parentTaskId, headers);
        }

        public Request rewriteRequest(
            String[] indices,
            IndicesOptions indicesOptions,
            String cluster,
            boolean includeResolvedIndexExpressions
        ) {
            return new Request(
                indices,
                indicesOptions,
                getQuery(),
                cluster,
                getTimeout(),
                getProjectRouting(),
                headers(),
                includeResolvedIndexExpressions
            );
        }
    }

    public static class Response extends ActionResponse {

        private final Map<String, long[]> checkpoints;
        @Nullable
        private final ResolvedIndexExpressions resolvedIndexExpressions;

        public Response(Map<String, long[]> checkpoints, @Nullable ResolvedIndexExpressions resolvedIndexExpressions) {
            this.checkpoints = checkpoints;
            this.resolvedIndexExpressions = resolvedIndexExpressions;
        }

        public Response(StreamInput in) throws IOException {
            this.checkpoints = in.readOrderedMap(StreamInput::readString, StreamInput::readLongArray);
            this.resolvedIndexExpressions = in.getTransportVersion().supports(CPS_SUPPORT)
                ? in.readOptionalWriteable(ResolvedIndexExpressions::new)
                : null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(getCheckpoints(), StreamOutput::writeLongArray);
            if (out.getTransportVersion().supports(CPS_SUPPORT)) {
                out.writeOptionalWriteable(resolvedIndexExpressions);
            }
        }

        public Map<String, long[]> getCheckpoints() {
            return Collections.unmodifiableMap(checkpoints);
        }

        @Nullable
        public ResolvedIndexExpressions resolvedIndexExpressions() {
            return resolvedIndexExpressions;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (obj == null || obj.getClass() != getClass()) {
                return false;
            }
            Response that = (Response) obj;

            return this.checkpoints.size() == that.checkpoints.size()
                && this.checkpoints.entrySet().stream().allMatch(e -> Arrays.equals(e.getValue(), that.checkpoints.get(e.getKey())))
                && Objects.equals(resolvedIndexExpressions, that.resolvedIndexExpressions);
        }

        @Override
        public int hashCode() {
            int hash = Objects.hash(resolvedIndexExpressions);

            for (Entry<String, long[]> e : checkpoints.entrySet()) {
                hash = 31 * hash + Objects.hash(e.getKey(), Arrays.hashCode(e.getValue()));
            }

            return hash;
        }
    }
}
