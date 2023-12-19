/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.resolve;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public class ResolveClusterActionRequest extends ActionRequest implements IndicesRequest.Replaceable {

    // only allow querying against open, non-hidden indices
    public static final IndicesOptions DEFAULT_INDICES_OPTIONS = IndicesOptions.strictExpandOpen();

    private String[] names;
    private IndicesOptions indicesOptions = DEFAULT_INDICES_OPTIONS;

    public ResolveClusterActionRequest(String[] names) {
        this.names = names;
    }

    public ResolveClusterActionRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getTransportVersion().before(TransportVersions.RESOLVE_CLUSTER_ENDPOINT_ADDED)) {
            throw new UnsupportedOperationException(
                "ResolveClusterAction requires at least Transport Version "
                    + TransportVersions.RESOLVE_CLUSTER_ENDPOINT_ADDED
                    + " but was "
                    + in.getTransportVersion()
            );
        }
        this.names = in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getTransportVersion().before(TransportVersions.RESOLVE_CLUSTER_ENDPOINT_ADDED)) {
            throw new UnsupportedOperationException(
                "ResolveClusterAction requires at least Transport Version "
                    + TransportVersions.RESOLVE_CLUSTER_ENDPOINT_ADDED
                    + " but was "
                    + out.getTransportVersion()
            );
        }
        out.writeStringArray(names);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ResolveClusterActionRequest request = (ResolveClusterActionRequest) o;
        return Arrays.equals(names, request.names);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(names);
    }

    @Override
    public String[] indices() {
        return names;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    @Override
    public IndicesRequest indices(String... indices) {
        this.names = indices;
        return this;
    }

    @Override
    public boolean allowsRemoteIndices() {
        return true;
    }

    @Override
    public boolean includeDataStreams() {
        // request must allow data streams because the index name expression resolver for the action handler assumes it
        return true;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, "", parentTaskId, headers) {
            @Override
            public String getDescription() {
                return "resolve/cluster for " + Arrays.toString(indices());
            }
        };
    }
}
