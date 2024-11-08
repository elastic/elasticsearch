/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.resolve;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.RemoteClusterAware;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

public class ResolveClusterActionRequest extends ActionRequest implements IndicesRequest.Replaceable {

    public static final IndicesOptions DEFAULT_INDICES_OPTIONS = IndicesOptions.strictExpandOpen();

    private String[] names;
    /*
     * Tracks whether the user originally requested any local indices
     * Due to how the IndicesAndAliasResolver.resolveIndicesAndAliases method works, when a user
     * requests both a local index with a wildcard and remote index and the local index wildcard
     * matches an index for which the user lacks permission, when the local index info is lost
     * when the indices(String... indices) method is called to overwrite the
     * indices array.
     *
     * Example: request is _resolve/cluster/index1*,my_remote_cluster:index1
     * The indices array in the Request is originally set as [index1*, my_remote_cluster:index1]
     * but gets overridden to [my_remote_cluster:index1]
     * Since the user requested information about whether there are any matching indices on the local
     * cluster, we need to parse the original indices request to track whether to include the local
     * cluster in the response.
     */
    private boolean localIndicesRequested = false;
    private IndicesOptions indicesOptions;

    public ResolveClusterActionRequest(String[] names) {
        this(names, DEFAULT_INDICES_OPTIONS);
    }

    @SuppressWarnings("this-escape")
    public ResolveClusterActionRequest(String[] names, IndicesOptions indicesOptions) {
        this.names = names;
        this.localIndicesRequested = localIndicesPresent(names);
        this.indicesOptions = indicesOptions;
    }

    @SuppressWarnings("this-escape")
    public ResolveClusterActionRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getTransportVersion().before(TransportVersions.V_8_13_0)) {
            throw new UnsupportedOperationException(
                "ResolveClusterAction requires at least version "
                    + TransportVersions.V_8_13_0.toReleaseVersion()
                    + " but was "
                    + in.getTransportVersion().toReleaseVersion()
            );
        }
        this.names = in.readStringArray();
        this.indicesOptions = IndicesOptions.readIndicesOptions(in);
        this.localIndicesRequested = localIndicesPresent(names);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getTransportVersion().before(TransportVersions.V_8_13_0)) {
            throw new UnsupportedOperationException(
                "ResolveClusterAction requires at least version "
                    + TransportVersions.V_8_13_0.toReleaseVersion()
                    + " but was "
                    + out.getTransportVersion().toReleaseVersion()
            );
        }
        out.writeStringArray(names);
        indicesOptions.writeIndicesOptions(out);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (names == null || names.length == 0) {
            validationException = ValidateActions.addValidationError("no index expressions specified", validationException);
        }
        return validationException;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ResolveClusterActionRequest request = (ResolveClusterActionRequest) o;
        return Arrays.equals(names, request.names) && indicesOptions.equals(request.indicesOptions());
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(indicesOptions);
        result = 31 * result + Arrays.hashCode(names);
        return result;
    }

    @Override
    public String[] indices() {
        return names;
    }

    public boolean isLocalIndicesRequested() {
        return localIndicesRequested;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    // for testing
    protected IndicesRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
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

    boolean localIndicesPresent(String[] indices) {
        for (String index : indices) {
            if (RemoteClusterAware.isRemoteIndexName(index) == false) {
                return true;
            }
        }
        return false;
    }
}
