/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.state;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;

/**
 * A remote-only version of {@link ClusterStateRequest} that should be used for cross-cluster requests.
 * It simply exists to handle incoming remote requests and forward them to the local transport action.
 */
public class RemoteClusterStateRequest extends ActionRequest {

    private final ClusterStateRequest clusterStateRequest;

    public RemoteClusterStateRequest(ClusterStateRequest clusterStateRequest) {
        this.clusterStateRequest = clusterStateRequest;
    }

    public RemoteClusterStateRequest(StreamInput in) throws IOException {
        this.clusterStateRequest = new ClusterStateRequest(in);
    }

    @Override
    public ActionRequestValidationException validate() {
        return clusterStateRequest.validate();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        clusterStateRequest.getParentTask().writeTo(out);
        out.writeTimeValue(clusterStateRequest.masterTimeout());
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
            out.writeVLong(0L); // Master term
        } // else no protection against routing loops in older versions
        out.writeBoolean(true); // Local
        out.writeBoolean(clusterStateRequest.routingTable());
        out.writeBoolean(clusterStateRequest.nodes());
        out.writeBoolean(clusterStateRequest.metadata());
        out.writeBoolean(clusterStateRequest.blocks());
        out.writeBoolean(clusterStateRequest.customs());
        out.writeStringArray(clusterStateRequest.indices());
        clusterStateRequest.indicesOptions().writeIndicesOptions(out);
        out.writeTimeValue(clusterStateRequest.waitForTimeout());
        out.writeOptionalLong(clusterStateRequest.waitForMetadataVersion());
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers);
    }

    @Override
    public String getDescription() {
        return clusterStateRequest.getDescription();
    }

    public ClusterStateRequest clusterStateRequest() {
        return clusterStateRequest;
    }
}
