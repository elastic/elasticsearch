/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;

/**
 * A request to get cluster level stats.
 * This request can be used both to request stats from single cluster or from remote cluster.
 */
public class ClusterStatsRequest extends BaseNodesRequest<ClusterStatsRequest> {
    /**
     * Should the remote cluster stats be included in the response.
     */
    private final boolean doRemotes;

    /**
     * Get stats from nodes based on the nodes ids specified. If none are passed, stats
     * based on all nodes will be returned.
     */
    public ClusterStatsRequest(String... nodesIds) {
        this(false, nodesIds);
    }

    public ClusterStatsRequest(boolean doRemotes, String... nodesIds) {
        super(nodesIds);
        this.doRemotes = doRemotes;
    }

    public ClusterStatsRequest(StreamInput in) throws IOException {
        super(in.readStringArray());
        // We will never ask the remote to collect remote stats
        doRemotes = false;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, "", parentTaskId, headers);
    }

    /**
     * Should the remote cluster stats be included in the response.
     */
    public boolean doRemotes() {
        return doRemotes;
    }

    public ClusterStatsRequest subRequest() {
        return new ClusterStatsRequest(false, nodesIds());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringArrayNullable(nodesIds());
        // We will never ask remote to collect remote stats
    }

}
