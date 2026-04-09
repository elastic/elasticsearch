/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.util.Map;

/**
 * A request to get cluster level stats.
 */
public class ClusterStatsRequest extends BaseNodesRequest {
    /**
     * Should the remote cluster stats be included in the response.
     */
    private final boolean doRemotes;
    /**
     * Return stripped down stats for remote clusters.
     */
    private final boolean remoteStats;
    /**
     * Are we dealing with CPS environment?
     * In CPS, we do not display MRT stats.
     */
    private final boolean isCPS;

    /**
     * Get stats from nodes based on the nodes ids specified. If none are passed, stats
     * based on all nodes will be returned.
     */
    public ClusterStatsRequest(String... nodesIds) {
        this(false, false, nodesIds);
    }

    public ClusterStatsRequest(boolean doRemotes, String... nodesIds) {
        this(doRemotes, false, nodesIds);
    }

    public ClusterStatsRequest(boolean doRemotes, boolean isCPS, String... nodesIds) {
        super(nodesIds);
        this.doRemotes = doRemotes;
        this.remoteStats = false;
        this.isCPS = isCPS;
    }

    public static ClusterStatsRequest newServerlessRequest(String[] nodeIds) {
        return new ClusterStatsRequest(false, true, nodeIds);
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, "", parentTaskId, headers);
    }

    public static ClusterStatsRequest newRemoteClusterStatsRequest() {
        return new ClusterStatsRequest(false, false);
    }

    /**
     * Should the remote cluster stats be included in the response.
     */
    public boolean doRemotes() {
        return doRemotes;
    }

    /**
     * Should the response be a stripped down version of the stats for remote clusters.
     */
    public boolean isRemoteStats() {
        return remoteStats;
    }

    public boolean isCPS() {
        return isCPS;
    }
}
