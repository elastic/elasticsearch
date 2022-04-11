/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.coordination;

import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;

/**
 * A request to get information about cluster formation from a list of nodes (it's usually the master-eligible nodes that have this
 * information).
 */
public class ClusterFormationInfoRequest extends BaseNodesRequest<ClusterFormationInfoRequest> {

    public ClusterFormationInfoRequest(StreamInput in) throws IOException {
        super(in);
    }

    /**
     * Get cluster formation info from the specified nodes.
     */
    public ClusterFormationInfoRequest(String... nodeIds) {
        super(nodeIds);
        if (nodeIds == null || nodeIds.length == 0) {
            throw new IllegalArgumentException("Target nodes must be specified");
        }
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, "", parentTaskId, headers);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

}
