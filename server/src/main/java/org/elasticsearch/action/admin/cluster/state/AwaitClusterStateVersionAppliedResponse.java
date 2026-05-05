/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.state;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.ActionNotFoundTransportException;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class AwaitClusterStateVersionAppliedResponse extends BaseNodesResponse<
    TransportAwaitClusterStateVersionAppliedAction.NodeResponse> {

    /// subset of [#failures] that are not caused by the target node not recognizing the action
    /// ([org.elasticsearch.transport.ActionNotFoundTransportException])
    private final List<FailedNodeException> actualFailures;

    public AwaitClusterStateVersionAppliedResponse(
        ClusterName clusterName,
        List<TransportAwaitClusterStateVersionAppliedAction.NodeResponse> nodeResponses,
        List<FailedNodeException> failures
    ) {
        super(clusterName, nodeResponses, failures);
        this.actualFailures = failures().stream()
            .filter(f -> ExceptionsHelper.unwrap(f, ActionNotFoundTransportException.class) == null)
            .collect(Collectors.toList());
    }

    public List<FailedNodeException> actualFailures() {
        return actualFailures;
    }

    public boolean hasActualFailures() {
        return actualFailures.isEmpty() == false;
    }

    @Override
    protected List<TransportAwaitClusterStateVersionAppliedAction.NodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return TransportAction.localOnly();
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<TransportAwaitClusterStateVersionAppliedAction.NodeResponse> nodes)
        throws IOException {
        TransportAction.localOnly();
    }
}
