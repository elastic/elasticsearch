/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.nodes;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public abstract class BaseNodesResponse<TNodeResponse extends BaseNodeResponse> extends ActionResponse {

    private final ClusterName clusterName;
    private final List<FailedNodeException> failures;
    private final List<TNodeResponse> nodes;
    private Map<String, TNodeResponse> nodesMap;

    protected BaseNodesResponse(StreamInput in) throws IOException {
        super(in);
        clusterName = new ClusterName(in);
        nodes = readNodesFrom(in);
        failures = in.readList(FailedNodeException::new);
    }

    protected BaseNodesResponse(ClusterName clusterName, List<TNodeResponse> nodes, List<FailedNodeException> failures) {
        this.clusterName = Objects.requireNonNull(clusterName);
        this.failures = Objects.requireNonNull(failures);
        this.nodes = Objects.requireNonNull(nodes);
    }

    /**
     * Get the {@link ClusterName} associated with all of the nodes.
     *
     * @return Never {@code null}.
     */
    public ClusterName getClusterName() {
        return clusterName;
    }

    /**
     * Get the failed node exceptions.
     *
     * @return Never {@code null}. Can be empty.
     */
    public List<FailedNodeException> failures() {
        return failures;
    }

    /**
     * Determine if there are any node failures in {@link #failures}.
     *
     * @return {@code true} if {@link #failures} contains at least 1 {@link FailedNodeException}.
     */
    public boolean hasFailures() {
        return failures.isEmpty() == false;
    }

    /**
     * Get the <em>successful</em> node responses.
     *
     * @return Never {@code null}. Can be empty.
     * @see #hasFailures()
     */
    public List<TNodeResponse> getNodes() {
        return nodes;
    }

    /**
     * Lazily build and get a map of Node ID to node response.
     *
     * @return Never {@code null}. Can be empty.
     * @see #getNodes()
     */
    public Map<String, TNodeResponse> getNodesMap() {
        if (nodesMap == null) {
            nodesMap = new HashMap<>();
            for (TNodeResponse nodeResponse : nodes) {
                nodesMap.put(nodeResponse.getNode().getId(), nodeResponse);
            }
        }
        return nodesMap;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        clusterName.writeTo(out);
        writeNodesTo(out, nodes);
        out.writeList(failures);
    }

    /**
     * Read the {@link #nodes} from the stream.
     *
     * @return Never {@code null}.
     */
    protected abstract List<TNodeResponse> readNodesFrom(StreamInput in) throws IOException;

    /**
     * Write the {@link #nodes} to the stream.
     */
    protected abstract void writeNodesTo(StreamOutput out, List<TNodeResponse> nodes) throws IOException;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BaseNodesResponse<?> that = (BaseNodesResponse<?>) o;
        return Objects.equals(clusterName, that.clusterName)
            && failures.equals(failures)
            && Objects.equals(nodes, that.nodes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterName, failures, nodes);
    }
}
