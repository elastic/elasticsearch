/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.support.nodes;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 *
 */
public abstract class BaseNodesResponse<TNodeResponse extends BaseNodeResponse> extends ActionResponse {

    private ClusterName clusterName;
    private List<FailedNodeException> failures;
    private List<TNodeResponse> nodes;
    private Map<String, TNodeResponse> nodesMap;

    protected BaseNodesResponse() {
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
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        clusterName = new ClusterName(in);
        nodes = readNodesFrom(in);
        failures = in.readList(FailedNodeException::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
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

}
