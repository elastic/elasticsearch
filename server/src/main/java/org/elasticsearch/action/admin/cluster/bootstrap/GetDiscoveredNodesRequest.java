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
package org.elasticsearch.action.admin.cluster.bootstrap;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Request the set of master-eligible nodes discovered by this node. Most useful in a brand-new cluster as a precursor to setting the
 * initial configuration using {@link BootstrapClusterRequest}.
 */
public class GetDiscoveredNodesRequest extends ActionRequest {

    private int waitForNodes = 1;

    @Nullable // if the request should wait indefinitely
    private TimeValue timeout = TimeValue.timeValueSeconds(30);

    private List<String> requiredNodes = Collections.emptyList();

    public GetDiscoveredNodesRequest() {
    }

    public GetDiscoveredNodesRequest(StreamInput in) throws IOException {
        super(in);
        waitForNodes = in.readInt();
        timeout = in.readOptionalTimeValue();
        requiredNodes = in.readList(StreamInput::readString);
    }

    /**
     * Sometimes it is useful only to receive a successful response after discovering a certain number of master-eligible nodes. This
     * parameter controls this behaviour.
     *
     * @param waitForNodes the minimum number of nodes to have discovered before this request will receive a successful response. Must
     *                     be at least 1, because we always discover the local node.
     */
    public void setWaitForNodes(int waitForNodes) {
        if (waitForNodes < 1) {
            throw new IllegalArgumentException("always finds at least one node, waiting for [" + waitForNodes + "] is not allowed");
        }
        this.waitForNodes = waitForNodes;
    }

    /**
     * Sometimes it is useful only to receive a successful response after discovering a certain number of master-eligible nodes. This
     * parameter controls this behaviour.
     *
     * @return the minimum number of nodes to have discovered before this request will receive a successful response.
     */
    public int getWaitForNodes() {
        return waitForNodes;
    }

    /**
     * Sometimes it is useful to wait until enough nodes have been discovered, rather than failing immediately. This parameter controls how
     * long to wait, and defaults to 30s.
     *
     * @param timeout how long to wait to discover sufficiently many nodes to respond successfully.
     */
    public void setTimeout(@Nullable TimeValue timeout) {
        if (timeout != null && timeout.compareTo(TimeValue.ZERO) < 0) {
            throw new IllegalArgumentException("negative timeout of [" + timeout + "] is not allowed");
        }
        this.timeout = timeout;
    }

    /**
     * Sometimes it is useful to wait until enough nodes have been discovered, rather than failing immediately. This parameter controls how
     * long to wait, and defaults to 30s.
     *
     * @return how long to wait to discover sufficiently many nodes to respond successfully.
     */
    @Nullable
    public TimeValue getTimeout() {
        return timeout;
    }

    /**
     * Sometimes it is useful only to receive a successful response after discovering a certain set of master-eligible nodes.
     * This parameter gives the names or transport addresses of the expected nodes.
     *
     * @return list of expected nodes
     */
    public List<String> getRequiredNodes() {
        return requiredNodes;
    }

    /**
     * Sometimes it is useful only to receive a successful response after discovering a certain set of master-eligible nodes.
     * This parameter gives the names or transport addresses of the expected nodes.
     *
     * @param requiredNodes list of expected nodes
     */
    public void setRequiredNodes(final List<String> requiredNodes) {
        this.requiredNodes = requiredNodes;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(waitForNodes);
        out.writeOptionalTimeValue(timeout);
        out.writeStringList(requiredNodes);
    }

    @Override
    public String toString() {
        return "GetDiscoveredNodesRequest{" +
            "waitForNodes=" + waitForNodes +
            ", timeout=" + timeout +
            ", requiredNodes=" + requiredNodes + "}";
    }
}
