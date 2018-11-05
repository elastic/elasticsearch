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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;

/**
 * Request the set of master-eligible nodes discovered by this node. Most useful in a brand-new cluster as a precursor to setting the
 * initial configuration using {@link BootstrapClusterRequest}.
 */
public class GetDiscoveredNodesRequest extends ActionRequest {

    private int minimumNodeCount = 1;
    private TimeValue timeout = TimeValue.ZERO;

    public GetDiscoveredNodesRequest() {
    }

    public GetDiscoveredNodesRequest(StreamInput in) throws IOException {
        super(in);
        minimumNodeCount = in.readInt();
        timeout = in.readTimeValue();
    }

    /**
     * Sometimes it is useful only to receive a successful response after discovering a certain number of master-eligible nodes. This
     * parameter controls this behaviour.
     *
     * @param minimumNodeCount the minimum number of nodes to have discovered before this request will receive a successful response. Must
     *                         be at least 1.
     */
    public void setMinimumNodeCount(int minimumNodeCount) {
        if (minimumNodeCount < 1) {
            throw new IllegalArgumentException("always finds at least one node, waiting for [" + minimumNodeCount + "] is not allowed");
        }
        this.minimumNodeCount = minimumNodeCount;
    }

    /**
     * Sometimes it is useful only to receive a successful response after discovering a certain number of master-eligible nodes. This
     * parameter controls this behaviour.
     *
     * @return the minimum number of nodes to have discovered before this request will receive a successful response.
     */
    public int getMinimumNodeCount() {
        return minimumNodeCount;
    }

    /**
     * Sometimes it is useful to wait until enough nodes have been discovered, rather than failing immediately. This parameter controls how
     * long to wait. It may only be set to a nonzero timeout when waiting for more than one node, because every node always discovers
     * at least one node, itself.
     * @param timeout how long to wait to discover sufficiently many nodes to respond successfully.
     */
    public void setTimeout(TimeValue timeout) {
        if (timeout.compareTo(TimeValue.ZERO) < 0) {
            throw new IllegalArgumentException("negative timeout of [" + timeout + "] is not allowed");
        }
        this.timeout = timeout;
    }

    /**
     * Sometimes it is useful to wait until enough nodes have been discovered, rather than failing immediately. This parameter controls how
     * long to wait. It may only be set to a nonzero timeout when waiting for more than one node, because every node always discovers
     * at least one node, itself.
     * @return how long to wait to discover sufficiently many nodes to respond successfully.
     */
    public TimeValue getTimeout() {
        return timeout;
    }

    @Override
    public ActionRequestValidationException validate() {
        final ActionRequestValidationException actionRequestValidationException = new ActionRequestValidationException();

        assert minimumNodeCount > 0 : minimumNodeCount;
        if (timeout.compareTo(TimeValue.ZERO) > 0 && minimumNodeCount <= 1) {
            actionRequestValidationException.addValidationError(
                "always discovers at least one node, so a timeout of [" + timeout + "] is unnecessary");
        }

        return actionRequestValidationException.validationErrors().isEmpty() ? null : actionRequestValidationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(getMinimumNodeCount());
        out.writeTimeValue(getTimeout());
    }

    @Override
    public String toString() {
        return "GetDiscoveredNodesRequest{" +
            "minimumNodeCount=" + minimumNodeCount +
            ", timeout=" + timeout +
            '}';
    }
}
