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
package org.elasticsearch.action.admin.cluster.configuration;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.Arrays;

/**
 * A request to add voting tombstones for certain master-eligible nodes, and wait for these nodes to be removed from the voting
 * configuration.
 */
public class AddVotingTombstonesRequest extends MasterNodeRequest<AddVotingTombstonesRequest> {
    private final String[] nodeDescriptions;
    private final TimeValue timeout;

    /**
     * Construct a request to add voting tombstones for master-eligible nodes matching the given descriptions, and wait for a default 30
     * seconds for these nodes to be removed from the voting configuration.
     * @param nodeDescriptions Descriptions of the nodes to add - see {@link DiscoveryNodes#resolveNodes(String...)}
     */
    public AddVotingTombstonesRequest(String[] nodeDescriptions) {
        this(nodeDescriptions, TimeValue.timeValueSeconds(30));
    }

    /**
     * Construct a request to add voting tombstones for master-eligible nodes matching the given descriptions, and wait for these nodes to
     * be removed from the voting configuration.
     * @param nodeDescriptions Descriptions of the nodes whose tombstones to add - see {@link DiscoveryNodes#resolveNodes(String...)}.
     * @param timeout How long to wait for the nodes to be removed from the voting configuration.
     */
    public AddVotingTombstonesRequest(String[] nodeDescriptions, TimeValue timeout) {
        if (timeout.compareTo(TimeValue.ZERO) < 0) {
            throw new IllegalArgumentException("timeout [" + timeout + "] must be non-negative");
        }
        this.nodeDescriptions = nodeDescriptions;
        this.timeout = timeout;
    }

    public AddVotingTombstonesRequest(StreamInput in) throws IOException {
        super(in);
        nodeDescriptions = in.readStringArray();
        timeout = in.readTimeValue();
    }

    /**
     * @return descriptions of the nodes for whom to add tombstones.
     */
    public String[] getNodeDescriptions() {
        return nodeDescriptions;
    }

    /**
     * @return how long to wait after adding the tombstones for the nodes to be removed from the voting configuration.
     */
    public TimeValue getTimeout() {
        return timeout;
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
        out.writeStringArray(nodeDescriptions);
        out.writeTimeValue(timeout);
    }

    @Override
    public String toString() {
        return "AddVotingTombstonesRequest{" +
            "nodeDescriptions=" + Arrays.asList(nodeDescriptions) +
            ", timeout=" + timeout +
            '}';
    }
}
