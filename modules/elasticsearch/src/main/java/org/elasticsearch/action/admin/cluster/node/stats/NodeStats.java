/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.action.admin.cluster.node.stats;

import org.elasticsearch.action.support.nodes.NodeOperationResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.indices.NodeIndicesStats;
import org.elasticsearch.monitor.jvm.JvmStats;
import org.elasticsearch.monitor.network.NetworkStats;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.monitor.process.ProcessStats;
import org.elasticsearch.transport.TransportStats;

import java.io.IOException;

/**
 * Node statistics (static, does not change over time).
 *
 * @author kimchy (shay.banon)
 */
public class NodeStats extends NodeOperationResponse {

    private NodeIndicesStats indices;

    private OsStats os;

    private ProcessStats process;

    private JvmStats jvm;

    private NetworkStats network;

    private TransportStats transport;

    NodeStats() {
    }

    public NodeStats(DiscoveryNode node, NodeIndicesStats indices,
                     OsStats os, ProcessStats process, JvmStats jvm, NetworkStats network,
                     TransportStats transport) {
        super(node);
        this.indices = indices;
        this.os = os;
        this.process = process;
        this.jvm = jvm;
        this.network = network;
        this.transport = transport;
    }

    /**
     * Indices level stats.
     */
    public NodeIndicesStats indices() {
        return this.indices;
    }

    /**
     * Indices level stats.
     */
    public NodeIndicesStats getIndices() {
        return indices();
    }

    /**
     * Operating System level statistics.
     */
    public OsStats os() {
        return this.os;
    }

    /**
     * Operating System level statistics.
     */
    public OsStats getOs() {
        return os();
    }

    /**
     * Process level statistics.
     */
    public ProcessStats process() {
        return process;
    }

    /**
     * Process level statistics.
     */
    public ProcessStats getProcess() {
        return process();
    }

    /**
     * JVM level statistics.
     */
    public JvmStats jvm() {
        return jvm;
    }

    /**
     * JVM level statistics.
     */
    public JvmStats getJvm() {
        return jvm();
    }

    /**
     * Network level statistics.
     */
    public NetworkStats network() {
        return network;
    }

    /**
     * Network level statistics.
     */
    public NetworkStats getNetwork() {
        return network();
    }

    public TransportStats transport() {
        return transport;
    }

    public TransportStats getTransport() {
        return transport();
    }

    public static NodeStats readNodeStats(StreamInput in) throws IOException {
        NodeStats nodeInfo = new NodeStats();
        nodeInfo.readFrom(in);
        return nodeInfo;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        if (in.readBoolean()) {
            indices = NodeIndicesStats.readIndicesStats(in);
        }
        if (in.readBoolean()) {
            os = OsStats.readOsStats(in);
        }
        if (in.readBoolean()) {
            process = ProcessStats.readProcessStats(in);
        }
        if (in.readBoolean()) {
            jvm = JvmStats.readJvmStats(in);
        }
        if (in.readBoolean()) {
            network = NetworkStats.readNetworkStats(in);
        }
        if (in.readBoolean()) {
            transport = TransportStats.readTransportStats(in);
        }
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (indices == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            indices.writeTo(out);
        }
        if (os == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            os.writeTo(out);
        }
        if (process == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            process.writeTo(out);
        }
        if (jvm == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            jvm.writeTo(out);
        }
        if (network == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            network.writeTo(out);
        }
        if (transport == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            transport.writeTo(out);
        }
    }
}