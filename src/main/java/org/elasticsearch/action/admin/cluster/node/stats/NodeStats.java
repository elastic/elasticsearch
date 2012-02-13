/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.http.HttpStats;
import org.elasticsearch.indices.NodeIndicesStats;
import org.elasticsearch.monitor.fs.FsStats;
import org.elasticsearch.monitor.jvm.JvmStats;
import org.elasticsearch.monitor.network.NetworkStats;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.monitor.process.ProcessStats;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.elasticsearch.transport.TransportStats;

import java.io.IOException;

/**
 * Node statistics (static, does not change over time).
 */
public class NodeStats extends NodeOperationResponse {

    @Nullable
    private String hostname;

    @Nullable
    private NodeIndicesStats indices;

    @Nullable
    private OsStats os;

    @Nullable
    private ProcessStats process;

    @Nullable
    private JvmStats jvm;

    @Nullable
    private ThreadPoolStats threadPool;

    @Nullable
    private NetworkStats network;

    @Nullable
    private FsStats fs;

    @Nullable
    private TransportStats transport;

    @Nullable
    private HttpStats http;

    NodeStats() {
    }

    public NodeStats(DiscoveryNode node, @Nullable String hostname, @Nullable NodeIndicesStats indices,
                     @Nullable OsStats os, @Nullable ProcessStats process, @Nullable JvmStats jvm, @Nullable ThreadPoolStats threadPool, @Nullable NetworkStats network,
                     @Nullable FsStats fs, @Nullable TransportStats transport, @Nullable HttpStats http) {
        super(node);
        this.hostname = hostname;
        this.indices = indices;
        this.os = os;
        this.process = process;
        this.jvm = jvm;
        this.threadPool = threadPool;
        this.network = network;
        this.fs = fs;
        this.transport = transport;
        this.http = http;
    }

    @Nullable
    public String hostname() {
        return this.hostname;
    }

    @Nullable
    public String getHostname() {
        return this.hostname;
    }

    /**
     * Indices level stats.
     */
    @Nullable
    public NodeIndicesStats indices() {
        return this.indices;
    }

    /**
     * Indices level stats.
     */
    @Nullable
    public NodeIndicesStats getIndices() {
        return indices();
    }

    /**
     * Operating System level statistics.
     */
    @Nullable
    public OsStats os() {
        return this.os;
    }

    /**
     * Operating System level statistics.
     */
    @Nullable
    public OsStats getOs() {
        return os();
    }

    /**
     * Process level statistics.
     */
    @Nullable
    public ProcessStats process() {
        return process;
    }

    /**
     * Process level statistics.
     */
    @Nullable
    public ProcessStats getProcess() {
        return process();
    }

    /**
     * JVM level statistics.
     */
    @Nullable
    public JvmStats jvm() {
        return jvm;
    }

    /**
     * JVM level statistics.
     */
    @Nullable
    public JvmStats getJvm() {
        return jvm();
    }

    /**
     * Thread Pool level statistics.
     */
    @Nullable
    public ThreadPoolStats threadPool() {
        return this.threadPool;
    }

    /**
     * Thread Pool level statistics.
     */
    @Nullable
    public ThreadPoolStats getThreadPool() {
        return threadPool();
    }

    /**
     * Network level statistics.
     */
    @Nullable
    public NetworkStats network() {
        return network;
    }

    /**
     * Network level statistics.
     */
    @Nullable
    public NetworkStats getNetwork() {
        return network();
    }

    /**
     * File system level stats.
     */
    @Nullable
    public FsStats fs() {
        return fs;
    }

    /**
     * File system level stats.
     */
    @Nullable
    public FsStats getFs() {
        return fs();
    }

    @Nullable
    public TransportStats transport() {
        return this.transport;
    }

    @Nullable
    public TransportStats getTransport() {
        return transport();
    }

    @Nullable
    public HttpStats http() {
        return this.http;
    }

    @Nullable
    public HttpStats getHttp() {
        return http();
    }

    public static NodeStats readNodeStats(StreamInput in) throws IOException {
        NodeStats nodeInfo = new NodeStats();
        nodeInfo.readFrom(in);
        return nodeInfo;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        if (in.readBoolean()) {
            hostname = in.readUTF();
        }
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
            threadPool = ThreadPoolStats.readThreadPoolStats(in);
        }
        if (in.readBoolean()) {
            network = NetworkStats.readNetworkStats(in);
        }
        if (in.readBoolean()) {
            fs = FsStats.readFsStats(in);
        }
        if (in.readBoolean()) {
            transport = TransportStats.readTransportStats(in);
        }
        if (in.readBoolean()) {
            http = HttpStats.readHttpStats(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (hostname == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(hostname);
        }
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
        if (threadPool == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            threadPool.writeTo(out);
        }
        if (network == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            network.writeTo(out);
        }
        if (fs == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            fs.writeTo(out);
        }
        if (transport == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            transport.writeTo(out);
        }
        if (http == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            http.writeTo(out);
        }
    }
}