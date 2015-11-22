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

package org.elasticsearch.action.admin.cluster.node.stats;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.discovery.DiscoveryStats;
import org.elasticsearch.http.HttpStats;
import org.elasticsearch.indices.NodeIndicesStats;
import org.elasticsearch.indices.breaker.AllCircuitBreakerStats;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.monitor.jvm.JvmStats;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.monitor.process.ProcessStats;
import org.elasticsearch.script.ScriptStats;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.elasticsearch.transport.TransportStats;

import java.io.IOException;

/**
 * Node statistics (dynamic, changes depending on when created).
 */
public class NodeStats extends BaseNodeResponse implements ToXContent {

    private long timestamp;

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
    private FsInfo fs;

    @Nullable
    private TransportStats transport;

    @Nullable
    private HttpStats http;

    @Nullable
    private AllCircuitBreakerStats breaker;

    @Nullable
    private ScriptStats scriptStats;

    @Nullable
    private DiscoveryStats discoveryStats;

    NodeStats() {
    }

    public NodeStats(DiscoveryNode node, long timestamp, @Nullable NodeIndicesStats indices,
                     @Nullable OsStats os, @Nullable ProcessStats process, @Nullable JvmStats jvm, @Nullable ThreadPoolStats threadPool,
                     @Nullable FsInfo fs, @Nullable TransportStats transport, @Nullable HttpStats http,
                     @Nullable AllCircuitBreakerStats breaker,
                     @Nullable ScriptStats scriptStats,
                     @Nullable DiscoveryStats discoveryStats) {
        super(node);
        this.timestamp = timestamp;
        this.indices = indices;
        this.os = os;
        this.process = process;
        this.jvm = jvm;
        this.threadPool = threadPool;
        this.fs = fs;
        this.transport = transport;
        this.http = http;
        this.breaker = breaker;
        this.scriptStats = scriptStats;
        this.discoveryStats = discoveryStats;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    @Nullable
    public String getHostname() {
        return getNode().getHostName();
    }

    /**
     * Indices level stats.
     */
    @Nullable
    public NodeIndicesStats getIndices() {
        return this.indices;
    }

    /**
     * Operating System level statistics.
     */
    @Nullable
    public OsStats getOs() {
        return this.os;
    }

    /**
     * Process level statistics.
     */
    @Nullable
    public ProcessStats getProcess() {
        return process;
    }

    /**
     * JVM level statistics.
     */
    @Nullable
    public JvmStats getJvm() {
        return jvm;
    }

    /**
     * Thread Pool level statistics.
     */
    @Nullable
    public ThreadPoolStats getThreadPool() {
        return this.threadPool;
    }

    /**
     * File system level stats.
     */
    @Nullable
    public FsInfo getFs() {
        return fs;
    }

    @Nullable
    public TransportStats getTransport() {
        return this.transport;
    }

    @Nullable
    public HttpStats getHttp() {
        return this.http;
    }

    @Nullable
    public AllCircuitBreakerStats getBreaker() {
        return this.breaker;
    }

    @Nullable
    public ScriptStats getScriptStats() {
        return this.scriptStats;
    }

    @Nullable
    public DiscoveryStats getDiscoveryStats() {
        return this.discoveryStats;
    }

    public static NodeStats readNodeStats(StreamInput in) throws IOException {
        NodeStats nodeInfo = new NodeStats();
        nodeInfo.readFrom(in);
        return nodeInfo;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        timestamp = in.readVLong();
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
            fs = FsInfo.readFsInfo(in);
        }
        if (in.readBoolean()) {
            transport = TransportStats.readTransportStats(in);
        }
        if (in.readBoolean()) {
            http = HttpStats.readHttpStats(in);
        }
        breaker = AllCircuitBreakerStats.readOptionalAllCircuitBreakerStats(in);
        scriptStats = in.readOptionalStreamable(ScriptStats::new);
        discoveryStats = in.readOptionalStreamable(() -> new DiscoveryStats(null));

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(timestamp);
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
        out.writeOptionalStreamable(breaker);
        out.writeOptionalStreamable(scriptStats);
        out.writeOptionalStreamable(discoveryStats);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (!params.param("node_info_format", "default").equals("none")) {
            builder.field("name", getNode().name(), XContentBuilder.FieldCaseConversion.NONE);
            builder.field("transport_address", getNode().address().toString(), XContentBuilder.FieldCaseConversion.NONE);
            builder.field("host", getNode().getHostName(), XContentBuilder.FieldCaseConversion.NONE);
            builder.field("ip", getNode().getAddress(), XContentBuilder.FieldCaseConversion.NONE);

            if (!getNode().attributes().isEmpty()) {
                builder.startObject("attributes");
                for (ObjectObjectCursor<String, String> attr : getNode().attributes()) {
                    builder.field(attr.key, attr.value, XContentBuilder.FieldCaseConversion.NONE);
                }
                builder.endObject();
            }
        }

        if (getIndices() != null) {
            getIndices().toXContent(builder, params);
        }

        if (getOs() != null) {
            getOs().toXContent(builder, params);
        }
        if (getProcess() != null) {
            getProcess().toXContent(builder, params);
        }
        if (getJvm() != null) {
            getJvm().toXContent(builder, params);
        }
        if (getThreadPool() != null) {
            getThreadPool().toXContent(builder, params);
        }
        if (getFs() != null) {
            getFs().toXContent(builder, params);
        }
        if (getTransport() != null) {
            getTransport().toXContent(builder, params);
        }
        if (getHttp() != null) {
            getHttp().toXContent(builder, params);
        }
        if (getBreaker() != null) {
            getBreaker().toXContent(builder, params);
        }
        if (getScriptStats() != null) {
            getScriptStats().toXContent(builder, params);
        }

        if (getDiscoveryStats() != null) {
            getDiscoveryStats().toXContent(builder, params);
        }

        return builder;
    }
}